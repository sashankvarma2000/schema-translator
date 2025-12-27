"""FastAPI routes for schema translation system."""

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ..core.config import settings
from ..core.discovery import SchemaDiscoverer
from ..core.llm_mapper import LLMMapper
from ..core.resolver import MappingResolver
from ..core.transforms import DataTransformer
from ..shared.logging import logger
from ..shared.models import (
    ColumnMapping,
    HITLRequest,
    MappingPlan,
    TransformResult,
)

router = APIRouter()

# Initialize components
discoverer = SchemaDiscoverer(
    schemas_dir=settings.customer_schemas_dir,
    samples_dir=settings.customer_samples_dir
)

llm_mapper = LLMMapper()
resolver = MappingResolver(llm_mapper)
transformer = DataTransformer()


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "0.1.0"}


@router.post("/map/preview")
async def preview_mapping(tenant: str = Query(..., description="Tenant identifier")):
    """
    Generate mapping preview for a tenant without applying transforms.
    
    Returns proposed mappings with confidence scores and HITL requirements.
    """
    try:
        logger.info(f"Generating mapping preview for tenant: {tenant}")
        
        # Profile all columns for the tenant
        column_profiles = discoverer.profile_tenant_columns(tenant)
        
        if not column_profiles:
            raise HTTPException(
                status_code=404, 
                detail=f"No schema or sample data found for tenant {tenant}"
            )
        
        # Generate mappings
        mappings = resolver.resolve_batch_mappings(column_profiles)
        
        # Separate into categories
        auto_accepted = [m for m in mappings if m.status == "accepted"]
        hitl_required = [m for m in mappings if m.status == "hitl_required"]
        rejected = [m for m in mappings if m.status == "rejected"]
        
        # Calculate coverage stats
        total_columns = len(mappings)
        mapped_columns = len([m for m in mappings if m.canonical_field])
        coverage_rate = mapped_columns / total_columns if total_columns > 0 else 0.0
        
        return {
            "tenant": tenant,
            "total_columns": total_columns,
            "coverage_rate": coverage_rate,
            "auto_accepted": len(auto_accepted),
            "hitl_required": len(hitl_required),
            "rejected": len(rejected),
            "mappings": [
                {
                    "source_column": f"{m.source_column.table}.{m.source_column.column}",
                    "canonical_field": m.canonical_field,
                    "status": m.status,
                    "confidence": m.mapping_score.final_score if m.mapping_score else 0.0,
                    "transform_rule": m.transform_rule,
                    "justification": (
                        m.llm_response.proposed_mappings[0].justification 
                        if m.llm_response and m.llm_response.proposed_mappings 
                        else None
                    )
                }
                for m in mappings
            ]
        }
        
    except Exception as e:
        logger.error(f"Error generating mapping preview for {tenant}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/map/apply")
async def apply_mapping(
    tenant: str = Query(..., description="Tenant identifier"),
    auto_approve_threshold: Optional[float] = Query(
        None, description="Override auto-approval threshold"
    )
):
    """
    Apply mapping transformations for a tenant.
    
    Only applies auto-accepted mappings unless overridden.
    """
    try:
        logger.info(f"Applying mappings for tenant: {tenant}")
        
        # Profile columns
        column_profiles = discoverer.profile_tenant_columns(tenant)
        
        if not column_profiles:
            raise HTTPException(
                status_code=404,
                detail=f"No schema or sample data found for tenant {tenant}"
            )
        
        # Generate mappings
        mappings = resolver.resolve_batch_mappings(column_profiles)
        
        # Override threshold if provided
        if auto_approve_threshold is not None:
            for mapping in mappings:
                if mapping.mapping_score:
                    if mapping.mapping_score.final_score >= auto_approve_threshold:
                        mapping.status = "accepted"
        
        # Create mapping plan
        mapping_plan = MappingPlan(
            tenant=tenant,
            version="1.0",
            canonical_schema_version="1.0",
            mappings=mappings,
            coverage_stats={
                "total_columns": len(mappings),
                "accepted_mappings": len([m for m in mappings if m.status == "accepted"]),
                "hitl_required": len([m for m in mappings if m.status == "hitl_required"]),
                "rejected": len([m for m in mappings if m.status == "rejected"])
            }
        )
        
        # Load source data
        source_data = _load_tenant_data(tenant)
        
        # Apply transformations
        transform_result = transformer.apply_mapping_plan(
            tenant, mapping_plan, source_data
        )
        
        return {
            "tenant": tenant,
            "mapping_plan_version": mapping_plan.version,
            "output_path": transform_result.output_path,
            "rows_processed": transform_result.rows_processed,
            "rows_successful": transform_result.rows_successful,
            "errors": transform_result.errors,
            "coverage_stats": mapping_plan.coverage_stats
        }
        
    except Exception as e:
        logger.error(f"Error applying mappings for {tenant}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/explain")
async def explain_mapping(
    tenant: str = Query(..., description="Tenant identifier"),
    field: str = Query(..., description="Canonical field to explain")
):
    """
    Explain how a canonical field was derived for a tenant.
    
    Returns mapping justification, source columns, and lineage information.
    """
    try:
        logger.info(f"Explaining mapping for {tenant}.{field}")
        
        # Profile columns and generate mappings
        column_profiles = discoverer.profile_tenant_columns(tenant)
        mappings = resolver.resolve_batch_mappings(column_profiles)
        
        # Find mappings for the requested field
        field_mappings = [m for m in mappings if m.canonical_field == field]
        
        if not field_mappings:
            raise HTTPException(
                status_code=404,
                detail=f"No mapping found for field '{field}' in tenant {tenant}"
            )
        
        explanations = []
        for mapping in field_mappings:
            explanation = {
                "source_column": f"{mapping.source_column.table}.{mapping.source_column.column}",
                "canonical_field": mapping.canonical_field,
                "status": mapping.status,
                "confidence": mapping.mapping_score.final_score if mapping.mapping_score else 0.0,
                "transform_rule": mapping.transform_rule,
            }
            
            # Add LLM reasoning if available
            if mapping.llm_response:
                if mapping.llm_response.proposed_mappings:
                    proposal = mapping.llm_response.proposed_mappings[0]
                    explanation.update({
                        "justification": proposal.justification,
                        "assumptions": proposal.assumptions,
                        "llm_confidence": proposal.confidence
                    })
                
                explanation["llm_reasoning"] = mapping.llm_response.reasoning
            
            # Add scoring breakdown
            if mapping.mapping_score:
                explanation["score_breakdown"] = {
                    "llm_confidence": mapping.mapping_score.llm_confidence,
                    "name_similarity": mapping.mapping_score.name_similarity,
                    "type_compatibility": mapping.mapping_score.type_compatibility,
                    "value_range_match": mapping.mapping_score.value_range_match,
                    "final_score": mapping.mapping_score.final_score
                }
            
            # Add sample values from profiling
            profile = next(
                (p for p in column_profiles 
                 if p.source_column.column == mapping.source_column.column),
                None
            )
            if profile:
                explanation["sample_values"] = profile.sample_values
                explanation["column_stats"] = {
                    "total_rows": profile.total_rows,
                    "non_null_count": profile.non_null_count,
                    "distinct_ratio": profile.distinct_ratio,
                    "inferred_type": profile.inferred_type.value
                }
            
            explanations.append(explanation)
        
        return {
            "tenant": tenant,
            "canonical_field": field,
            "mappings": explanations
        }
        
    except Exception as e:
        logger.error(f"Error explaining mapping for {tenant}.{field}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tenants")
async def list_tenants():
    """List available tenants with schema information."""
    try:
        tenants = []
        
        for tenant_dir in settings.customer_schemas_dir.iterdir():
            if tenant_dir.is_dir() and (tenant_dir / "schema.yaml").exists():
                tenant_name = tenant_dir.name
                
                # Get basic schema info
                try:
                    schema_tables = discoverer.discover_tenant_schema(tenant_name)
                    table_count = len(schema_tables)
                    column_count = sum(len(cols) for cols in schema_tables.values())
                    
                    # Check if sample data exists
                    sample_dir = settings.customer_samples_dir / tenant_name
                    has_samples = sample_dir.exists() and any(sample_dir.glob("*.csv"))
                    
                    tenants.append({
                        "tenant": tenant_name,
                        "tables": table_count,
                        "columns": column_count,
                        "has_sample_data": has_samples,
                        "table_names": list(schema_tables.keys())
                    })
                    
                except Exception as e:
                    logger.warning(f"Error processing tenant {tenant_name}: {e}")
                    tenants.append({
                        "tenant": tenant_name,
                        "error": str(e)
                    })
        
        return {"tenants": tenants}
        
    except Exception as e:
        logger.error(f"Error listing tenants: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema/canonical")
async def get_canonical_schema():
    """Get the canonical schema definition."""
    try:
        canonical_schema = llm_mapper.canonical_schema
        return canonical_schema.dict()
        
    except Exception as e:
        logger.error(f"Error retrieving canonical schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _load_tenant_data(tenant: str) -> Dict[str, pd.DataFrame]:
    """Load all CSV data for a tenant."""
    data = {}
    tenant_sample_dir = settings.customer_samples_dir / tenant
    
    if not tenant_sample_dir.exists():
        raise FileNotFoundError(f"Sample directory not found: {tenant_sample_dir}")
    
    for csv_file in tenant_sample_dir.glob("*.csv"):
        table_name = csv_file.stem
        try:
            df = pd.read_csv(csv_file)
            data[table_name] = df
            logger.debug(f"Loaded {len(df)} rows from {csv_file}")
        except Exception as e:
            logger.error(f"Error loading {csv_file}: {e}")
    
    return data
