#!/usr/bin/env python3
"""
Minimal API server for Schema Translator that avoids pandas import issues.
"""

import sys
from pathlib import Path
import json
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from app.core.config import settings
from app.core.llm_mapper import LLMMapper
from app.shared.logging import logger

# Create FastAPI app
app = FastAPI(
    title="Schema Translator",
    description="LLM-powered semantic schema mapping for heterogeneous tenant data",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize LLM mapper
try:
    llm_mapper = LLMMapper()
    logger.info("Initialized LLM mapper with OpenAI")
except Exception as e:
    logger.error(f"Failed to initialize LLM mapper: {e}")
    llm_mapper = None


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy", 
        "version": "0.1.0",
        "llm_available": llm_mapper is not None,
        "openai_configured": bool(settings.openai_api_key)
    }


@app.get("/schema/canonical")
async def get_canonical_schema():
    """Get the canonical schema definition."""
    try:
        if not llm_mapper:
            raise HTTPException(status_code=500, detail="LLM mapper not available")
        
        canonical_schema = llm_mapper.canonical_schema
        return canonical_schema.dict()
        
    except Exception as e:
        logger.error(f"Error retrieving canonical schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/test/mapping")
async def test_mapping(
    tenant: str = Query(..., description="Tenant identifier"),
    table: str = Query(..., description="Table name"),
    column: str = Query(..., description="Column name"),
    sample_values: str = Query(..., description="Comma-separated sample values")
):
    """
    Test LLM mapping for a single column.
    
    This endpoint allows you to test the LLM mapping functionality without
    needing the full discovery pipeline.
    """
    try:
        if not llm_mapper:
            raise HTTPException(status_code=500, detail="LLM mapper not available")
        
        from app.shared.models import ColumnProfile, SourceColumn, ColumnType
        
        # Create test column profile
        source_col = SourceColumn(
            tenant=tenant,
            table=table,
            column=column
        )
        
        # Parse sample values
        samples = [val.strip() for val in sample_values.split(",") if val.strip()]
        
        profile = ColumnProfile(
            source_column=source_col,
            total_rows=len(samples),
            non_null_count=len(samples),
            distinct_count=len(set(samples)),
            distinct_ratio=len(set(samples)) / len(samples) if samples else 0,
            sample_values=samples[:10],
            inferred_type=ColumnType.STRING,  # Simplified for demo
            cooccurring_columns=[]
        )
        
        # Get LLM mapping
        response = llm_mapper.map_column(profile)
        
        return {
            "tenant": tenant,
            "source_column": f"{table}.{column}",
            "sample_values": samples,
            "llm_response": {
                "proposed_mappings": [
                    {
                        "canonical_field": p.canonical_field,
                        "justification": p.justification,
                        "confidence": p.confidence,
                        "transform_hint": p.transform_hint,
                        "assumptions": p.assumptions
                    }
                    for p in response.proposed_mappings
                ],
                "alternatives": [
                    {
                        "canonical_field": a.canonical_field,
                        "confidence": a.confidence,
                        "note": a.note
                    }
                    for a in response.alternatives
                ],
                "reasoning": response.reasoning
            }
        }
        
    except Exception as e:
        logger.error(f"Error testing mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tenants/sample")
async def get_sample_tenants():
    """Get information about sample tenants."""
    try:
        tenants = []
        
        schemas_dir = settings.customer_schemas_dir
        if schemas_dir.exists():
            for tenant_dir in schemas_dir.iterdir():
                if tenant_dir.is_dir() and (tenant_dir / "schema.yaml").exists():
                    tenant_name = tenant_dir.name
                    
                    # Check if sample data exists
                    sample_dir = settings.customer_samples_dir / tenant_name
                    has_samples = sample_dir.exists() and any(sample_dir.glob("*.csv"))
                    
                    tenants.append({
                        "tenant": tenant_name,
                        "has_schema": True,
                        "has_sample_data": has_samples,
                        "schema_path": str(tenant_dir / "schema.yaml"),
                        "sample_path": str(sample_dir) if has_samples else None
                    })
        
        return {"tenants": tenants}
        
    except Exception as e:
        logger.error(f"Error listing tenants: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/demo/examples")
async def get_demo_examples():
    """Get pre-configured demo examples for testing."""
    basic_examples = [
        {
            "category": "Basic Mappings",
            "examples": [
                {
                    "name": "Contract ID",
                    "tenant": "tenant_A",
                    "table": "contracts",
                    "column": "contract_id",
                    "sample_values": "CNT-001,CNT-002,CNT-003,CNT-004",
                    "expected_mapping": "contract_id",
                    "description": "Direct mapping - unique contract identifier",
                    "complexity": "Simple"
                },
                {
                    "name": "Customer Name",
                    "tenant": "tenant_A", 
                    "table": "contracts",
                    "column": "customer_name",
                    "sample_values": "Acme Corp,Beta LLC,Gamma Industries,Delta Systems",
                    "expected_mapping": "party_buyer",
                    "description": "Semantic mapping - customer to buyer party",
                    "complexity": "Simple"
                }
            ]
        }
    ]
    
    complex_examples = [
        {
            "category": "Complex Legacy Systems",
            "examples": [
                {
                    "name": "Encoded Status Codes",
                    "tenant": "tenant_D",
                    "table": "master_agreements",
                    "column": "agmt_status_cd",
                    "sample_values": "AC,EX,PE,CA,SU",
                    "expected_mapping": "status",
                    "description": "Decode legacy status codes: AC=Active, EX=Expired, PE=Pending, CA=Cancelled, SU=Suspended",
                    "complexity": "Complex"
                },
                {
                    "name": "Financial Terms with Context",
                    "tenant": "tenant_D",
                    "table": "financial_terms",
                    "column": "term_type",
                    "sample_values": "ANNUAL_RECURRING,LIFETIME_VALUE,MILESTONE_BASED,USAGE_BASED",
                    "expected_mapping": "contract_value_arr",
                    "description": "Context-dependent mapping based on term_type field",
                    "complexity": "Complex"
                },
                {
                    "name": "Values in Cents",
                    "tenant": "tenant_D",
                    "table": "master_agreements", 
                    "column": "base_value_amt",
                    "sample_values": "12000000,850000,0,2400000",
                    "expected_mapping": "contract_value_ltv",
                    "description": "Legacy system stores values in cents - requires division by 100",
                    "complexity": "Complex"
                }
            ]
        },
        {
            "category": "SaaS/Subscription Models",
            "examples": [
                {
                    "name": "MRR in Cents",
                    "tenant": "tenant_E",
                    "table": "subscriptions",
                    "column": "mrr_usd_cents",
                    "sample_values": "833333,49900,1666,208333",
                    "expected_mapping": "contract_value_arr",
                    "description": "Monthly recurring revenue in cents - convert to ARR",
                    "complexity": "Complex"
                },
                {
                    "name": "Subscription States",
                    "tenant": "tenant_E",
                    "table": "subscriptions",
                    "column": "subscription_state",
                    "sample_values": "ACTIVE,PAUSED,CANCELLED,CHURNED,TRIAL,PENDING_ACTIVATION",
                    "expected_mapping": "status",
                    "description": "Map SaaS subscription states to contract status",
                    "complexity": "Medium"
                },
                {
                    "name": "UUID Identifiers",
                    "tenant": "tenant_E",
                    "table": "subscriptions",
                    "column": "subscription_uuid",
                    "sample_values": "550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "expected_mapping": "contract_id",
                    "description": "UUID format identifiers vs traditional contract IDs",
                    "complexity": "Medium"
                }
            ]
        },
        {
            "category": "Government/Compliance",
            "examples": [
                {
                    "name": "Government Contract Numbers",
                    "tenant": "tenant_F",
                    "table": "procurement_contracts",
                    "column": "contract_number",
                    "sample_values": "GS-35F-0119Y-DOD-001,VA-118-23-C-0045,NASA-2024-C-12345",
                    "expected_mapping": "contract_id",
                    "description": "Complex government contract numbering schemes",
                    "complexity": "Medium"
                },
                {
                    "name": "Award vs Obligation Amounts",
                    "tenant": "tenant_F",
                    "table": "procurement_contracts",
                    "column": "award_amount_dollars",
                    "sample_values": "15750000.00,3200000.00,8900000.00,2100000.00",
                    "expected_mapping": "contract_value_ltv",
                    "description": "Government contracts: award amount vs obligation amount semantics",
                    "complexity": "Complex"
                },
                {
                    "name": "Performance Periods",
                    "tenant": "tenant_F",
                    "table": "procurement_contracts",
                    "column": "base_period_months",
                    "sample_values": "12,24,36,12",
                    "expected_mapping": "renewal_term_months",
                    "description": "Government base periods vs commercial renewal terms",
                    "complexity": "Complex"
                }
            ]
        },
        {
            "category": "Multi-Table Relationships",
            "examples": [
                {
                    "name": "Cross-Table Client Names",
                    "tenant": "tenant_D",
                    "table": "client_master",
                    "column": "client_legal_name",
                    "sample_values": "GlobalTech Solutions Inc.,European Manufacturing GmbH,Innovation Labs Ltd",
                    "expected_mapping": "party_buyer",
                    "description": "Client names in separate reference table requiring joins",
                    "complexity": "Complex"
                },
                {
                    "name": "Vendor DUNS Numbers",
                    "tenant": "tenant_F",
                    "table": "vendor_information",
                    "column": "legal_business_name",
                    "sample_values": "TechCorp Solutions Inc.,SmallBiz IT Services LLC,Aerospace Engineering Consortium",
                    "expected_mapping": "party_seller",
                    "description": "Vendor information in separate table with DUNS identifiers",
                    "complexity": "Complex"
                }
            ]
        }
    ]
    
    return {
        "basic_examples": basic_examples,
        "complex_examples": complex_examples,
        "total_scenarios": sum(len(cat["examples"]) for cat in basic_examples + complex_examples)
    }


@app.on_event("startup")
async def startup_event():
    """Application startup."""
    logger.info("Starting Schema Translator API (Minimal Version)")
    logger.info(f"OpenAI configured: {bool(settings.openai_api_key)}")
    logger.info(f"Using model: {settings.openai_model}")
    logger.info(f"LLM mapper available: {llm_mapper is not None}")


if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Schema Translator API (Minimal Version)")
    print(f"📊 OpenAI API Key configured: {bool(settings.openai_api_key)}")
    print(f"🤖 Model: {settings.openai_model}")
    print("📖 Visit http://localhost:8000/docs for interactive documentation")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
