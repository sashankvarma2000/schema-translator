"""LLM-powered semantic column mapping."""

import yaml
from pathlib import Path
from typing import List, Optional

from ..adapters.llm_openai import OpenAIAdapter
from ..core.config import settings
from ..shared.logging import logger
from ..shared.models import (
    CanonicalSchema,
    ColumnProfile,
    LLMResponse,
)


class LLMMapper:
    """Coordinates LLM-based semantic mapping of columns to canonical schema."""
    
    def __init__(self):
        if not settings.openai_api_key:
            raise ValueError("OpenAI API key is required. Please set OPENAI_API_KEY environment variable.")
        
        logger.info("Using OpenAI LLM adapter and real canonical schema")
        self.canonical_schema = self._load_canonical_schema()
        self.llm_adapter = OpenAIAdapter()
    
    def _load_canonical_schema(self) -> CanonicalSchema:
        """Load the canonical schema definition."""
        if not settings.canonical_schema_path.exists():
            raise FileNotFoundError(
                f"Canonical schema not found: {settings.canonical_schema_path}"
            )
        
        with open(settings.canonical_schema_path, 'r') as f:
            schema_data = yaml.safe_load(f)
        
        # Convert to Pydantic model
        return CanonicalSchema(**schema_data)
    
    
    def map_column(
        self, 
        column_profile: ColumnProfile,
        additional_context: Optional[str] = None
    ) -> LLMResponse:
        """
        Get LLM mapping proposal for a column profile.
        
        Args:
            column_profile: Statistical profile of the source column
            additional_context: Optional additional context
            
        Returns:
            LLM response with mapping proposals
        """
        
        # Build canonical schema excerpt for the LLM
        schema_excerpt = self._build_schema_excerpt()
        
        # Extract information from profile
        source_col = column_profile.source_column
        
        try:
            response = self.llm_adapter.map_column(
                canonical_schema_excerpt=schema_excerpt,
                tenant=source_col.tenant,
                table=source_col.table,
                column=source_col.column,
                column_samples=column_profile.sample_values,
                cooccurring_columns=column_profile.cooccurring_columns,
                column_type=column_profile.inferred_type.value,
                description=source_col.description,
                additional_context=additional_context
            )
            
            logger.info(
                f"Generated mapping proposal for {source_col.tenant}.{source_col.table}.{source_col.column}: "
                f"{len(response.proposed_mappings)} proposals, "
                f"{len(response.alternatives)} alternatives"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to generate mapping for {source_col.column}: {e}")
            # Return empty response on error
            return LLMResponse(
                proposed_mappings=[],
                alternatives=[],
                reasoning=f"Error occurred during mapping: {str(e)}"
            )
    
    def map_columns_batch(
        self, 
        column_profiles: List[ColumnProfile],
        additional_context: Optional[str] = None
    ) -> List[LLMResponse]:
        """
        Map multiple columns in batch.
        
        Args:
            column_profiles: List of column profiles to map
            additional_context: Optional additional context
            
        Returns:
            List of LLM responses
        """
        responses = []
        
        for i, profile in enumerate(column_profiles):
            logger.info(f"Mapping column {i+1}/{len(column_profiles)}: "
                       f"{profile.source_column.table}.{profile.source_column.column}")
            
            response = self.map_column(profile, additional_context)
            responses.append(response)
        
        return responses
    
    def _build_schema_excerpt(self) -> str:
        """Build a formatted excerpt of the canonical schema for the LLM."""
        lines = [
            "### Canonical Contract Schema Fields",
            ""
        ]
        
        for field in self.canonical_schema.fields:
            field_line = f"- **{field.name}** ({field.type.value})"
            
            if field.required:
                field_line += " [REQUIRED]"
            
            if field.values:  # Enum values
                field_line += f" - Values: {', '.join(field.values)}"
            
            if field.description:
                field_line += f" - {field.description}"
            
            lines.append(field_line)
        
        # Add derived field information
        lines.extend([
            "",
            "### Derived Field Rules",
            "Some fields can be derived from combinations of other fields:",
            "",
            "- **expiry_date** can be derived from:",
            "  - effective_date + (renewal_term_months * 30) days",
            "  - status_date + days_remaining days",
            "",
            "- **contract_value_arr** can be derived from:",
            "  - contract_value_ltv / (renewal_term_months / 12)",
            "",
            "Consider these derivation rules when proposing mappings."
        ])
        
        return "\n".join(lines)
    
    def get_schema_field_names(self) -> List[str]:
        """Get list of all canonical field names."""
        return [field.name for field in self.canonical_schema.fields]
    
    def get_schema_field_by_name(self, field_name: str) -> Optional[dict]:
        """Get canonical field definition by name."""
        for field in self.canonical_schema.fields:
            if field.name == field_name:
                return {
                    "name": field.name,
                    "type": field.type.value,
                    "required": field.required,
                    "description": field.description,
                    "values": field.values
                }
        return None

