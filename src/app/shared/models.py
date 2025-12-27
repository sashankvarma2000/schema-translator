"""Pydantic models for schema translation system."""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class ColumnType(str, Enum):
    """Supported column types."""
    STRING = "string"
    INT = "int"
    DECIMAL = "decimal"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"
    ENUM = "enum"


class ContractType(str, Enum):
    """Contract types."""
    MSA = "MSA"
    NDA = "NDA"
    SOW = "SOW"
    ORDER_FORM = "ORDER_FORM"
    OTHER = "OTHER"


class ContractStatus(str, Enum):
    """Contract statuses."""
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    TERMINATED = "TERMINATED"
    EXPIRED = "EXPIRED"


class CanonicalField(BaseModel):
    """Definition of a canonical schema field."""
    name: str
    type: ColumnType
    required: bool = False
    description: Optional[str] = None
    values: Optional[List[str]] = None  # For enum types
    precision: Optional[int] = None  # For decimal types
    scale: Optional[int] = None  # For decimal types
    currency_code: Optional[str] = None


class CanonicalSchema(BaseModel):
    """Complete canonical schema definition."""
    version: str
    description: Optional[str] = None
    last_updated: Optional[str] = None
    fields: List[CanonicalField]


class SourceColumn(BaseModel):
    """Source column information."""
    tenant: str
    table: str
    column: str
    type: Optional[str] = None
    description: Optional[str] = None
    nullable: bool = True


class ColumnProfile(BaseModel):
    """Statistical profile of a source column."""
    source_column: SourceColumn
    total_rows: int
    non_null_count: int
    distinct_count: int
    distinct_ratio: float
    sample_values: List[str] = Field(max_items=10)
    inferred_type: ColumnType
    date_patterns: List[str] = []
    currency_symbols: List[str] = []
    cooccurring_columns: List[str] = []
    
    @validator('distinct_ratio')
    def validate_distinct_ratio(cls, v: float) -> float:
        """Ensure distinct ratio is between 0 and 1."""
        return max(0.0, min(1.0, v))


class MappingProposal(BaseModel):
    """LLM proposal for mapping a source column to canonical field."""
    canonical_field: str
    justification: str
    transform_hint: Optional[str] = None
    assumptions: List[str] = []
    confidence: float = Field(ge=0.0, le=1.0)


class AlternativeMapping(BaseModel):
    """Alternative mapping suggestion."""
    canonical_field: str
    confidence: float = Field(ge=0.0, le=1.0)
    note: Optional[str] = None


class LLMResponse(BaseModel):
    """LLM response for column mapping."""
    proposed_mappings: List[MappingProposal]
    alternatives: List[AlternativeMapping] = []
    reasoning: Optional[str] = None


class MappingScore(BaseModel):
    """Combined scoring for a mapping proposal."""
    llm_confidence: float
    name_similarity: float
    type_compatibility: float
    value_range_match: float
    final_score: float
    auto_accept: bool
    needs_hitl: bool


class ColumnMapping(BaseModel):
    """Final mapping decision for a source column."""
    source_column: SourceColumn
    canonical_field: Optional[str] = None
    mapping_score: Optional[MappingScore] = None
    llm_response: Optional[LLMResponse] = None
    transform_rule: Optional[str] = None
    status: str = Field(default="pending")  # pending, accepted, rejected, hitl_required
    human_feedback: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class MappingPlan(BaseModel):
    """Complete mapping plan for a tenant."""
    tenant: str
    version: str
    canonical_schema_version: str
    mappings: List[ColumnMapping]
    coverage_stats: Dict[str, Any] = {}
    created_at: datetime = Field(default_factory=datetime.utcnow)
    approved_by: Optional[str] = None


class TransformResult(BaseModel):
    """Result of applying transforms to source data."""
    tenant: str
    source_table: str
    output_path: str
    rows_processed: int
    rows_successful: int
    errors: List[str] = []
    lineage: Dict[str, Any] = {}
    created_at: datetime = Field(default_factory=datetime.utcnow)


class HITLRequest(BaseModel):
    """Human-in-the-loop review request."""
    tenant: str
    source_column: SourceColumn
    proposed_mappings: List[MappingProposal]
    column_profile: ColumnProfile
    reasoning: str
    priority: str = "normal"  # low, normal, high
    created_at: datetime = Field(default_factory=datetime.utcnow)


class HITLResponse(BaseModel):
    """Human response to HITL request."""
    request_id: str
    decision: str  # accept, reject, modify
    selected_mapping: Optional[str] = None
    custom_mapping: Optional[MappingProposal] = None
    feedback: Optional[str] = None
    reviewer: str
    reviewed_at: datetime = Field(default_factory=datetime.utcnow)


class LineageRecord(BaseModel):
    """Data lineage tracking."""
    output_field: str
    source_columns: List[SourceColumn]
    transform_applied: str
    mapping_version: str
    prompt_version: str
    confidence_score: float
    created_at: datetime = Field(default_factory=datetime.utcnow)

