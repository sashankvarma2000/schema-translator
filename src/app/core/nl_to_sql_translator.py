"""
Natural Language to SQL Translator

This module translates natural language questions about contracts into canonical SQL queries
that can then be translated to customer-specific schemas using the existing query translator.
"""

import json
import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import yaml
from pathlib import Path

from ..adapters.llm_openai import OpenAIAdapter

logger = logging.getLogger(__name__)


class QueryIntent(Enum):
    """Types of query intents we can handle"""
    LIST_CONTRACTS = "list_contracts"
    FILTER_CONTRACTS = "filter_contracts"
    COUNT_CONTRACTS = "count_contracts"
    AGGREGATE_VALUES = "aggregate_values"
    COMPARE_PERIODS = "compare_periods"
    FIND_EXPIRING = "find_expiring"
    UNKNOWN = "unknown"


@dataclass
class FilterCondition:
    """Represents a filter condition extracted from natural language"""
    field: str
    operator: str  # =, >, <, >=, <=, LIKE, BETWEEN, IN
    value: Any
    confidence: float
    original_text: str


@dataclass
class DateRange:
    """Represents a date range"""
    start_date: str  # ISO format
    end_date: str    # ISO format
    confidence: float
    original_text: str


@dataclass
class IntentAnalysis:
    """Analysis of user's natural language query intent"""
    query_intent: QueryIntent
    primary_entity: str  # contracts, parties, awards, etc.
    requested_fields: List[str]
    filter_conditions: List[FilterCondition]
    date_ranges: List[DateRange]
    aggregations: List[str]  # COUNT, SUM, AVG, etc.
    sort_fields: List[Tuple[str, str]]  # [(field, direction)]
    confidence: float
    assumptions: List[str]
    clarifications_needed: List[str]
    original_query: str


@dataclass
class SQLGeneration:
    """Result of SQL generation from intent"""
    sql_query: str
    fields_used: List[str]
    tables_used: List[str]
    validation_status: str  # valid, warning, error
    confidence: float
    reasoning: str


class NLToSQLTranslator:
    """
    Translates natural language questions into canonical SQL queries
    """
    
    def __init__(self, llm_adapter: Optional[OpenAIAdapter] = None, canonical_schema_path: Optional[str] = None):
        """Initialize the NL to SQL translator"""
        self.llm_adapter = llm_adapter or OpenAIAdapter()
        self.logger = logging.getLogger(__name__)
        
        # Load canonical schema
        if canonical_schema_path:
            self.canonical_schema = self._load_canonical_schema(canonical_schema_path)
        else:
            # Default path
            schema_path = Path.cwd() / "canonical_schema_original.yaml"
            self.canonical_schema = self._load_canonical_schema(str(schema_path))
        
        # Common contract-related synonyms and mappings
        self.field_synonyms = {
            'value': ['amount', 'price', 'cost', 'worth', 'money'],
            'status': ['state', 'condition', 'phase'],
            'expiry': ['expiration', 'end', 'termination', 'completion'],
            'start': ['begin', 'commencement', 'inception'],
            'supplier': ['vendor', 'contractor', 'provider'],
            'buyer': ['client', 'customer', 'purchaser', 'agency'],
            'active': ['current', 'ongoing', 'live'],
            'expired': ['terminated', 'ended', 'completed', 'finished']
        }
        
        # Date period mappings
        self.date_mappings = {
            'q1': ('01-01', '03-31'),
            'q2': ('04-01', '06-30'),
            'q3': ('07-01', '09-30'),
            'q4': ('10-01', '12-31'),
            'first quarter': ('01-01', '03-31'),
            'second quarter': ('04-01', '06-30'),
            'third quarter': ('07-01', '09-30'),
            'fourth quarter': ('10-01', '12-31'),
        }
    
    def _load_canonical_schema(self, schema_path: str) -> Dict[str, Any]:
        """Load the canonical schema from YAML file"""
        try:
            with open(schema_path, 'r') as f:
                schema = yaml.safe_load(f)
            self.logger.info(f"Loaded canonical schema from {schema_path}")
            return schema
        except Exception as e:
            self.logger.error(f"Failed to load canonical schema: {e}")
            raise
    
    def translate_natural_language_to_sql(self, natural_query: str) -> Tuple[IntentAnalysis, SQLGeneration]:
        """
        Main method to translate natural language to SQL
        
        Args:
            natural_query: User's natural language question
            
        Returns:
            Tuple of (IntentAnalysis, SQLGeneration)
        """
        self.logger.info("=" * 60)
        self.logger.info("🗣️  NATURAL LANGUAGE TO SQL TRANSLATION")
        self.logger.info("=" * 60)
        self.logger.info(f"📝 User question: {natural_query}")
        
        # Step 1: Analyze user intent
        self.logger.info("🧠 STEP 1: Analyzing user intent...")
        intent_analysis = self._analyze_intent(natural_query)
        self.logger.info(f"✅ Intent analysis completed:")
        self.logger.info(f"   - Primary intent: {intent_analysis.query_intent.value}")
        self.logger.info(f"   - Entity: {intent_analysis.primary_entity}")
        self.logger.info(f"   - Filters: {len(intent_analysis.filter_conditions)}")
        self.logger.info(f"   - Confidence: {intent_analysis.confidence:.2f}")
        
        # Step 2: Generate canonical SQL
        self.logger.info("📝 STEP 2: Generating canonical SQL...")
        sql_generation = self._generate_canonical_sql(intent_analysis)
        self.logger.info(f"✅ SQL generation completed:")
        self.logger.info(f"   - Validation: {sql_generation.validation_status}")
        self.logger.info(f"   - Confidence: {sql_generation.confidence:.2f}")
        self.logger.info(f"   - Tables used: {sql_generation.tables_used}")
        
        self.logger.info("=" * 60)
        self.logger.info("✨ TRANSLATION COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info("📋 GENERATED SQL:")
        for i, line in enumerate(sql_generation.sql_query.split('\n'), 1):
            self.logger.info(f"   {i:2d}| {line}")
        self.logger.info("=" * 60)
        
        return intent_analysis, sql_generation
    
    def _analyze_intent(self, natural_query: str) -> IntentAnalysis:
        """Analyze the user's intent using LLM"""
        
        # Build schema context for LLM
        schema_context = self._build_schema_context_for_nl()
        
        prompt = f"""
You are an expert at understanding natural language questions about contract data and extracting structured intent.

## Available Canonical Schema
{schema_context}

## User's Question
"{natural_query}"

## Your Task
Analyze this natural language question and extract the user's intent in structured format.

Consider:
1. What is the user trying to find out? (list, count, filter, aggregate, compare)
2. Which entity are they asking about? (contracts, parties, awards, etc.)
3. What fields do they want to see in the results?
4. What filter conditions are implied?
5. Are there any date ranges mentioned?
6. Do they want aggregations (count, sum, average)?
7. How should results be sorted?

## Date Understanding
- "Q1 2025" = January 1, 2025 to March 31, 2025
- "next month" = calculate from current date
- "this year" = current calendar year
- "expiring soon" = within next 30-90 days

## Field Mapping
- "value", "amount", "worth", "cost" → value_amount
- "status", "state", "condition" → status
- "expiry", "expiration", "end date" → period_end
- "start", "begin" → period_start
- "supplier", "vendor", "contractor" → supplier info
- "buyer", "client", "agency" → buyer info

Respond with JSON following this structure:
{{
    "query_intent": "list_contracts|filter_contracts|count_contracts|aggregate_values|find_expiring",
    "primary_entity": "contracts",
    "requested_fields": ["contract_id", "value_amount", "status"],
    "filter_conditions": [
        {{
            "field": "status",
            "operator": "=",
            "value": "active",
            "confidence": 0.9,
            "original_text": "active contracts"
        }}
    ],
    "date_ranges": [
        {{
            "start_date": "2025-01-01",
            "end_date": "2025-03-31",
            "confidence": 0.9,
            "original_text": "Q1 2025"
        }}
    ],
    "aggregations": [],
    "sort_fields": [["value_amount", "DESC"]],
    "confidence": 0.85,
    "assumptions": ["'active' means status='active'", "Q1 means calendar quarter"],
    "clarifications_needed": [],
    "original_query": "{natural_query}"
}}
"""
        
        try:
            # Define JSON schema for structured output
            intent_schema = {
                "type": "object",
                "properties": {
                    "query_intent": {
                        "type": "string",
                        "enum": ["list_contracts", "filter_contracts", "count_contracts", "aggregate_values", "compare_periods", "find_expiring", "unknown"]
                    },
                    "primary_entity": {"type": "string"},
                    "requested_fields": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "filter_conditions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "field": {"type": "string"},
                                "operator": {"type": "string"},
                                "value": {"type": ["string", "number", "boolean", "null"]},
                                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                                "original_text": {"type": "string"}
                            },
                            "required": ["field", "operator", "value", "confidence", "original_text"],
                            "additionalProperties": False
                        }
                    },
                    "date_ranges": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "start_date": {"type": "string"},
                                "end_date": {"type": "string"},
                                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                                "original_text": {"type": "string"}
                            },
                            "required": ["start_date", "end_date", "confidence", "original_text"],
                            "additionalProperties": False
                        }
                    },
                    "aggregations": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "sort_fields": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 2,
                            "maxItems": 2
                        }
                    },
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "assumptions": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "clarifications_needed": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "original_query": {"type": "string"}
                },
                "required": ["query_intent", "primary_entity", "requested_fields", "filter_conditions", "date_ranges", "aggregations", "sort_fields", "confidence", "assumptions", "clarifications_needed", "original_query"],
                "additionalProperties": False
            }
            
            response = self.llm_adapter.generate_completion(prompt, json_schema=intent_schema)
            intent_data = json.loads(response) if isinstance(response, str) else response
            
            # Convert to IntentAnalysis object
            filter_conditions = [
                FilterCondition(
                    field=fc["field"],
                    operator=fc["operator"],
                    value=fc["value"],
                    confidence=fc["confidence"],
                    original_text=fc["original_text"]
                ) for fc in intent_data.get("filter_conditions", [])
            ]
            
            date_ranges = [
                DateRange(
                    start_date=dr["start_date"],
                    end_date=dr["end_date"],
                    confidence=dr["confidence"],
                    original_text=dr["original_text"]
                ) for dr in intent_data.get("date_ranges", [])
            ]
            
            return IntentAnalysis(
                query_intent=QueryIntent(intent_data.get("query_intent", "unknown")),
                primary_entity=intent_data.get("primary_entity", "contracts"),
                requested_fields=intent_data.get("requested_fields", []),
                filter_conditions=filter_conditions,
                date_ranges=date_ranges,
                aggregations=intent_data.get("aggregations", []),
                sort_fields=[(sf[0], sf[1]) for sf in intent_data.get("sort_fields", [])],
                confidence=intent_data.get("confidence", 0.0),
                assumptions=intent_data.get("assumptions", []),
                clarifications_needed=intent_data.get("clarifications_needed", []),
                original_query=natural_query
            )
            
        except Exception as e:
            self.logger.error(f"Failed to analyze intent: {e}")
            # Return fallback intent analysis
            return IntentAnalysis(
                query_intent=QueryIntent.UNKNOWN,
                primary_entity="contracts",
                requested_fields=["contract_id"],
                filter_conditions=[],
                date_ranges=[],
                aggregations=[],
                sort_fields=[],
                confidence=0.1,
                assumptions=[],
                clarifications_needed=["Could not understand the question"],
                original_query=natural_query
            )
    
    def _generate_canonical_sql(self, intent_analysis: IntentAnalysis) -> SQLGeneration:
        """Generate canonical SQL from intent analysis"""
        
        schema_context = self._build_schema_context_for_nl()
        
        # Convert intent analysis to prompt context
        intent_context = self._build_intent_context(intent_analysis)
        
        prompt = f"""
You are a SQL expert who writes queries against a canonical contract schema.

## Canonical Schema
{schema_context}

## User Intent Analysis
{intent_context}

## Your Task
Generate a SQL query that fulfills the user's intent using ONLY the canonical schema fields above.

Requirements:
1. Use ONLY table and column names from the canonical schema
2. Handle date ranges properly with BETWEEN or >= <= operators
3. Map status conditions appropriately
4. Include proper JOINs if multiple tables are needed
5. Add appropriate sorting (ORDER BY)
6. Use proper aggregation functions if needed
7. Ensure the query is syntactically correct
8. IMPORTANT: Use the EXACT filter conditions from the intent analysis

## Field Mapping Rules
- For contract value: Use contracts.value_amount
- For contract status: Use contracts.status (values: pending, active, cancelled, terminated, expired)
- For dates: Use contracts.period_start, contracts.period_end, contracts.date_signed
- For parties: JOIN with parties table using supplier_party_ids or buyer_party_id
- For contract ID: Use contracts.contract_id

## Status Mapping
- "active" → status = 'active'
- "expired" → status = 'expired'
- "terminated" → status = 'terminated'
- "pending" → status = 'pending'
- "cancelled" → status = 'cancelled'

Return a JSON response with:
- sql_query: The complete SQL query
- reasoning: Brief explanation of how you mapped the intent to SQL
- confidence: Your confidence score (0-1)

CRITICAL: Use the exact filter values from the intent analysis above. Do not substitute different values.
"""
        
        try:
            # Define schema for structured SQL generation
            sql_schema = {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": "The generated SQL query"
                    },
                    "reasoning": {
                        "type": "string", 
                        "description": "Brief explanation of how intent was mapped to SQL"
                    },
                    "confidence": {
                        "type": "number",
                        "description": "Confidence score 0-1"
                    }
                },
                "required": ["sql_query", "reasoning", "confidence"],
                "additionalProperties": False
            }
            
            # Generate SQL with structured output
            response = self.llm_adapter.generate_completion(prompt, json_schema=sql_schema)
            
            # Parse structured response
            sql_data = json.loads(response)
            sql_query = sql_data["sql_query"]
            
            # Validate the generated SQL
            validation_result = self._validate_canonical_sql(sql_query, intent_analysis)
            
            return SQLGeneration(
                sql_query=sql_query,
                fields_used=self._extract_fields_from_sql(sql_query),
                tables_used=self._extract_tables_from_sql(sql_query),
                validation_status=validation_result["status"],
                confidence=intent_analysis.confidence * 0.9,  # Slight reduction for SQL generation
                reasoning=f"Generated SQL from intent: {intent_analysis.query_intent.value}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate SQL: {e}")
            # Return fallback SQL
            return SQLGeneration(
                sql_query="SELECT contract_id FROM contracts LIMIT 10",
                fields_used=["contract_id"],
                tables_used=["contracts"],
                validation_status="error",
                confidence=0.1,
                reasoning=f"Fallback SQL due to error: {e}"
            )
    
    def _build_schema_context_for_nl(self) -> str:
        """Build schema context string for NL processing"""
        context_parts = []
        context_parts.append("## Available Tables and Fields")
        
        # Extract key tables from canonical schema
        if 'fields' in self.canonical_schema:
            for table_name, table_info in self.canonical_schema['fields'].items():
                context_parts.append(f"\n### {table_name}")
                context_parts.append(f"Description: {table_info.get('description', 'No description')}")
                
                if 'fields' in table_info:
                    context_parts.append("Fields:")
                    for field in table_info['fields']:
                        field_name = field.get('name', 'unknown')
                        field_type = field.get('type', 'unknown')
                        field_desc = field.get('description', 'No description')
                        context_parts.append(f"  - {field_name} ({field_type}): {field_desc}")
        
        return "\n".join(context_parts)
    
    def _build_intent_context(self, intent: IntentAnalysis) -> str:
        """Build intent context for SQL generation"""
        context_parts = []
        context_parts.append(f"Query Intent: {intent.query_intent.value}")
        context_parts.append(f"Primary Entity: {intent.primary_entity}")
        context_parts.append(f"Requested Fields: {', '.join(intent.requested_fields)}")
        
        if intent.filter_conditions:
            context_parts.append("\nFilter Conditions:")
            for fc in intent.filter_conditions:
                context_parts.append(f"  - {fc.field} {fc.operator} {fc.value} (from: '{fc.original_text}')")
        
        if intent.date_ranges:
            context_parts.append("\nDate Ranges:")
            for dr in intent.date_ranges:
                context_parts.append(f"  - {dr.start_date} to {dr.end_date} (from: '{dr.original_text}')")
        
        if intent.aggregations:
            context_parts.append(f"\nAggregations: {', '.join(intent.aggregations)}")
        
        if intent.sort_fields:
            context_parts.append("\nSorting:")
            for field, direction in intent.sort_fields:
                context_parts.append(f"  - {field} {direction}")
        
        if intent.assumptions:
            context_parts.append("\nAssumptions Made:")
            for assumption in intent.assumptions:
                context_parts.append(f"  - {assumption}")
        
        return "\n".join(context_parts)
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL from LLM response"""
        # Look for SQL between backticks
        sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL)
        if sql_match:
            return sql_match.group(1).strip()
        
        # Look for SQL starting with SELECT
        select_match = re.search(r'(SELECT\s+.*?)(?:\n\n|\Z)', response, re.DOTALL | re.IGNORECASE)
        if select_match:
            return select_match.group(1).strip()
        
        # Fallback: return the whole response
        return response.strip()
    
    def _validate_canonical_sql(self, sql_query: str, intent: IntentAnalysis) -> Dict[str, Any]:
        """Validate the generated SQL against canonical schema"""
        errors = []
        warnings = []
        
        # Basic SQL structure validation
        if not sql_query.upper().strip().startswith('SELECT'):
            errors.append("Query does not start with SELECT")
        
        # Check if tables exist in canonical schema
        tables_used = self._extract_tables_from_sql(sql_query)
        canonical_tables = set(self.canonical_schema.get('fields', {}).keys())
        
        for table in tables_used:
            if table not in canonical_tables:
                errors.append(f"Table '{table}' not found in canonical schema")
        
        # Check if fields exist in their respective tables
        fields_used = self._extract_fields_from_sql(sql_query)
        for field in fields_used:
            if '.' in field:
                table, column = field.split('.', 1)
                if table in canonical_tables:
                    table_fields = [f['name'] for f in self.canonical_schema['fields'][table].get('fields', [])]
                    if column not in table_fields:
                        errors.append(f"Field '{column}' not found in table '{table}'")
        
        # Determine status
        if errors:
            status = "error"
        elif warnings:
            status = "warning"
        else:
            status = "valid"
        
        return {
            "status": status,
            "errors": errors,
            "warnings": warnings
        }
    
    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """Extract table names from SQL query"""
        # Simple regex to find FROM and JOIN clauses
        table_pattern = r'(?:FROM|JOIN)\s+(\w+)'
        matches = re.findall(table_pattern, sql, re.IGNORECASE)
        return list(set(matches))
    
    def _extract_fields_from_sql(self, sql: str) -> List[str]:
        """Extract field references from SQL query"""
        # Extract SELECT fields
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        fields = []
        
        if select_match:
            select_clause = select_match.group(1)
            # Simple field extraction (could be more sophisticated)
            field_parts = select_clause.split(',')
            for part in field_parts:
                part = part.strip()
                # Extract table.column references
                if '.' in part:
                    field_ref = part.split()[0]  # Take first word (ignore AS aliases)
                    fields.append(field_ref)
        
        return fields
    
    def suggest_clarifications(self, intent: IntentAnalysis) -> List[str]:
        """Suggest clarifications for ambiguous queries"""
        clarifications = []
        
        if intent.confidence < 0.7:
            clarifications.append("I'm not entirely sure I understood your question correctly.")
        
        # Check for ambiguous value references
        for condition in intent.filter_conditions:
            if condition.field == 'value_amount' and condition.confidence < 0.8:
                clarifications.append("When you mentioned 'value', did you mean total contract value or annual revenue?")
        
        # Check for ambiguous date references
        for date_range in intent.date_ranges:
            if date_range.confidence < 0.8:
                clarifications.append(f"For the date range '{date_range.original_text}', did I interpret the dates correctly?")
        
        # Check for missing sort order
        if not intent.sort_fields and intent.query_intent in [QueryIntent.LIST_CONTRACTS, QueryIntent.FILTER_CONTRACTS]:
            clarifications.append("How would you like the results sorted? By value, date, or something else?")
        
        return clarifications
    
    def get_example_queries(self) -> List[Dict[str, str]]:
        """Get example natural language queries for the UI"""
        return [
            {
                "category": "Getting Started",
                "examples": [
                    "Show me all active contracts",
                    "Which contracts expire next month?",
                    "List all contracts from 2024"
                ]
            },
            {
                "category": "Value-Based Queries",
                "examples": [
                    "Show contracts worth more than $500,000",
                    "What are our highest value contracts?",
                    "Find contracts between $100K and $1M"
                ]
            },
            {
                "category": "Time-Based Queries",
                "examples": [
                    "Contracts expiring in the next 30 days",
                    "Show contracts created this quarter",
                    "Which contracts expired last year?"
                ]
            },
            {
                "category": "Party-Based Queries",
                "examples": [
                    "Show all contracts with Acme Corporation",
                    "Which suppliers have active contracts?",
                    "Find contracts for government agencies"
                ]
            },
            {
                "category": "Complex Queries",
                "examples": [
                    "Show me the total value of all active contracts",
                    "Which contracts were amended in the last 6 months?",
                    "Compare Q1 vs Q2 contract values"
                ]
            }
        ]
