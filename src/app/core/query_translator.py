"""
Query Translation Engine for Multi-Table Schema Translation

This module provides sophisticated LLM-powered query translation that can take
standardized queries against a canonical schema and translate them into complex
multi-table JOINs for customers who split logical contract data across multiple tables.
"""

import json
import re
import logging
import time
import json
import os
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

from ..adapters.llm_openai import OpenAIAdapter
from ..shared.models import ColumnType
from .table_relationship_analyzer import TableRelationshipAnalyzer, RelationshipType, TableRelationship
from .config_manager import ConfigManager

logger = logging.getLogger(__name__)


class QueryComplexity(Enum):
    """Query complexity levels"""
    SIMPLE = "simple"           # Single table, no JOINs needed
    MODERATE = "moderate"       # 2-3 table JOINs
    COMPLEX = "complex"         # 4+ table JOINs with complex logic
    VERY_COMPLEX = "very_complex"  # Multiple entities, subqueries, etc.


@dataclass
class QueryAnalysis:
    """Analysis of a standardized query"""
    original_query: str
    complexity: QueryComplexity
    required_tables: List[str]
    required_fields: List[str]
    where_conditions: List[str]
    group_by_fields: List[str]
    order_by_fields: List[str]
    has_aggregations: bool
    has_subqueries: bool


@dataclass
class JoinStrategy:
    """Strategy for joining tables"""
    primary_table: str
    join_tables: List[Tuple[str, str, str]]  # (table, join_condition, join_type)
    join_order: List[str]
    confidence: float
    reasoning: str
    performance_notes: List[str] = field(default_factory=list)


@dataclass
class QueryTranslation:
    """Result of query translation"""
    original_query: str
    translated_query: str
    customer_schema: str
    join_strategy: JoinStrategy
    confidence: float
    reasoning: str
    performance_optimization: List[str]
    warnings: List[str]
    execution_plan: Dict[str, Any]
    validation_errors: List[str] = None


class QueryTranslationEngine:
    """
    Sophisticated query translation engine that uses LLM reasoning to translate
    standardized queries into customer-specific multi-table JOINs.
    """
    
    def __init__(self, llm_adapter: Optional[OpenAIAdapter] = None):
        """Initialize the query translation engine"""
        self.llm_adapter = llm_adapter or OpenAIAdapter()
        self.relationship_analyzer = TableRelationshipAnalyzer()
        self.config_manager = ConfigManager()
        self.logger = logging.getLogger(__name__)  # Initialize logger first
        self.cache_file = "cache/mapping_cache.json"
        self.mapping_cache = self._load_cache()  # Load persistent cache
        self.schema_cache = {}   # Cache loaded schemas
        
    def translate_query_original(
        self, 
        canonical_query: str, 
        customer_schema: Dict[str, Any],
        customer_id: str
    ) -> QueryTranslation:
        """
        Translate a standardized query into customer-specific SQL
        
        Args:
            canonical_query: Standardized query against canonical schema
            customer_schema: Customer's actual database schema
            customer_id: Identifier for the customer
            
        Returns:
            QueryTranslation with translated SQL and metadata
        """
        self.logger.info("🔴 ENTERING translate_query_original (OLD METHOD)")
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"🚀 STARTING QUERY TRANSLATION for customer {customer_id}")
            self.logger.info("=" * 80)
            self.logger.info(f"📝 Original query: {canonical_query}")
            
            # Step 1: Analyze the canonical query
            self.logger.info("📊 STEP 1: Analyzing canonical query...")
            query_analysis = self._analyze_canonical_query(canonical_query)
            self.logger.info(f"✅ Query analysis completed:")
            self.logger.info(f"   - Complexity: {query_analysis.complexity.value}")
            self.logger.info(f"   - Required fields: {query_analysis.required_fields}")
            self.logger.info(f"   - Required tables: {query_analysis.required_tables}")
            self.logger.info(f"   - Has aggregations: {query_analysis.has_aggregations}")
            self.logger.info(f"   - Has subqueries: {query_analysis.has_subqueries}")
            
            # Step 2: Discover table relationships
            self.logger.info("🔗 STEP 2: Discovering table relationships...")
            relationships = self.relationship_analyzer.discover_relationships(
                customer_schema, query_analysis.required_fields
            )
            self.logger.info(f"✅ Relationship discovery completed:")
            self.logger.info(f"   - Found {len(relationships)} relationships")
            for i, rel in enumerate(relationships[:3]):  # Show first 3
                self.logger.info(f"   - Relationship {i+1}: {rel.table1}.{rel.column1} → {rel.table2}.{rel.column2} ({rel.confidence:.2f})")
            if len(relationships) > 3:
                self.logger.info(f"   - ... and {len(relationships) - 3} more relationships")
            
            # Step 3: Generate JOIN strategy using LLM
            self.logger.info("🎯 STEP 3: Generating JOIN strategy...")
            join_strategy = self._generate_join_strategy(
                query_analysis, relationships, customer_schema
            )
            self.logger.info(f"✅ JOIN strategy generated:")
            self.logger.info(f"   - Primary table: {join_strategy.primary_table}")
            self.logger.info(f"   - JOIN tables: {len(join_strategy.join_tables)}")
            for i, join in enumerate(join_strategy.join_tables):
                self.logger.info(f"   - JOIN {i+1}: {join[2]} JOIN {join[0]} ON {join[1]}")
            self.logger.info(f"   - Strategy confidence: {join_strategy.confidence:.2f}")
            
            # Step 4: Translate query using LLM
            self.logger.info("🔄 STEP 4: Translating query with LLM...")
            translated_query = self._translate_with_llm(
                canonical_query, query_analysis, join_strategy, customer_schema, customer_id
            )
            self.logger.info("✅ Query translation with LLM completed")
            
            # Step 5: Validate and optimize
            self.logger.info("✅ STEP 5: Validating translated query...")
            validation_result = self._validate_translation(translated_query, customer_schema)
            
            if validation_result.get('valid', True):
                self.logger.info("✅ Query validation passed:")
                if validation_result.get('warnings'):
                    self.logger.info(f"   - Warnings: {len(validation_result['warnings'])}")
                    for warning in validation_result['warnings']:
                        self.logger.info(f"     ⚠️  {warning}")
                else:
                    self.logger.info("   - No validation issues found")
            else:
                self.logger.warning("❌ Initial translation failed validation:")
                for error in validation_result.get('errors', []):
                    self.logger.warning(f"   - ❌ {error}")
                
                # Try to regenerate with enhanced schema context
                self.logger.info("🔄 STEP 5b: Regenerating query with validation feedback...")
                translated_query = self._regenerate_with_validation_feedback(
                    canonical_query, query_analysis, join_strategy, customer_schema, validation_result
                )
                # Re-validate
                self.logger.info("🔍 STEP 5c: Re-validating regenerated query...")
                validation_result = self._validate_translation(translated_query, customer_schema)
                
                if validation_result.get('valid', True):
                    self.logger.info("✅ Regenerated query passed validation")
                else:
                    self.logger.error("❌ Regenerated query still has validation errors")
                    for error in validation_result.get('errors', []):
                        self.logger.error(f"   - ❌ {error}")
            
            # Step 6: Generate performance optimizations
            self.logger.info("⚡ STEP 6: Generating performance optimizations...")
            performance_optimization = self._suggest_optimizations(translated_query, join_strategy)
            self.logger.info(f"✅ Generated {len(performance_optimization)} optimization suggestions")
            
            # Step 7: Calculate final confidence
            self.logger.info("📊 STEP 7: Calculating final confidence score...")
            final_confidence = join_strategy.confidence
            if not validation_result.get('valid', True):
                final_confidence = max(0.1, final_confidence * 0.3)  # Severely reduce confidence for invalid queries
                self.logger.info(f"   - Confidence reduced due to validation errors: {final_confidence:.2f}")
            elif validation_result.get('warnings'):
                final_confidence = max(0.3, final_confidence * 0.8)  # Reduce confidence for warnings
                self.logger.info(f"   - Confidence reduced due to warnings: {final_confidence:.2f}")
            else:
                self.logger.info(f"   - Final confidence maintained: {final_confidence:.2f}")
            
            # Step 8: Generate execution plan
            self.logger.info("📋 STEP 8: Generating execution plan...")
            execution_plan = self._generate_execution_plan(translated_query, join_strategy)
            self.logger.info("✅ Execution plan generated")
            
            # Final result
            result = QueryTranslation(
                original_query=canonical_query,
                translated_query=translated_query,
                customer_schema=customer_id,
                join_strategy=join_strategy,
                confidence=final_confidence,
                reasoning=join_strategy.reasoning + (f"\n\nValidation: {validation_result}" if validation_result.get('errors') or validation_result.get('warnings') else ""),
                performance_optimization=performance_optimization,
                warnings=validation_result.get('warnings', []),
                execution_plan=execution_plan,
                validation_errors=validation_result.get('errors', [])
            )
            
            # Completion summary
            self.logger.info("=" * 80)
            self.logger.info("🎉 QUERY TRANSLATION COMPLETED SUCCESSFULLY!")
            self.logger.info("=" * 80)
            self.logger.info(f"📊 FINAL SUMMARY:")
            self.logger.info(f"   - Customer: {customer_id}")
            self.logger.info(f"   - Final confidence: {final_confidence:.2f}")
            self.logger.info(f"   - Validation status: {'✅ PASSED' if validation_result.get('valid', True) else '❌ FAILED'}")
            self.logger.info(f"   - Warnings: {len(validation_result.get('warnings', []))}")
            self.logger.info(f"   - Errors: {len(validation_result.get('errors', []))}")
            self.logger.info(f"   - Performance suggestions: {len(performance_optimization)}")
            self.logger.info(f"   - Tables involved: {len(execution_plan.get('tables_involved', []))}")
            self.logger.info(f"   - JOINs required: {execution_plan.get('join_count', 0)}")
            self.logger.info("📝 TRANSLATED QUERY:")
            # Log the translated query with line numbers for readability
            for i, line in enumerate(translated_query.split('\n'), 1):
                self.logger.info(f"   {i:2d}| {line}")
            self.logger.info("=" * 80)
            
            return result
            
        except Exception as e:
            self.logger.error("=" * 80)
            self.logger.error("💥 QUERY TRANSLATION FAILED!")
            self.logger.error("=" * 80)
            self.logger.error(f"❌ Error: {str(e)}")
            self.logger.error(f"📝 Original query: {canonical_query}")
            self.logger.error(f"🎯 Customer: {customer_id}")
            self.logger.error("=" * 80)
            raise
    
    def _analyze_canonical_query(self, query: str) -> QueryAnalysis:
        """Analyze the canonical query to understand its requirements"""
        # Parse SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
        required_fields = []
        if select_match:
            select_clause = select_match.group(1).strip()
            if select_clause == '*':
                required_fields = ['*']  # All fields
            else:
                required_fields = [field.strip() for field in select_clause.split(',')]
        
        # Parse FROM clause
        from_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
        primary_table = from_match.group(1) if from_match else 'contracts'
        
        # Parse WHERE clause
        where_conditions = []
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s*$)', query, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1).strip()
            # Simple split by AND/OR (could be more sophisticated)
            conditions = re.split(r'\s+(?:AND|OR)\s+', where_clause)
            where_conditions = [cond.strip() for cond in conditions]
        
        # Parse GROUP BY clause
        group_by_fields = []
        group_match = re.search(r'GROUP\s+BY\s+(.*?)(?:\s+ORDER\s+BY|\s*$)', query, re.IGNORECASE | re.DOTALL)
        if group_match:
            group_clause = group_match.group(1).strip()
            group_by_fields = [field.strip() for field in group_clause.split(',')]
        
        # Parse ORDER BY clause
        order_by_fields = []
        order_match = re.search(r'ORDER\s+BY\s+(.*?)$', query, re.IGNORECASE | re.DOTALL)
        if order_match:
            order_clause = order_match.group(1).strip()
            order_by_fields = [field.strip() for field in order_clause.split(',')]
        
        # Determine complexity
        has_aggregations = any(func in query.upper() for func in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN'])
        has_subqueries = '(' in query and 'SELECT' in query.upper()
        
        if has_subqueries:
            complexity = QueryComplexity.VERY_COMPLEX
        elif has_aggregations or len(where_conditions) > 2:
            complexity = QueryComplexity.COMPLEX
        elif len(where_conditions) > 0:
            complexity = QueryComplexity.MODERATE
        else:
            complexity = QueryComplexity.SIMPLE
        
        return QueryAnalysis(
            original_query=query,
            complexity=complexity,
            required_tables=[primary_table],
            required_fields=required_fields,
            where_conditions=where_conditions,
            group_by_fields=group_by_fields,
            order_by_fields=order_by_fields,
            has_aggregations=has_aggregations,
            has_subqueries=has_subqueries
        )
    
    def _generate_join_strategy(
        self, 
        query_analysis: QueryAnalysis, 
        relationships: List[TableRelationship], 
        customer_schema: Dict[str, Any]
    ) -> JoinStrategy:
        """Generate JOIN strategy using LLM reasoning"""
        
        # Build context for LLM
        schema_context = self._build_schema_context(customer_schema)
        query_context = self._build_query_context(query_analysis)
        relationships_context = self._build_relationships_context(relationships)
        
        prompt = f"""
You are a database expert specializing in query optimization and multi-table JOIN strategies.

## Customer Schema Context
{schema_context}

## Query Requirements
{query_context}

## Discovered Relationships
{relationships_context}

## Your Task
Analyze the query requirements and customer schema to determine the optimal JOIN strategy.

Consider:
1. Which tables contain the required fields?
2. What are the primary/foreign key relationships?
3. What is the optimal JOIN order for performance?
4. Are there any missing relationships that need to be inferred?

Respond with a JSON object containing:
- primary_table: The main table to start the query
- join_tables: List of [table_name, join_condition, join_type] tuples
- join_order: Optimal order for joining tables
- confidence: Confidence score (0.0-1.0)
- reasoning: Step-by-step explanation of the JOIN strategy
- performance_notes: Performance optimization suggestions

Example response:
{{
    "primary_table": "contract_headers",
    "join_tables": [
        ["contract_status_history", "contract_headers.id = contract_status_history.contract_id", "INNER"],
        ["renewal_schedule", "contract_headers.id = renewal_schedule.contract_id", "LEFT"]
    ],
    "join_order": ["contract_headers", "contract_status_history", "renewal_schedule"],
    "confidence": 0.85,
    "reasoning": "The query requires status and expiry_date fields. Status is in contract_status_history, expiry_date is in renewal_schedule. We start with contract_headers as the primary table and join the others.",
    "performance_notes": ["Add index on contract_id columns", "Consider filtering early in the query"]
}}
"""
        
        try:
            # Import the schema from the LLM adapter
            from ..adapters.llm_openai import JOIN_STRATEGY_SCHEMA
            response = self.llm_adapter.generate_completion(prompt, json_schema=JOIN_STRATEGY_SCHEMA)
            
            # With structured outputs, we can parse directly as JSON
            try:
                strategy_data = json.loads(response) if isinstance(response, str) else response
            except json.JSONDecodeError:
                # Fallback to the existing parsing method
                strategy_data = self.llm_adapter.parse_json_response(response)
            
            # Handle case where LLM returns a list instead of a dictionary
            if isinstance(strategy_data, list):
                self.logger.warning("JOIN strategy response is a list, expected dict. Using fallback.")
                strategy_data = {
                    'primary_table': 'awards',  # Default fallback for tenant_A
                    'join_tables': [],
                    'join_order': ['awards'],
                    'confidence': 0.5,
                    'reasoning': 'Fallback strategy due to parsing error',
                    'performance_notes': []
                }
            elif not isinstance(strategy_data, dict):
                self.logger.warning(f"JOIN strategy response is {type(strategy_data)}, expected dict. Using fallback.")
                strategy_data = {
                    'primary_table': 'awards',
                    'join_tables': [],
                    'join_order': ['awards'],
                    'confidence': 0.5,
                    'reasoning': 'Fallback strategy due to parsing error',
                    'performance_notes': []
                }
            
            # Validate and fix join_tables format
            join_tables: List[Tuple[str, str, str]] = []
            if 'join_tables' in strategy_data and strategy_data['join_tables']:
                for join in strategy_data['join_tables']:
                    if isinstance(join, (list, tuple)) and len(join) >= 3:
                        join_tables.append(tuple(join[:3]))  # Take first 3 elements
                    elif isinstance(join, dict):
                        # Handle dictionary format
                        table = join.get('table', '')
                        condition = join.get('condition', '')
                        join_type = join.get('type', 'INNER')
                        join_tables.append((table, condition, join_type))
                    else:
                        self.logger.warning(f"Invalid join format: {join}")
            
            return JoinStrategy(
                primary_table=strategy_data.get('primary_table', ''),
                join_tables=join_tables,
                join_order=strategy_data.get('join_order', []),
                confidence=float(strategy_data.get('confidence', 0.0)),
                reasoning=strategy_data.get('reasoning', ''),
                performance_notes=list(strategy_data.get('performance_notes', []) or [])
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate JOIN strategy: {str(e)}")
            raise RuntimeError(f"LLM failed to generate JOIN strategy: {str(e)}")
    
    def _translate_with_llm(
        self, 
        canonical_query: str, 
        query_analysis: QueryAnalysis, 
        join_strategy: JoinStrategy, 
        customer_schema: Dict[str, Any],
        customer_id: str
    ) -> str:
        """Use LLM to translate the query into customer-specific SQL"""
        
        schema_context = self._build_schema_context(customer_schema)
        
        # Build intelligent schema context with LLM-inferred mappings
        intelligent_schema_context = self._build_intelligent_schema_context(
            canonical_query, customer_schema, customer_id, query_analysis
        )
        
        prompt = f"""
You are a SQL expert specializing in query translation across different database schemas.

## Original Canonical Query
{canonical_query}

## Query Analysis
- Required fields: {query_analysis.required_fields}
- WHERE conditions: {query_analysis.where_conditions}
- GROUP BY: {query_analysis.group_by_fields}
- ORDER BY: {query_analysis.order_by_fields}
- Has aggregations: {query_analysis.has_aggregations}

## Intelligent Schema Analysis & Mapping Suggestions
{intelligent_schema_context}

## JOIN Strategy
- Primary table: {join_strategy.primary_table}
- Join tables: {join_strategy.join_tables}
- Join order: {join_strategy.join_order}
- Reasoning: {join_strategy.reasoning}

## Your Task
Translate the canonical query into customer-specific SQL using the comprehensive guide above.

Requirements:
1. **CRITICAL**: Follow the field mappings EXACTLY - never guess or substitute
2. **CRITICAL**: Use ONLY columns that exist in the schema structure above
3. Use the primary table as your main FROM table
4. Apply the JOIN strategy for multi-table relationships
5. Use proper table aliases for clarity (e.g., 'c' for contracts, 's' for suppliers)
6. Preserve all WHERE conditions, GROUP BY, ORDER BY clauses
7. Use appropriate JOIN types (INNER, LEFT, RIGHT) as specified in strategy
8. Add table prefixes to avoid column ambiguity
9. Ensure the query is syntactically correct

## Translation Priority:
1. Field mappings override schema inference
2. Direct mappings override derived logic
3. Constants (like 'USD') use exact values
4. Primary table should be the main table in FROM clause
5. Verify every column exists in schema before using

Respond with only the translated SQL query, no explanations.
"""
        
        try:
            response = self.llm_adapter.generate_completion(prompt)
            # Clean up the response to extract just the SQL
            sql_query = self._extract_sql_from_response(response)
            return sql_query
            
        except Exception as e:
            self.logger.error(f"Failed to translate query with LLM: {str(e)}")
            # Fallback to rule-based translation
            return self._translate_with_rules(canonical_query, query_analysis, join_strategy, customer_schema)
    
    def _build_schema_context(self, customer_schema: Dict[str, Any]) -> str:
        """Build context string for customer schema with sample data"""
        context_parts = []
        context_parts.append("=== CUSTOMER SCHEMA DEFINITION ===")
        context_parts.append("⚠️  CRITICAL: Only use columns listed below. Any column not listed does NOT exist!")
        
        # Handle case where customer_schema might be a list (defensive programming)
        if isinstance(customer_schema, list):
            self.logger.warning("⚠️ customer_schema is a list, attempting to convert to dict format")
            # If it's a list, it might be a list of tables - try to convert
            schema_dict = {'tables': {}}
            for item in customer_schema:
                if isinstance(item, dict) and 'name' in item:
                    schema_dict['tables'][item['name']] = item
            customer_schema = schema_dict
        
        for table_name, table_info in customer_schema.get('tables', {}).items():
            context_parts.append(f"\n📋 Table: {table_name}")
            context_parts.append(f"Description: {table_info.get('description', 'No description')}")
            
            # List all available columns explicitly
            columns = table_info.get('columns', {})
            if columns:
                context_parts.append("✅ AVAILABLE COLUMNS (use ONLY these):")
                for col_name, col_info in columns.items():
                    col_type = col_info.get('type', 'unknown')
                    col_desc = col_info.get('description', 'No description')
                    context_parts.append(f"  ✓ {col_name} ({col_type}): {col_desc}")
                
                # Explicitly state common missing columns
                common_missing = []
                if 'status' not in columns:
                    common_missing.append("'status' - use period_start/period_end dates or action_type instead")
                if 'contract_id' not in columns and 'id' not in columns:
                    if 'generated_unique_award_id' in columns:
                        common_missing.append("'contract_id' - use generated_unique_award_id instead")
                    elif 'piid' in columns:
                        common_missing.append("'contract_id' - use piid instead")
                
                if common_missing:
                    context_parts.append("❌ MISSING COLUMNS (do NOT use these):")
                    for missing in common_missing:
                        context_parts.append(f"  ✗ {missing}")
            
            # Add sample data if available
            sample_data = self._get_sample_data(table_name, customer_schema.get('tenant', 'tenant_A'))
            if sample_data:
                context_parts.append(f"\n📊 Sample Data for {table_name}:")
                context_parts.append(sample_data)
        
        context_parts.append("\n⚠️  REMINDER: Verify every column exists before using it in your SQL!")
        return "\n".join(context_parts)
    
    def _get_sample_data(self, table_name: str, tenant_id: str = "tenant_A") -> str:
        """Get sample data for a table to help LLM understand actual values"""
        try:
            import csv
            from pathlib import Path
            
            # Try to find sample data file
            sample_file = Path.cwd() / "customer_samples" / tenant_id / f"{table_name}.csv"
            if not sample_file.exists():
                return ""
            
            # Read first few rows of CSV
            sample_lines = []
            with open(sample_file, 'r') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    sample_lines.append("  Headers: " + ", ".join(header))
                    
                    # Get first 2-3 sample rows
                    for i, row in enumerate(reader):
                        if i >= 2:  # Limit to 2 sample rows
                            break
                        sample_lines.append(f"  Row {i+1}: " + ", ".join(row[:5]) + ("..." if len(row) > 5 else ""))
                    
                    # Show unique values for key columns that might indicate status/type
                    f.seek(0)
                    reader = csv.DictReader(f)
                    
                    # Collect unique values for columns that might contain status/type info
                    status_columns = ['status', 'award_type', 'action_type', 'contract_type', 'type']
                    unique_values = {}
                    
                    for row in reader:
                        for col in status_columns:
                            if col in row and row[col]:
                                if col not in unique_values:
                                    unique_values[col] = set()
                                unique_values[col].add(row[col])
                                if len(unique_values[col]) >= 5:  # Limit to 5 unique values
                                    break
                    
                    # Add unique values info
                    for col, values in unique_values.items():
                        if values:
                            sample_lines.append(f"  Unique {col} values: {', '.join(list(values)[:5])}")
            
            return "\n".join(sample_lines)
            
        except Exception as e:
            self.logger.debug(f"Could not load sample data for {table_name}: {e}")
            return ""
    
    def _build_query_context(self, query_analysis: QueryAnalysis) -> str:
        """Build context string for query analysis"""
        return f"""
Query Complexity: {query_analysis.complexity.value}
Required Fields: {', '.join(query_analysis.required_fields)}
WHERE Conditions: {', '.join(query_analysis.where_conditions)}
GROUP BY Fields: {', '.join(query_analysis.group_by_fields)}
ORDER BY Fields: {', '.join(query_analysis.order_by_fields)}
Has Aggregations: {query_analysis.has_aggregations}
Has Subqueries: {query_analysis.has_subqueries}
"""
    
    def _build_relationships_context(self, relationships: List[TableRelationship]) -> str:
        """Build context string for discovered relationships"""
        if not relationships:
            return "No relationships discovered"
        
        context_parts = []
        for rel in relationships:
            context_parts.append(f"- {rel.table1}.{rel.column1} -> {rel.table2}.{rel.column2} ({rel.relationship_type.value})")
        
        return "\n".join(context_parts)
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL query from LLM response"""
        # Look for SQL between backticks or after "SELECT"
        sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL)
        if sql_match:
            return sql_match.group(1).strip()
        
        # Look for SQL starting with SELECT
        select_match = re.search(r'(SELECT\s+.*?)(?:\n\n|\Z)', response, re.DOTALL | re.IGNORECASE)
        if select_match:
            return select_match.group(1).strip()
        
        # Fallback: return the whole response
        return response.strip()
    
    def _translate_with_rules(
        self, 
        canonical_query: str, 
        query_analysis: QueryAnalysis, 
        join_strategy: JoinStrategy, 
        customer_schema: Dict[str, Any]
    ) -> str:
        """Fallback rule-based translation when LLM fails"""
        # This is a simplified rule-based approach
        # In practice, this would be much more sophisticated
        
        primary_table = join_strategy.primary_table
        translated_query = canonical_query.replace('contracts', primary_table)
        
        # Add JOINs if needed
        if join_strategy.join_tables:
            from_clause = f"FROM {primary_table}"
            for table, condition, join_type in join_strategy.join_tables:
                from_clause += f" {join_type} JOIN {table} ON {condition}"
            
            translated_query = re.sub(
                r'FROM\s+\w+', 
                from_clause, 
                translated_query, 
                flags=re.IGNORECASE
            )
        
        return translated_query
    
    
    
    def _validate_translation(self, translated_query: str, customer_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the translated query against the customer schema"""
        errors = []
        warnings = []
        
        # Check for basic SQL syntax
        if not translated_query.upper().startswith('SELECT'):
            warnings.append("Query does not start with SELECT")
        
        # Check for common problematic patterns
        if '.status' in translated_query.lower():
            # Check if any table actually has a status column
            has_status = False
            for table_name, table_info in customer_schema.get('tables', {}).items():
                if 'status' in table_info.get('columns', {}):
                    has_status = True
                    break
            if not has_status:
                errors.append("Query references '.status' column but no table in this schema has a 'status' column. Use period_start/period_end dates or action_type instead.")
        
        # Extract table and column references from SQL
        table_refs, column_refs = self._extract_sql_references(translated_query)
        
        # Validate table names exist in schema
        schema_tables = set(customer_schema.get('tables', {}).keys())
        for table in table_refs:
            if table not in schema_tables:
                errors.append(f"❌ Table '{table}' does not exist in customer schema. Available tables: {', '.join(sorted(schema_tables))}")
        
        # Validate column names exist in their respective tables
        for table, columns in column_refs.items():
            if table in customer_schema.get('tables', {}):
                schema_columns = set(customer_schema['tables'][table].get('columns', {}).keys())
                for column in columns:
                    if column not in schema_columns:
                        errors.append(f"❌ Column '{column}' does not exist in table '{table}'. Available columns: {', '.join(sorted(schema_columns))}")
                        
                        # Provide specific suggestions for common fields
                        if column.lower() == 'status':
                            if 'period_start' in schema_columns and 'period_end' in schema_columns:
                                warnings.append(f"💡 For 'status' in '{table}', use: CASE WHEN period_end >= CURRENT_DATE THEN 'active' ELSE 'inactive' END")
                            elif 'action_type' in schema_columns:
                                warnings.append(f"💡 For 'status' in '{table}', consider using 'action_type' column")
                        elif column.lower() == 'contract_id':
                            if 'generated_unique_award_id' in schema_columns:
                                warnings.append(f"💡 For 'contract_id' in '{table}', use 'generated_unique_award_id'")
                            elif 'piid' in schema_columns:
                                warnings.append(f"💡 For 'contract_id' in '{table}', use 'piid'")
                        else:
                            # Suggest similar columns
                            suggestions = self._suggest_similar_columns(column, schema_columns)
                            if suggestions:
                                warnings.append(f"💡 Did you mean one of these columns in '{table}': {', '.join(suggestions)}?")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    def _extract_sql_references(self, sql: str) -> tuple:
        """Extract table and column references from SQL"""
        import re
        
        # Extract table names (FROM and JOIN clauses)
        table_pattern = r'(?:FROM|JOIN)\s+(\w+)(?:\s+\w+)?'
        table_matches = re.findall(table_pattern, sql, re.IGNORECASE)
        table_refs = set(table_matches)
        
        # Extract column references (table.column format)
        column_pattern = r'(\w+)\.(\w+)'
        column_matches = re.findall(column_pattern, sql)
        
        column_refs = {}
        for table, column in column_matches:
            if table not in column_refs:
                column_refs[table] = set()
            column_refs[table].add(column)
        
        return table_refs, column_refs
    
    def _suggest_similar_columns(self, target_column: str, available_columns: set) -> list:
        """Suggest similar column names using simple string similarity"""
        suggestions = []
        target_lower = target_column.lower()
        
        for col in available_columns:
            col_lower = col.lower()
            # Check for partial matches or similar names
            if (target_lower in col_lower or col_lower in target_lower or 
                self._levenshtein_distance(target_lower, col_lower) <= 2):
                suggestions.append(col)
        
        return suggestions[:3]  # Return top 3 suggestions
    
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings"""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _regenerate_with_validation_feedback(
        self, 
        canonical_query: str, 
        query_analysis: QueryAnalysis, 
        join_strategy: JoinStrategy, 
        customer_schema: Dict[str, Any], 
        validation_result: Dict[str, Any]
    ) -> str:
        """Regenerate query with validation feedback to fix errors"""
        try:
            # Build enhanced prompt with validation feedback
            schema_context = self._build_schema_context(customer_schema)
            
            # Create specific error guidance
            error_guidance = []
            for error in validation_result.get('errors', []):
                if 'status' in error.lower():
                    error_guidance.append("🔧 FIX FOR STATUS: Create a CASE statement using period_start/period_end dates to determine if a contract is 'active' or 'inactive'")
                elif 'contract_id' in error.lower():
                    error_guidance.append("🔧 FIX FOR CONTRACT_ID: Use 'generated_unique_award_id' as the contract identifier")
                else:
                    error_guidance.append(f"🔧 ERROR: {error}")
            
            error_context = "\n".join([
                "🚨 CRITICAL VALIDATION ERRORS - MUST FIX:",
                *error_guidance,
                "\n💡 HELPFUL SUGGESTIONS:",
                *validation_result.get('warnings', []),
                "\n⚠️  The query MUST use only existing columns from the schema above!"
            ])
            
            prompt = f"""
🔧 SQL ERROR CORRECTION TASK

You are fixing a SQL query that failed validation due to non-existent columns.

{schema_context}

ORIGINAL CANONICAL QUERY:
{canonical_query}

VALIDATION ERRORS TO FIX:
{error_context}

🎯 CORRECTION REQUIREMENTS:
1. ✅ MANDATORY: Use ONLY columns listed in the schema above
2. ✅ For 'status' field: Create CASE statement with period_start/period_end dates
3. ✅ For 'contract_id' field: Use 'generated_unique_award_id' 
4. ✅ Maintain the business logic intent of the original query
5. ✅ Test every column name against the schema before using it

EXAMPLE CORRECTION for tenant_A:
Instead of: SELECT contract_id, status FROM awards WHERE status = 'active'
Use this: SELECT generated_unique_award_id AS contract_id, 
                 CASE WHEN period_end >= CURRENT_DATE THEN 'active' ELSE 'inactive' END AS status
          FROM awards 
          WHERE period_end >= CURRENT_DATE

Generate the corrected SQL query:
"""

            response = self.llm_adapter.generate_completion(prompt)
            corrected_sql = self._extract_sql_from_response(response)
            
            self.logger.info("Regenerated query with validation feedback")
            return corrected_sql
            
        except Exception as e:
            self.logger.error(f"Failed to regenerate query: {e}")
            # Return a basic fallback
            return f"-- ERROR: Could not generate valid SQL for this schema\n-- Original query: {canonical_query}"
    
    def _suggest_optimizations(self, translated_query: str, join_strategy: JoinStrategy) -> List[str]:
        """Suggest performance optimizations"""
        optimizations = []
        
        # Add index suggestions
        for table, condition, _ in join_strategy.join_tables:
            if '=' in condition:
                column = condition.split('=')[0].split('.')[-1].strip()
                optimizations.append(f"Consider adding index on {table}.{column}")
        
        # JOIN order optimization
        if len(join_strategy.join_tables) > 2:
            optimizations.append("Consider optimizing JOIN order for better performance")
        
        # WHERE clause optimization
        if 'WHERE' in translated_query.upper():
            optimizations.append("Ensure WHERE conditions use indexed columns")
        
        return optimizations
    
    def translate_query(
        self, 
        canonical_query: str, 
        customer_schema: Dict[str, Any],
        customer_id: str
    ) -> QueryTranslation:
        """
        OPTIMIZED: Translate a canonical query to customer-specific SQL using intelligent caching
        """
        self.logger.info("🔵 ENTERING translate_query (OPTIMIZED)")
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"🚀 OPTIMIZED QUERY TRANSLATION for customer {customer_id}")
            self.logger.info("=" * 80)
            
            # Step 1: Get or discover field mappings (cached after first use)
            start_time = time.time()
            mappings = self.get_or_discover_mappings(customer_id, customer_schema)
            mapping_time = time.time() - start_time
            self.logger.info(f"✅ Mappings retrieved in {mapping_time:.2f}s")
            
            # Step 2: Analyze the canonical query
            query_analysis = self._analyze_canonical_query(canonical_query)
            
            # Step 3: Check if this is a simple query that can use cached mappings directly
            if self._is_simple_query(canonical_query, query_analysis, mappings):
                self.logger.info("⚡ Using FAST PATH - direct mapping application")
                start_time = time.time()
                translated_query = self._apply_cached_mappings(canonical_query, mappings, query_analysis)
                translation_time = time.time() - start_time
                self.logger.info(f"✅ Fast translation completed in {translation_time:.2f}s")
                
                # Create simplified result for fast path
                result = QueryTranslation(
                    original_query=canonical_query,
                    translated_query=translated_query,
                    customer_schema=customer_id,
                    join_strategy=JoinStrategy(
                        primary_table=mappings.get("table_mappings", {}).get("contracts", "contracts"),
                        join_tables=[],
                        join_order=[],
                confidence=mappings.get("confidence_score", 0.9),
                reasoning="Fast path using cached field mappings",
                performance_notes=["Cached mapping fast-path used"]
                    ),
                    confidence=mappings.get("confidence_score", 0.9),
                    reasoning="Fast path translation using cached field mappings",
                    performance_optimization=["Query used optimized caching path"],
                    warnings=[],
                    execution_plan={
                        "tables_involved": [mappings.get("table_mappings", {}).get("contracts", "contracts")],
                        "join_count": 0,
                        "estimated_complexity": "simple"
                    },
                    validation_errors=[]
                )
                
            else:
                self.logger.info("🧠 Using COMPLEX PATH - LLM with mapping context")
                # Use complex translation with cached mappings
                start_time = time.time()
                
                # Debug: Check customer_schema type
                self.logger.debug(f"customer_schema type: {type(customer_schema)}")
                if isinstance(customer_schema, dict):
                    self.logger.debug(f"customer_schema keys: {list(customer_schema.keys())[:5]}")
                
                # We need to get join_strategy for the complex path
                # First discover relationships
                discovered_relationships = self.relationship_analyzer.discover_relationships(
                    customer_schema,
                    query_analysis.required_fields if hasattr(query_analysis, 'required_fields') else None
                )
                
                # Then generate join strategy with correct parameter order
                join_strategy = self._generate_join_strategy(
                    query_analysis, 
                    discovered_relationships,  # 2nd param: relationships
                    customer_schema            # 3rd param: customer_schema
                )
                
                translated_sql = self._translate_complex_with_mappings(
                    canonical_query,
                    query_analysis,
                    join_strategy,
                    customer_schema,
                    mappings
                )
                
                translation_time = time.time() - start_time
                self.logger.info(f"✅ Complex translation completed in {translation_time:.2f}s")
                
                # STEP 1: Validate complex mappings were applied
                validation_result = self._validate_complex_mappings_applied(
                    translated_sql, 
                    mappings, 
                    customer_schema
                )
                
                if not validation_result["valid"]:
                    self.logger.warning(f"⚠️ Complex mapping validation failed: {validation_result['reason']}")
                    self.logger.info("🔄 Regenerating with stricter instructions...")
                    
                    # Regenerate with explicit complex mapping injection
                    translated_sql = self._regenerate_with_explicit_mappings(
                        canonical_query,
                        query_analysis,
                        join_strategy,
                        customer_schema,
                        mappings,
                        validation_result
                    )
                    self.logger.info("✅ Regeneration completed with explicit mappings")
                
                # STEP 2: Validate against tenant schema (tables and columns exist)
                self.logger.info("🔍 Validating query against tenant schema...")
                schema_validation = self._validate_query_against_schema(
                    translated_sql,
                    customer_schema,
                    customer_id
                )
                
                if not schema_validation["valid"]:
                    self.logger.warning(f"⚠️ Schema validation failed: {len(schema_validation['errors'])} errors")
                    self.logger.info("🔄 Regenerating with full schema context...")
                    
                    # Regenerate with explicit tenant schema context
                    translated_sql = self._regenerate_with_schema_validation(
                        canonical_query,
                        customer_schema,
                        customer_id,
                        mappings,
                        schema_validation["errors"]
                    )
                    
                    # Re-validate the regenerated query
                    self.logger.info("🔍 Re-validating regenerated query...")
                    schema_validation = self._validate_query_against_schema(
                        translated_sql,
                        customer_schema,
                        customer_id
                    )
                    
                    if schema_validation["valid"]:
                        self.logger.info("✅ Regenerated query passed schema validation!")
                    else:
                        self.logger.error(f"❌ Regenerated query still has {len(schema_validation['errors'])} schema errors")
                
                result = QueryTranslation(
                    original_query=canonical_query,  # REQUIRED: Add original query
                    translated_query=translated_sql,
                    customer_schema=customer_id,  # REQUIRED: Add customer schema ID
                    join_strategy=join_strategy,
                    confidence=mappings.get("confidence_score", 0.85),
                    reasoning="Complex query translated using LLM with cached field mappings and schema validation",
                    performance_optimization=[],
                    warnings=validation_result.get("warnings", []) + schema_validation.get("warnings", []),
                    execution_plan={
                        'tables_involved': [join_strategy.primary_table] if join_strategy.primary_table else [],
                        'join_count': len(join_strategy.join_tables) if join_strategy.join_tables else 0,
                        'estimated_complexity': 'complex'
                    },
                    validation_errors=schema_validation.get("errors", [])
                )
            
            # Final summary
            total_time = mapping_time + (translation_time if 'translation_time' in locals() else 0)
            self.logger.info("=" * 80)
            self.logger.info("🎉 OPTIMIZED TRANSLATION COMPLETED!")
            self.logger.info("=" * 80)
            self.logger.info(f"📊 PERFORMANCE SUMMARY:")
            self.logger.info(f"   - Total time: {total_time:.2f}s")
            self.logger.info(f"   - Mapping lookup: {mapping_time:.2f}s")
            self.logger.info(f"   - Cache status: {'HIT' if customer_id in self.mapping_cache else 'MISS'}")
            self.logger.info(f"   - Translation path: {'FAST' if self._is_simple_query(canonical_query, query_analysis) else 'COMPLEX'}")
            self.logger.info(f"   - Final confidence: {result.confidence:.2f}")
            self.logger.info("=" * 80)
            
            return result
            
        except Exception as e:
            import traceback
            self.logger.error(f"❌ Optimized translation failed: {str(e)}")
            self.logger.error(f"❌ Traceback: {traceback.format_exc()}")
            # Fallback to original method
            self.logger.warning(f"⚠️ Falling back to original translation method")
            return self.translate_query_original(canonical_query, customer_schema, customer_id)

    def get_or_discover_mappings(self, customer_id: str, customer_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Get cached mappings or discover them once per tenant"""
        if customer_id not in self.mapping_cache:
            self.logger.info(f"🔍 First-time schema discovery for {customer_id}")
            mappings = self._discover_schema_mappings(customer_id, customer_schema)
            # Initialize usage tracking
            mappings['usage_count'] = 1
            mappings['last_used'] = time.time()
            self.mapping_cache[customer_id] = mappings
            self._save_cache()  # Persist to disk
            self.logger.info(f"✅ Cached mappings for {customer_id}: {len(mappings.get('field_mappings', {}))} field mappings")
        else:
            self.logger.info(f"⚡ Using cached mappings for {customer_id}")
            # Update usage tracking
            self.mapping_cache[customer_id]['usage_count'] = self.mapping_cache[customer_id].get('usage_count', 0) + 1
            self.mapping_cache[customer_id]['last_used'] = time.time()
            self._save_cache()  # Persist usage updates
        
        return self.mapping_cache[customer_id]
    
    def _load_cache(self) -> Dict[str, Any]:
        """Load mapping cache from disk"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    self.logger.info(f"💾 Loaded cache for {len(cache_data)} tenants from disk")
                    return cache_data
        except Exception as e:
            self.logger.warning(f"Failed to load cache: {e}")
        return {}
    
    def _save_cache(self):
        """Save mapping cache to disk"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.mapping_cache, f, indent=2, default=str)
            self.logger.info(f"💾 Saved cache for {len(self.mapping_cache)} tenants to disk")
        except Exception as e:
            self.logger.error(f"Failed to save cache: {e}")
    
    def _discover_schema_mappings(self, customer_id: str, customer_schema: Dict[str, Any]) -> Dict[str, Any]:
        """One-time LLM-powered schema analysis and mapping discovery"""
        try:
            # Build comprehensive schema analysis prompt
            discovery_prompt = f"""
You are a database schema expert. Analyze this customer schema and create field mappings to our canonical schema.

=== CANONICAL SCHEMA (TARGET) ===
Tables: contracts, parties, releases, awards, items, documents
Key fields:
- contracts: contract_id, title, status, date_signed, period_start, period_end, value_amount, value_currency, buyer_party_id, supplier_party_ids
- parties: party_id, name, role, contact_email
- Other tables as needed

=== CUSTOMER SCHEMA (SOURCE) ===
{json.dumps(customer_schema, indent=2)}

=== YOUR TASK ===
Create CLEAN, EXECUTABLE field mappings. Return JSON with these rules:

CRITICAL RULES FOR MAPPINGS:
1. target_field must be JUST the column name (e.g., "contract_value_usd") NOT explanations
2. constant_value must be the actual value (e.g., "USD") NOT explanatory text
3. logic must be valid SQL ONLY (no "Use this..." or "Set because..." text)
4. Keep reasoning field for explanations - NEVER put explanations in target_field or logic

{{
  "field_mappings": {{
    "value_currency": {{
      "target_field": null,
      "confidence": 0.90,
      "reasoning": "Currency is implicit in contract_value_usd column name",
      "transformation": "constant",
      "constant_value": "USD"
    }},
    "value_amount": {{
      "target_field": "contract_value_usd",
      "confidence": 0.95,
      "reasoning": "Direct mapping to monetary value column",
      "transformation": "none",
      "constant_value": null
    }}
  }},
  "table_mappings": {{
    "contracts": "customer_table_name"
  }},
  "complex_mappings": [
    {{
      "canonical_field": "status",
      "logic": "CASE WHEN end_date IS NULL THEN 'active' ELSE 'closed' END",
      "confidence": 0.85,
      "reasoning": "Derive status from end_date presence"
    }}
  ]
}}

Focus on high-confidence mappings. Keep reasoning separate from executable code.
"""

            # Use LLM to discover mappings
            response = self.llm_adapter.generate_completion(discovery_prompt)
            mappings_data = json.loads(response)
            
            # Validate and process mappings
            processed_mappings = self._process_discovered_mappings(mappings_data, customer_schema)
            
            return processed_mappings
            
        except Exception as e:
            self.logger.error(f"Failed to discover mappings for {customer_id}: {e}")
            return self._fallback_mappings(customer_schema)
    
    def _process_discovered_mappings(self, mappings_data: Dict, customer_schema: Dict) -> Dict[str, Any]:
        """Process and validate discovered mappings"""
        processed = {
            "field_mappings": {},
            "table_mappings": {},
            "complex_mappings": [],
            "confidence_score": 0.0,
            "discovery_timestamp": time.time(),
            "needs_review": []
        }
        
        # Process field mappings
        total_confidence = 0
        mapping_count = 0
        
        for canonical_field, mapping_info in mappings_data.get("field_mappings", {}).items():
            confidence = mapping_info.get("confidence", 0.0)
            
            if confidence >= 0.7:  # High confidence threshold
                processed["field_mappings"][canonical_field] = mapping_info
                total_confidence += confidence
                mapping_count += 1
            else:
                processed["needs_review"].append({
                    "field": canonical_field,
                    "mapping": mapping_info,
                    "reason": "Low confidence"
                })
        
        # Calculate overall confidence
        if mapping_count > 0:
            processed["confidence_score"] = total_confidence / mapping_count
        
        # Store other mappings
        processed["table_mappings"] = mappings_data.get("table_mappings", {})
        processed["complex_mappings"] = mappings_data.get("complex_mappings", [])
        
        return processed
    
    def _is_simple_query(self, canonical_query: str, query_analysis, mappings: Dict = None) -> bool:
        """Determine if query can use fast path with cached mappings"""
        # Simple queries: no JOINs, no subqueries, basic WHERE clauses
        query_upper = canonical_query.upper()
        
        # If mappings provided, check for derived fields in the query
        if mappings:
            field_mappings = mappings.get("field_mappings", {})
            for field_key, mapping_info in field_mappings.items():
                # Extract just the field name (e.g., "status" from "contracts.status")
                field_name = field_key.split('.')[-1] if '.' in field_key else field_key
                
                # Check if this field appears in the query
                if field_name.upper() in query_upper:
                    # Check if it's a derived field or has no target
                    if mapping_info.get("transformation") == "derived":
                        self.logger.info(f"🚫 Query contains derived field '{field_name}' - cannot use fast path")
                        return False
                    if mapping_info.get("target_field") is None:
                        self.logger.info(f"🚫 Query contains null-target field '{field_name}' - cannot use fast path")
                        return False
        
        simple_indicators = [
            'JOIN' not in query_upper,
            'UNION' not in query_upper,
            'SUBQUERY' not in query_upper,
            '(' not in canonical_query,  # No complex expressions
            len(query_analysis.required_tables) <= 1,
            not query_analysis.has_aggregations or len(query_analysis.group_by_fields) == 0
        ]
        
        return sum(simple_indicators) >= 4  # Most indicators suggest simplicity
    
    def _apply_cached_mappings(self, canonical_query: str, mappings: Dict, query_analysis) -> str:
        """Apply cached field mappings directly for simple queries"""
        try:
            translated_query = canonical_query
            field_mappings = mappings.get("field_mappings", {})
            table_mappings = mappings.get("table_mappings", {})
            
            # Replace table names
            for canonical_table, customer_table in table_mappings.items():
                # Skip tables that don't exist in customer schema
                if customer_table is None:
                    self.logger.debug(f"Skipping table '{canonical_table}' - no mapping in customer schema")
                    continue
                
                # Use word boundaries to avoid partial matches
                translated_query = re.sub(
                    rf'\b{canonical_table}\b', 
                    customer_table, 
                    translated_query, 
                    flags=re.IGNORECASE
                )
            
            # Replace field names with confidence-based selection
            for canonical_field, mapping_info in field_mappings.items():
                if mapping_info.get("confidence", 0) >= 0.8:  # High confidence only
                    target_field = mapping_info.get("target_field")
                    transformation = mapping_info.get("transformation")
                    
                    # Skip derived fields - they should never reach fast path
                    if transformation == "derived" or target_field is None:
                        field_name = canonical_field.split('.')[-1] if '.' in canonical_field else canonical_field
                        self.logger.warning(f"⚠️ Skipping derived/null field in fast path: {field_name} (should have been caught earlier)")
                        continue
                    
                    # Extract just the field name (e.g., "period_end" from "contracts.period_end")
                    field_name = canonical_field.split('.')[-1] if '.' in canonical_field else canonical_field
                    
                    # Safety check: ensure target_field is a string
                    if not isinstance(target_field, str):
                        self.logger.error(f"❌ target_field for '{field_name}' is not a string: {type(target_field)} = {target_field}")
                        continue
                    
                    if transformation == "constant":
                        # Replace with constant value
                        constant_value = mapping_info.get("constant_value", "NULL")
                        translated_query = re.sub(
                            rf'\b{field_name}\b',
                            str(constant_value),
                            translated_query,
                            flags=re.IGNORECASE
                        )
                    else:
                        # Direct field mapping - use just field name for matching
                        try:
                            translated_query = re.sub(
                                rf'\b{field_name}\b',
                                target_field,
                                translated_query,
                                flags=re.IGNORECASE
                            )
                        except Exception as field_error:
                            self.logger.error(f"❌ Failed to replace '{field_name}' with '{target_field}': {field_error}")
            
            return translated_query
            
        except Exception as e:
            self.logger.error(f"Failed to apply cached mappings: {e}")
            # Return a basic fallback query
            return f"-- ERROR: Could not apply cached mappings\n-- {str(e)}\n{canonical_query}"
    
    def _translate_complex_with_mappings(
        self, 
        canonical_query: str, 
        query_analysis, 
        join_strategy, 
        customer_schema: Dict, 
        mappings: Dict
    ) -> str:
        """Translate complex queries using LLM with mapping context - DIRECT INJECTION VERSION"""
        
        # Extract complex mappings and build explicit CASE statements
        complex_mappings = mappings.get("complex_mappings", [])
        field_mappings = mappings.get("field_mappings", {})
        table_mappings = mappings.get("table_mappings", {})
        
        # Build explicit CASE statements for derived fields in the query
        derived_field_logic = []
        for complex_mapping in complex_mappings:
            canonical_field = complex_mapping.get("canonical_field", "")
            logic = complex_mapping.get("logic", "")
            field_name = canonical_field.split('.')[-1] if '.' in canonical_field else canonical_field
            
            if field_name.upper() in canonical_query.upper():
                derived_field_logic.append(f"\n{field_name}: {logic}")
        
        prompt = f"""
You are a SQL expert. Translate this canonical query to match the customer's database schema.

=== CANONICAL QUERY ===
{canonical_query}

=== CUSTOMER SCHEMA ===
{json.dumps(customer_schema, indent=2)}

=== TABLE NAME MAPPINGS ===
{chr(10).join([f"  {k} → {v}" for k, v in table_mappings.items() if v is not None])}

=== DERIVED FIELDS - COPY THESE EXACTLY IN YOUR SELECT CLAUSE ===
{"".join(derived_field_logic) if derived_field_logic else "None - all fields are direct mappings"}

=== SIMPLE FIELD MAPPINGS (Direct column references) ===
{chr(10).join([f"  {k} → {v.get('target_field')}" for k, v in field_mappings.items() if v.get('transformation') != 'derived' and v.get('target_field')])}

=== MANDATORY RULES ===
1. For DERIVED FIELDS above: Copy the ENTIRE expression EXACTLY as written (including CASE/WHEN/THEN/ELSE/END)
2. DO NOT simplify or modify derived field logic
3. DO NOT use direct column references (like t.action_type AS status) for derived fields  
4. For simple fields: Use the direct column mappings
5. Add appropriate JOINs based on schema relationships
6. Translate WHERE clause field names using the mappings

=== CRITICAL OUTPUT REQUIREMENTS ===
⚠️  OUTPUT ONLY VALID, EXECUTABLE SQL - NO EXPLANATIONS, COMMENTS, OR NOTES IN THE SQL ITSELF
⚠️  DO NOT include literal strings like 'Use this field...' or 'SET because...' in the SELECT clause
⚠️  DO NOT include instructional comments as column values
⚠️  Every SELECT column must be an actual column reference, expression, or literal value (like 'USD')
⚠️  Example WRONG: 'Use contracts.signing_date as period_start' AS period_start
⚠️  Example CORRECT: contracts.signing_date AS period_start

OUTPUT: Only the SQL query with NO explanatory text.
"""

        try:
            translated_sql = self.llm_adapter.generate_completion(prompt)
            # Post-process to clean up any literal instruction strings
            cleaned_sql = self._clean_instruction_strings(translated_sql)
            return cleaned_sql
        except Exception as e:
            self.logger.error(f"LLM translation failed: {e}")
            return canonical_query  # Fallback to original
    
    def _clean_instruction_strings(self, sql: str) -> str:
        """
        Clean up literal instruction strings that the LLM might have included in the SQL.
        These are explanatory comments that should not be in the actual query.
        """
        import re
        
        # Pattern to detect literal instruction strings in SELECT clause
        # Matches strings like: 'Use this field...' AS column_name or 'SET because...' AS column_name
        instruction_patterns = [
            r"'(?:Use|SET|ARRAY|Join|Map|Convert|Apply|Calculate|Derive|Extract|Transform)[^']{10,}'\s+AS\s+(\w+)",
            r"'[^']*(?:when available|otherwise NULL|because|implicit in|should be)[^']*'\s+AS\s+(\w+)"
        ]
        
        cleaned_sql = sql
        for pattern in instruction_patterns:
            matches = re.finditer(pattern, cleaned_sql, re.IGNORECASE)
            for match in matches:
                full_match = match.group(0)
                column_name = match.group(1)
                
                # Log the cleanup
                self.logger.warning(f"⚠️  Removing instruction string: {full_match[:50]}...")
                
                # Replace with NULL AS column_name (better than leaving broken SQL)
                replacement = f"NULL AS {column_name}"
                cleaned_sql = cleaned_sql.replace(full_match, replacement)
        
        return cleaned_sql
    
    def _validate_query_against_schema(
        self,
        translated_sql: str,
        customer_schema: Dict,
        customer_id: str
    ) -> Dict:
        """
        Comprehensive validation: Check if all tables and columns in the translated query
        actually exist in the customer's schema.
        
        Returns:
            Dict with keys: valid (bool), errors (list), warnings (list)
        """
        import re
        
        errors = []
        warnings = []
        
        # Handle case where customer_schema might be a list (defensive programming)
        if isinstance(customer_schema, list):
            self.logger.warning("⚠️ customer_schema is a list in validation, attempting to convert")
            schema_dict = {'tables': {}}
            for item in customer_schema:
                if isinstance(item, dict) and 'name' in item:
                    schema_dict['tables'][item['name']] = item
            customer_schema = schema_dict
        
        # Extract table names from customer schema
        schema_tables = set()
        schema_columns = {}  # table -> set of columns
        
        if 'tables' in customer_schema:
            for table_name, table_def in customer_schema['tables'].items():
                schema_tables.add(table_name.lower())
                columns = set()
                if 'columns' in table_def:
                    for col_name in table_def['columns'].keys():
                        columns.add(col_name.lower())
                schema_columns[table_name.lower()] = columns
        
        # Parse translated SQL to extract referenced tables and columns
        sql_upper = translated_sql.upper()
        sql_lower = translated_sql.lower()
        
        # Extract table names from FROM and JOIN clauses
        # Pattern: FROM table_name or JOIN table_name
        from_pattern = r'(?:FROM|JOIN)\s+(\w+)'
        referenced_tables = set()
        for match in re.finditer(from_pattern, translated_sql, re.IGNORECASE):
            table_name = match.group(1).lower()
            # Skip SQL keywords
            if table_name not in ['select', 'where', 'group', 'order', 'having', 'limit', 'case', 'when', 'then', 'else', 'end', 'as']:
                referenced_tables.add(table_name)
        
        # Validate tables exist in schema
        for table in referenced_tables:
            if table not in schema_tables:
                errors.append(f"Table '{table}' referenced in query but not found in customer schema")
        
        # Extract column references (pattern: table_alias.column_name or just column_name)
        # This is a simplified extraction - won't catch all cases but covers most
        column_pattern = r'(\w+)\.(\w+)'
        table_aliases = {}  # alias -> actual table name
        
        # First pass: identify table aliases (FROM table AS alias or FROM table alias)
        alias_pattern = r'(?:FROM|JOIN)\s+(\w+)(?:\s+AS)?\s+(\w+)'
        for match in re.finditer(alias_pattern, translated_sql, re.IGNORECASE):
            table_name = match.group(1).lower()
            alias = match.group(2).lower()
            # Make sure alias is not a SQL keyword
            if alias not in ['on', 'where', 'inner', 'left', 'right', 'outer', 'join']:
                table_aliases[alias] = table_name
        
        # Second pass: check column references
        for match in re.finditer(column_pattern, translated_sql, re.IGNORECASE):
            alias_or_table = match.group(1).lower()
            column = match.group(2).lower()
            
            # Resolve alias to actual table name
            actual_table = table_aliases.get(alias_or_table, alias_or_table)
            
            # Skip if table doesn't exist (already reported)
            if actual_table not in schema_tables:
                continue
            
            # Check if column exists in that table
            if actual_table in schema_columns:
                if column not in schema_columns[actual_table]:
                    errors.append(f"Column '{column}' referenced in table '{actual_table}' but not found in schema")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info(f"✅ Schema validation passed for {customer_id}")
        else:
            self.logger.warning(f"❌ Schema validation failed for {customer_id}: {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"   - {error}")
        
        return {
            "valid": is_valid,
            "errors": errors,
            "warnings": warnings,
            "referenced_tables": list(referenced_tables),
            "schema_tables": list(schema_tables)
        }
    
    def _validate_complex_mappings_applied(
        self, 
        translated_sql: str, 
        mappings: Dict, 
        customer_schema: Dict
    ) -> Dict:
        """Validate that complex mappings (CASE statements, derived fields) were actually applied"""
        
        complex_mappings = mappings.get("complex_mappings", [])
        if not complex_mappings:
            return {"valid": True, "reason": "No complex mappings to validate"}
        
        sql_upper = translated_sql.upper()
        warnings = []
        
        for complex_mapping in complex_mappings:
            canonical_field = complex_mapping.get("canonical_field", "")
            expected_logic = complex_mapping.get("logic", "")
            
            # Extract field name (e.g., "status" from "contracts.status")
            field_name = canonical_field.split('.')[-1] if '.' in canonical_field else canonical_field
            
            # Check if this field appears in the query
            if field_name.upper() in sql_upper:
                # Check if the logic was applied (look for CASE statement keywords)
                if "CASE" in expected_logic.upper():
                    if "CASE" not in sql_upper or "WHEN" not in sql_upper:
                        return {
                            "valid": False,
                            "reason": f"Field '{field_name}' requires CASE statement but none found in translated SQL",
                            "field": field_name,
                            "expected_logic": expected_logic,
                            "warnings": warnings
                        }
                    
                    # Check if the field is being used as a direct column reference (wrong!)
                    # Look for patterns like "t.action_type AS status" or "a.award_type AS status"
                    import re
                    direct_ref_pattern = rf'\w+\.[\w_]+\s+AS\s+{field_name}\b'
                    if re.search(direct_ref_pattern, translated_sql, re.IGNORECASE):
                        return {
                            "valid": False,
                            "reason": f"Field '{field_name}' is using direct column reference instead of CASE statement",
                            "field": field_name,
                            "expected_logic": expected_logic,
                            "warnings": warnings
                        }
        
        return {"valid": True, "reason": "All complex mappings validated", "warnings": warnings}
    
    def _regenerate_with_explicit_mappings(
        self,
        canonical_query: str,
        query_analysis,
        join_strategy,
        customer_schema: Dict,
        mappings: Dict,
        validation_result: Dict
    ) -> str:
        """Regenerate query with explicit CASE statement injection for derived fields"""
        
        complex_mappings = mappings.get("complex_mappings", [])
        field_mappings = mappings.get("field_mappings", {})
        table_mappings = mappings.get("table_mappings", {})
        
        # Build a strict prompt that includes pre-written CASE statements
        case_statements = []
        for complex_mapping in complex_mappings:
            canonical_field = complex_mapping.get("canonical_field", "")
            logic = complex_mapping.get("logic", "")
            field_name = canonical_field.split('.')[-1] if '.' in canonical_field else canonical_field
            
            if field_name.upper() in canonical_query.upper():
                case_statements.append(f"\n-- For field '{field_name}', use this EXACT expression:\n{logic} AS {field_name}")
        
        prompt = f"""
You are a SQL expert. Translate this canonical query to the customer schema.

=== CUSTOMER SCHEMA ===
{json.dumps(customer_schema, indent=2)}

=== TABLE MAPPINGS ===
{chr(10).join([f"{k} → {v}" for k, v in table_mappings.items() if v is not None])}

=== CANONICAL QUERY ===
{canonical_query}

=== CRITICAL: DERIVED FIELDS (USE THESE EXACTLY) ===
{"".join(case_statements)}

=== SIMPLE FIELD MAPPINGS ===
{chr(10).join([f"{k} → {v.get('target_field', 'UNKNOWN')}" for k, v in field_mappings.items() if v.get('transformation') != 'derived' and v.get('target_field') is not None])}

=== STRICT REQUIREMENTS ===
1. **MANDATORY**: For derived fields listed above, copy the ENTIRE CASE statement EXACTLY as shown
2. DO NOT create your own derivation logic
3. DO NOT use direct column references (like "t.action_type") for derived fields
4. For simple fields, use the direct mappings
5. Apply appropriate JOINs based on the schema
6. Preserve WHERE conditions, translating field names as needed

Generate ONLY the SQL query, no explanations.
"""
        
        try:
            regenerated_sql = self.llm_adapter.generate_completion(prompt)
            return regenerated_sql
        except Exception as e:
            self.logger.error(f"Regeneration failed: {e}")
            # Return original with error comment
            return f"-- REGENERATION FAILED: {str(e)}\n{canonical_query}"
    
    def _regenerate_with_schema_validation(
        self,
        canonical_query: str,
        customer_schema: Dict,
        customer_id: str,
        mappings: Dict,
        schema_validation_errors: List[str]
    ) -> str:
        """
        Regenerate query using LLM with full tenant schema context when schema validation fails.
        This ensures all referenced tables and columns actually exist in the customer schema.
        """
        
        field_mappings = mappings.get("field_mappings", {})
        table_mappings = mappings.get("table_mappings", {})
        complex_mappings = mappings.get("complex_mappings", [])
        
        # Build detailed schema context showing available tables and columns
        schema_context = []
        schema_context.append("=== AVAILABLE TABLES AND COLUMNS IN CUSTOMER SCHEMA ===")
        
        if 'tables' in customer_schema:
            for table_name, table_def in customer_schema['tables'].items():
                schema_context.append(f"\nTable: {table_name}")
                if 'description' in table_def:
                    schema_context.append(f"  Description: {table_def['description']}")
                
                if 'columns' in table_def:
                    schema_context.append("  Columns:")
                    for col_name, col_def in table_def['columns'].items():
                        col_type = col_def.get('type', 'unknown')
                        col_desc = col_def.get('description', '')
                        schema_context.append(f"    - {col_name} ({col_type}){' - ' + col_desc if col_desc else ''}")
        
        # Build error context
        error_context = "\n".join([f"  - {err}" for err in schema_validation_errors])
        
        # Build derived fields context
        derived_fields = []
        for complex_mapping in complex_mappings:
            canonical_field = complex_mapping.get("canonical_field", "")
            logic = complex_mapping.get("logic", "")
            derived_fields.append(f"  {canonical_field}: {logic}")
        
        prompt = f"""
You are a SQL expert. The previous query translation failed schema validation.
You must regenerate the query using ONLY tables and columns that exist in the customer schema.

=== SCHEMA VALIDATION ERRORS FROM PREVIOUS ATTEMPT ===
{error_context}

{chr(10).join(schema_context)}

=== CANONICAL QUERY TO TRANSLATE ===
{canonical_query}

=== SUGGESTED TABLE MAPPINGS ===
{chr(10).join([f"  {k} → {v}" for k, v in table_mappings.items() if v is not None])}

=== SUGGESTED FIELD MAPPINGS ===
{chr(10).join([f"  {k} → {v.get('target_field', 'UNKNOWN')}" for k, v in field_mappings.items() if v.get('target_field') is not None and v.get('transformation') != 'derived'])}

=== DERIVED FIELDS (Copy these expressions exactly) ===
{chr(10).join(derived_fields) if derived_fields else "None"}

=== CRITICAL REQUIREMENTS ===
1. **MANDATORY**: Use ONLY tables and columns listed in the "AVAILABLE TABLES AND COLUMNS" section above
2. Verify each table name and column name exists before using it
3. For derived fields, copy the expressions EXACTLY as shown
4. If a suggested mapping references a non-existent table/column, find an alternative or omit it
5. Preserve the intent of the canonical query while respecting schema constraints
6. Use appropriate JOINs based on the relationships shown in the schema
7. Add table aliases for readability (e.g., FROM contracts c)

=== OUTPUT ===
Generate ONLY the corrected SQL query. No explanations, no comments except SQL comments in the query itself.
"""
        
        try:
            self.logger.info("🔄 Regenerating query with full schema validation context...")
            regenerated_sql = self.llm_adapter.generate_completion(prompt)
            self.logger.info("✅ Schema-aware regeneration completed")
            return regenerated_sql
        except Exception as e:
            self.logger.error(f"❌ Schema-aware regeneration failed: {e}")
            # Return original with error comments
            return f"""-- SCHEMA VALIDATION FAILED
-- Errors: {', '.join(schema_validation_errors)}
-- Regeneration also failed: {str(e)}
{canonical_query}"""
    
    def _build_mapping_context_for_llm(self, mappings: Dict) -> str:
        """Build formatted mapping context for LLM prompt"""
        context_parts = []
        
        field_mappings = mappings.get("field_mappings", {})
        table_mappings = mappings.get("table_mappings", {})
        complex_mappings = mappings.get("complex_mappings", [])
        
        if table_mappings:
            context_parts.append("TABLE MAPPINGS:")
            for canonical, customer in table_mappings.items():
                context_parts.append(f"  {canonical} → {customer}")
            context_parts.append("")
        
        if field_mappings:
            context_parts.append("FIELD MAPPINGS:")
            for canonical, mapping in field_mappings.items():
                confidence = mapping.get("confidence", 0)
                target = mapping.get("target_field", "UNKNOWN")
                context_parts.append(f"  {canonical} → {target} (confidence: {confidence:.2f})")
            context_parts.append("")
        
        if complex_mappings:
            context_parts.append("COMPLEX MAPPINGS:")
            for mapping in complex_mappings:
                field = mapping.get("canonical_field", "")
                logic = mapping.get("logic", "")
                context_parts.append(f"  {field} → {logic}")
            context_parts.append("")
        
        return "\n".join(context_parts) if context_parts else "No specific mappings available"
    
    def _regenerate_with_corrections(
        self, 
        canonical_query: str, 
        failed_query: str, 
        validation_result, 
        mappings: Dict
    ) -> str:
        """Regenerate query with validation corrections"""
        
        errors = validation_result.errors if hasattr(validation_result, 'errors') else []
        
        correction_prompt = f"""
The previous SQL translation failed validation. Please fix these issues:

VALIDATION ERRORS:
{chr(10).join(f"- {error}" for error in errors)}

FAILED QUERY:
{failed_query}

ORIGINAL CANONICAL QUERY:
{canonical_query}

AVAILABLE MAPPINGS:
{self._build_mapping_context_for_llm(mappings)}

Generate a corrected SQL query that addresses all validation errors.
"""

        try:
            return self.llm_adapter.generate_completion(correction_prompt)
        except Exception as e:
            self.logger.error(f"Query regeneration failed: {e}")
            return failed_query
    
    def _fallback_mappings(self, customer_schema: Dict) -> Dict[str, Any]:
        """Fallback mappings when LLM discovery fails"""
        return {
            "field_mappings": {},
            "table_mappings": {},
            "complex_mappings": [],
            "confidence_score": 0.0,
            "discovery_timestamp": time.time(),
            "needs_review": ["Full schema needs manual review - LLM discovery failed"]
        }
    
    def _build_field_mappings_context(self, tenant_config) -> str:
        """Build context string for tenant-specific field mappings"""
        if not tenant_config or not tenant_config.field_mappings:
            return "No specific field mappings configured - LLM should infer mappings from schema"
        
        context_parts = []
        context_parts.append("=== TENANT FIELD MAPPINGS (CRITICAL - USE THESE EXACTLY) ===")
        context_parts.append("⚠️  MANDATORY: Use these exact field mappings, do not guess or derive alternatives!")
        
        for canonical_field, target_mapping in tenant_config.field_mappings.items():
            if isinstance(target_mapping, str):
                if target_mapping.startswith("'") and target_mapping.endswith("'"):
                    # Constant value
                    context_parts.append(f"✅ {canonical_field} → {target_mapping} (constant value)")
                else:
                    # Direct field mapping
                    context_parts.append(f"✅ {canonical_field} → {target_mapping} (direct field)")
            elif isinstance(target_mapping, dict):
                # Complex mapping (derived logic)
                mapping_type = target_mapping.get('type', 'unknown')
                if mapping_type == 'derived':
                    logic = target_mapping.get('logic', 'No logic specified')
                    source_fields = target_mapping.get('source_fields', [])
                    context_parts.append(f"✅ {canonical_field} → DERIVED: {logic}")
                    context_parts.append(f"   Source fields: {', '.join(source_fields)}")
                else:
                    context_parts.append(f"✅ {canonical_field} → {target_mapping}")
            else:
                context_parts.append(f"✅ {canonical_field} → {target_mapping}")
        
        context_parts.append("\n🎯 PRIMARY TABLE: " + tenant_config.primary_table)
        context_parts.append("\n⚠️  CRITICAL REMINDERS:")
        context_parts.append("- Use ONLY the mappings above - do not invent field names")
        context_parts.append("- For status fields, use the exact mapped field name (e.g., 'contract_status', not derived logic)")
        context_parts.append("- For constants like 'USD', use the exact constant value")
        context_parts.append("- Verify every field exists in the customer schema before using it")
        
        return "\n".join(context_parts)
    
    def _build_enhanced_schema_context(self, customer_schema: Dict[str, Any], tenant_config) -> str:
        """Build comprehensive context combining schema structure with field mappings"""
        context_parts = []
        
        # Header
        context_parts.append("=" * 80)
        context_parts.append("🎯 TENANT-SPECIFIC TRANSLATION GUIDE")
        context_parts.append("=" * 80)
        
        # Field mappings section (most critical)
        if tenant_config and tenant_config.field_mappings:
            context_parts.append("\n🔥 CRITICAL FIELD MAPPINGS (USE THESE EXACTLY)")
            context_parts.append("─" * 50)
            context_parts.append("⚠️  MANDATORY: Use these exact mappings, never guess alternatives!")
            
            for canonical_field, target_mapping in tenant_config.field_mappings.items():
                if isinstance(target_mapping, str):
                    if target_mapping.startswith("'") and target_mapping.endswith("'"):
                        # Constant value
                        context_parts.append(f"✅ {canonical_field:<20} → {target_mapping:<25} (CONSTANT)")
                    else:
                        # Direct field mapping
                        context_parts.append(f"✅ {canonical_field:<20} → {target_mapping:<25} (DIRECT FIELD)")
                elif isinstance(target_mapping, dict):
                    # Complex mapping (derived logic)
                    mapping_type = target_mapping.get('type', 'unknown')
                    if mapping_type == 'derived':
                        logic = target_mapping.get('logic', 'No logic specified')
                        context_parts.append(f"✅ {canonical_field:<20} → DERIVED: {logic}")
                    else:
                        context_parts.append(f"✅ {canonical_field:<20} → {target_mapping}")
            
            context_parts.append(f"\n🎯 PRIMARY TABLE: {tenant_config.primary_table}")
        else:
            context_parts.append("\n⚠️  No field mappings configured - infer from schema")
        
        # Schema structure section
        context_parts.append("\n📊 CUSTOMER SCHEMA STRUCTURE")
        context_parts.append("─" * 50)
        context_parts.append("⚠️  Use ONLY columns listed below - any unlisted column does NOT exist!")
        
        for table_name, table_info in customer_schema.get('tables', {}).items():
            # Mark primary table
            primary_marker = " 🎯 PRIMARY" if (tenant_config and table_name == tenant_config.primary_table) else ""
            context_parts.append(f"\n📋 Table: {table_name}{primary_marker}")
            context_parts.append(f"   Description: {table_info.get('description', 'No description')}")
            
            # List all available columns with mapping annotations
            columns = table_info.get('columns', {})
            if columns:
                context_parts.append("   ✅ AVAILABLE COLUMNS:")
                for col_name, col_info in columns.items():
                    col_type = col_info.get('type', 'unknown')
                    col_desc = col_info.get('description', 'No description')
                    
                    # Check if this column is mapped from a canonical field
                    mapped_from = []
                    if tenant_config and tenant_config.field_mappings:
                        for canonical_field, target_mapping in tenant_config.field_mappings.items():
                            if isinstance(target_mapping, str) and target_mapping == col_name:
                                mapped_from.append(f"maps from '{canonical_field}'")
                    
                    mapping_note = f" [{', '.join(mapped_from)}]" if mapped_from else ""
                    context_parts.append(f"      ✓ {col_name:<25} ({col_type:<15}) {col_desc}{mapping_note}")
            
            # Show sample data if available
            sample_data = self._get_sample_data(table_name, customer_schema.get('tenant', 'tenant_A'))
            if sample_data:
                context_parts.append(f"\n   📊 Sample Data:")
                context_parts.append("   " + sample_data.replace("\n", "\n   "))
        
        # Critical reminders
        context_parts.append("\n" + "⚠️" * 20 + " CRITICAL RULES " + "⚠️" * 20)
        context_parts.append("1. Use EXACT field mappings above - never substitute or guess")
        context_parts.append("2. For status fields, use direct mapped fields (e.g., 'contract_status')")
        context_parts.append("3. For constants like 'USD', use the exact constant value")
        context_parts.append("4. Verify every column exists in schema before using")
        context_parts.append("5. Use primary table as main FROM table")
        context_parts.append("6. Follow JOIN strategy for multi-table queries")
        
        return "\n".join(context_parts)
    
    def _build_intelligent_schema_context(
        self, 
        canonical_query: str, 
        customer_schema: Dict[str, Any], 
        customer_id: str,
        query_analysis: QueryAnalysis
    ) -> str:
        """Build intelligent schema context with LLM-inferred field mappings"""
        
        # Extract canonical fields from the query
        canonical_fields = self._extract_canonical_fields(canonical_query, query_analysis)
        
        # Get customer schema structure
        schema_structure = self._analyze_customer_schema(customer_schema)
        
        # Use LLM to intelligently map fields
        field_mappings = self._infer_field_mappings_with_llm(
            canonical_fields, schema_structure, customer_schema
        )
        
        # Build comprehensive context
        context_parts = []
        context_parts.append("=" * 80)
        context_parts.append("🧠 INTELLIGENT SCHEMA MAPPING ANALYSIS")
        context_parts.append("=" * 80)
        
        # Show LLM-inferred mappings
        context_parts.append("\n🎯 LLM-INFERRED FIELD MAPPINGS")
        context_parts.append("─" * 50)
        context_parts.append("✨ These mappings were intelligently inferred by analyzing:")
        context_parts.append("   • Field names & semantic similarity")
        context_parts.append("   • Data types & compatibility") 
        context_parts.append("   • Column descriptions & context")
        context_parts.append("   • Sample data patterns")
        context_parts.append("")
        
        for canonical_field, mapping_info in field_mappings.items():
            confidence = mapping_info.get('confidence', 0)
            target_field = mapping_info.get('target_field', 'NOT_FOUND')
            reasoning = mapping_info.get('reasoning', 'No reasoning provided')
            
            confidence_emoji = "🟢" if confidence > 0.8 else "🟡" if confidence > 0.5 else "🔴"
            
            context_parts.append(f"{confidence_emoji} {canonical_field:<20} → {target_field:<25} ({confidence:.0%})")
            context_parts.append(f"   💭 {reasoning}")
        
        # Show customer schema with mapping annotations
        context_parts.append(f"\n📊 CUSTOMER SCHEMA: {customer_schema.get('tenant', customer_id)}")
        context_parts.append("─" * 50)
        
        for table_name, table_info in customer_schema.get('tables', {}).items():
            context_parts.append(f"\n📋 Table: {table_name}")
            context_parts.append(f"   Description: {table_info.get('description', 'No description')}")
            
            columns = table_info.get('columns', {})
            if columns:
                context_parts.append("   Available Columns:")
                for col_name, col_info in columns.items():
                    col_type = col_info.get('type', 'unknown')
                    col_desc = col_info.get('description', 'No description')
                    
                    # Check if this column was mapped
                    mapped_from = []
                    for canonical_field, mapping_info in field_mappings.items():
                        if mapping_info.get('target_field') == col_name:
                            confidence = mapping_info.get('confidence', 0)
                            conf_emoji = "🟢" if confidence > 0.8 else "🟡" if confidence > 0.5 else "🔴"
                            mapped_from.append(f"{conf_emoji} maps from '{canonical_field}' ({confidence:.0%})")
                    
                    mapping_note = f" [{'; '.join(mapped_from)}]" if mapped_from else ""
                    context_parts.append(f"      ✓ {col_name:<25} ({col_type:<15}) {col_desc}{mapping_note}")
        
        # Add intelligent translation rules
        context_parts.append("\n" + "🧠" * 20 + " INTELLIGENT TRANSLATION RULES " + "🧠" * 20)
        context_parts.append("1. Use high-confidence mappings (🟢) directly")
        context_parts.append("2. For medium-confidence mappings (🟡), verify field compatibility")
        context_parts.append("3. For low-confidence mappings (🔴), use fallback logic or skip")
        context_parts.append("4. Consider semantic similarity when choosing between similar fields")
        context_parts.append("5. Prefer exact name matches over semantic matches")
        context_parts.append("6. Use data type compatibility as a secondary filter")
        
        return "\n".join(context_parts)
    
    def _extract_canonical_fields(self, canonical_query: str, query_analysis: QueryAnalysis) -> List[str]:
        """Extract canonical fields that need mapping from the query"""
        fields = set()
        
        # Add fields from query analysis
        fields.update(query_analysis.required_fields)
        
        # Extract additional fields from WHERE conditions
        for condition in query_analysis.where_conditions:
            # Simple regex to find field names in conditions
            import re
            field_matches = re.findall(r'(\w+)\s*[=<>!]', condition)
            fields.update(field_matches)
        
        # Add common contract fields that might be needed
        common_fields = [
            'contract_id', 'title', 'status', 'date_signed', 'period_start', 'period_end',
            'value_amount', 'value_currency', 'buyer_party_id', 'supplier_party_ids'
        ]
        
        # Only add common fields that are likely needed based on query
        query_lower = canonical_query.lower()
        for field in common_fields:
            if field in query_lower or field.replace('_', '') in query_lower:
                fields.add(field)
        
        return list(fields)
    
    def _analyze_customer_schema(self, customer_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze customer schema structure for intelligent mapping"""
        analysis = {
            'tables': {},
            'all_fields': [],
            'field_types': {},
            'field_descriptions': {}
        }
        
        for table_name, table_info in customer_schema.get('tables', {}).items():
            table_analysis = {
                'columns': [],
                'primary_keys': [],
                'foreign_keys': []
            }
            
            for col_name, col_info in table_info.get('columns', {}).items():
                col_type = col_info.get('type', 'unknown')
                col_desc = col_info.get('description', '')
                
                table_analysis['columns'].append({
                    'name': col_name,
                    'type': col_type,
                    'description': col_desc,
                    'table': table_name
                })
                
                analysis['all_fields'].append(f"{table_name}.{col_name}")
                analysis['field_types'][f"{table_name}.{col_name}"] = col_type
                analysis['field_descriptions'][f"{table_name}.{col_name}"] = col_desc
                
                # Detect primary/foreign keys
                if 'primary key' in col_desc.lower() or col_name.endswith('_id') and table_name.startswith(col_name.replace('_id', '')):
                    table_analysis['primary_keys'].append(col_name)
                elif col_name.endswith('_id'):
                    table_analysis['foreign_keys'].append(col_name)
            
            analysis['tables'][table_name] = table_analysis
        
        return analysis
    
    def _infer_field_mappings_with_llm(
        self, 
        canonical_fields: List[str], 
        schema_analysis: Dict[str, Any],
        customer_schema: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Use LLM to intelligently infer field mappings"""
        
        # Build context for LLM
        mapping_prompt = f"""
You are a database schema expert. Analyze the customer schema and suggest the best field mappings for canonical contract fields.

## Canonical Fields to Map:
{', '.join(canonical_fields)}

## Customer Schema Analysis:
"""
        
        for table_name, table_info in customer_schema.get('tables', {}).items():
            mapping_prompt += f"\n### Table: {table_name}\n"
            mapping_prompt += f"Description: {table_info.get('description', 'No description')}\n"
            mapping_prompt += "Columns:\n"
            
            for col_name, col_info in table_info.get('columns', {}).items():
                col_type = col_info.get('type', 'unknown')
                col_desc = col_info.get('description', 'No description')
                mapping_prompt += f"  - {col_name} ({col_type}): {col_desc}\n"
        
        mapping_prompt += """

## Your Task:
For each canonical field, suggest the best matching customer field with:
1. Target field name (table.column format)
2. Confidence score (0.0-1.0)
3. Reasoning for the mapping

Consider:
- Semantic similarity of field names
- Data type compatibility
- Column descriptions and context
- Common database naming patterns

Return JSON format:
{
  "canonical_field": {
    "target_field": "table.column",
    "confidence": 0.95,
    "reasoning": "Exact name match with compatible data type"
  }
}
"""
        
        try:
            # Use LLM to generate mappings
            response = self.llm_adapter.generate_completion(mapping_prompt)
            
            # Parse LLM response
            import json
            mappings = json.loads(response)
            
            # Validate and clean mappings
            validated_mappings = {}
            for canonical_field in canonical_fields:
                if canonical_field in mappings:
                    mapping_info = mappings[canonical_field]
                    # Ensure confidence is between 0 and 1
                    confidence = max(0.0, min(1.0, mapping_info.get('confidence', 0.5)))
                    validated_mappings[canonical_field] = {
                        'target_field': mapping_info.get('target_field', 'NOT_FOUND'),
                        'confidence': confidence,
                        'reasoning': mapping_info.get('reasoning', 'No reasoning provided')
                    }
                else:
                    # Fallback for missing mappings
                    validated_mappings[canonical_field] = {
                        'target_field': 'NOT_FOUND',
                        'confidence': 0.0,
                        'reasoning': 'No suitable mapping found by LLM'
                    }
            
            return validated_mappings
            
        except Exception as e:
            self.logger.warning(f"Failed to infer mappings with LLM: {str(e)}")
            
            # Fallback to simple name-based matching
            return self._fallback_name_matching(canonical_fields, schema_analysis)
    
    def _fallback_name_matching(self, canonical_fields: List[str], schema_analysis: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Fallback method using simple name similarity matching"""
        mappings = {}
        
        for canonical_field in canonical_fields:
            best_match = None
            best_score = 0.0
            
            for customer_field in schema_analysis['all_fields']:
                # Simple similarity scoring
                customer_col = customer_field.split('.')[-1]
                
                # Exact match
                if canonical_field == customer_col:
                    best_match = customer_field
                    best_score = 1.0
                    break
                
                # Partial match
                if canonical_field in customer_col or customer_col in canonical_field:
                    score = len(set(canonical_field) & set(customer_col)) / len(set(canonical_field) | set(customer_col))
                    if score > best_score:
                        best_match = customer_field
                        best_score = score
            
            mappings[canonical_field] = {
                'target_field': best_match or 'NOT_FOUND',
                'confidence': best_score,
                'reasoning': f"Name similarity matching (score: {best_score:.2f})"
            }
        
        return mappings
    
    def _generate_execution_plan(self, translated_query: str, join_strategy: JoinStrategy) -> Dict[str, Any]:
        """Generate execution plan metadata"""
        # Safely extract recommended indexes
        recommended_indexes = []
        for table, condition, _ in join_strategy.join_tables:
            if table and condition and '=' in condition:
                try:
                    column_part = condition.split('=')[0].strip()
                    if '.' in column_part:
                        column = column_part.split('.')[-1].strip()
                        if column:
                            recommended_indexes.append(f"{table}.{column}")
                except (IndexError, AttributeError):
                    # Skip if we can't parse the condition
                    pass
        
        return {
            'tables_involved': [join_strategy.primary_table] + [table for table, _, _ in join_strategy.join_tables if table],
            'join_count': len(join_strategy.join_tables),
            'estimated_complexity': 'high' if len(join_strategy.join_tables) > 3 else 'medium',
            'recommended_indexes': recommended_indexes
        }
