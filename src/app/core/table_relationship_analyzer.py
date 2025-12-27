"""
Table Relationship Analyzer for Multi-Table Schema Discovery

This module provides LLM-powered analysis of database schemas to automatically
discover relationships between tables, including primary/foreign key relationships,
logical entity mappings, and optimal JOIN strategies.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

from ..adapters.llm_openai import OpenAIAdapter

logger = logging.getLogger(__name__)


class RelationshipType(Enum):
    """Types of table relationships"""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    LOGICAL_ENTITY = "logical_entity"  # Tables that form a logical entity together


@dataclass
class TableRelationship:
    """Represents a relationship between two tables"""
    table1: str
    column1: str
    table2: str
    column2: str
    relationship_type: RelationshipType
    confidence: float
    reasoning: str
    join_condition: str
    is_primary_key: bool = False
    is_foreign_key: bool = False


@dataclass
class LogicalEntity:
    """Represents a logical entity composed of multiple tables"""
    entity_name: str
    primary_table: str
    related_tables: List[str]
    relationships: List[TableRelationship]
    confidence: float
    reasoning: str


class TableRelationshipAnalyzer:
    """
    LLM-powered analyzer that discovers relationships between tables in a database schema.
    Uses semantic understanding to identify logical entities and optimal JOIN strategies.
    """
    
    def __init__(self, llm_adapter: Optional[OpenAIAdapter] = None):
        """Initialize the relationship analyzer"""
        self.llm_adapter = llm_adapter or OpenAIAdapter()
        self.logger = logging.getLogger(__name__)
    
    def discover_relationships(
        self, 
        customer_schema: Dict[str, Any], 
        required_fields: List[str] = None
    ) -> List[TableRelationship]:
        """
        Discover relationships between tables in the customer schema
        
        Args:
            customer_schema: Customer's database schema
            required_fields: Fields required by the query (for focused analysis)
            
        Returns:
            List of discovered relationships
        """
        try:
            self.logger.info("🔍 STARTING RELATIONSHIP DISCOVERY")
            self.logger.info(f"   - Required fields: {required_fields}")
            self.logger.info(f"   - Schema tables: {list(customer_schema.get('tables', {}).keys())}")
            
            # Build comprehensive schema context
            self.logger.info("📋 Building comprehensive schema context for LLM...")
            schema_context = self._build_comprehensive_schema_context(customer_schema)
            field_context = self._build_field_requirements_context(required_fields) if required_fields else ""
            self.logger.info("✅ Schema context prepared")
            
            prompt = f"""
You are a database expert specializing in schema analysis and relationship discovery.

## Customer Database Schema
{schema_context}

## Query Field Requirements
{field_context}

## Your Task
Analyze the schema to discover relationships between tables. Look for:

1. **Primary/Foreign Key Relationships**: Columns that likely reference other tables
2. **Logical Entity Relationships**: Tables that together represent a single business entity
3. **Naming Pattern Relationships**: Tables with similar naming patterns that might be related
4. **Data Type Relationships**: Columns with matching data types that could be related

For each relationship, provide:
- Table and column names
- Relationship type (one_to_one, one_to_many, many_to_one, many_to_many, logical_entity)
- Confidence score (0.0-1.0)
- Reasoning for the relationship
- Suggested JOIN condition
- Whether it's a primary key or foreign key

Respond with a JSON array of relationships:

```json
[
    {{
        "table1": "contract_headers",
        "column1": "id",
        "table2": "contract_status_history", 
        "column2": "contract_id",
        "relationship_type": "one_to_many",
        "confidence": 0.95,
        "reasoning": "contract_status_history.contract_id references contract_headers.id based on naming pattern and data types",
        "join_condition": "contract_headers.id = contract_status_history.contract_id",
        "is_primary_key": true,
        "is_foreign_key": false
    }},
    {{
        "table1": "contract_status_history",
        "column1": "contract_id", 
        "table2": "contract_headers",
        "column2": "id",
        "relationship_type": "many_to_one",
        "confidence": 0.95,
        "reasoning": "contract_status_history.contract_id is a foreign key referencing contract_headers.id",
        "join_condition": "contract_status_history.contract_id = contract_headers.id",
        "is_primary_key": false,
        "is_foreign_key": true
    }}
]
```

Focus on relationships that would be needed to reconstruct logical entities like "contracts" from multiple tables.
"""
            
            self.logger.info("🤖 Sending relationship discovery request to LLM...")
            # Import the schema from the LLM adapter
            from ..adapters.llm_openai import RELATIONSHIP_SCHEMA
            import time
            start_time = time.time()
            try:
                response_text = self.llm_adapter.generate_completion(prompt, json_schema=RELATIONSHIP_SCHEMA)
                elapsed = time.time() - start_time
                self.logger.info(f"✅ LLM response received in {elapsed:.2f}s, parsing relationships...")
            except Exception as e:
                elapsed = time.time() - start_time
                self.logger.error(f"❌ LLM relationship discovery failed after {elapsed:.2f}s: {e}")
                self.logger.error(f"Error type: {type(e).__name__}")
                raise
            self.logger.debug(f"Response text type: {type(response_text)}")
            self.logger.debug(f"Response text content: {response_text[:200] if isinstance(response_text, str) else response_text}")
            
            # With structured outputs, we can parse directly as JSON
            try:
                parsed_response = json.loads(response_text) if isinstance(response_text, str) else response_text
                # Extract relationships array from the structured response
                if isinstance(parsed_response, dict) and 'relationships' in parsed_response:
                    relationships_data = parsed_response['relationships']
                else:
                    relationships_data = parsed_response
            except json.JSONDecodeError:
                # Fallback to the existing parsing method
                parsed_response = self.llm_adapter.parse_json_response(response_text)
                if isinstance(parsed_response, dict) and 'relationships' in parsed_response:
                    relationships_data = parsed_response['relationships']
                else:
                    relationships_data = parsed_response
            
            self.logger.info(f"📋 Parsed {len(relationships_data) if isinstance(relationships_data, list) else 0} relationship entries from LLM")
            
            # Ensure relationships_data is a list
            if not isinstance(relationships_data, list):
                self.logger.warning(f"Expected list but got {type(relationships_data)}: {relationships_data}")
                relationships_data = []
            
            relationships = []
            for rel_data in relationships_data:
                try:
                    # Validate and normalize relationship type
                    rel_type_str = rel_data['relationship_type'].lower().strip()
                    
                    # Map common variations to valid types
                    type_mapping = {
                        'naming_pattern': 'logical_entity',
                        'semantic': 'logical_entity',
                        'business_logic': 'logical_entity',
                        'one_to_one': 'one_to_one',
                        'one_to_many': 'one_to_many',
                        'many_to_one': 'many_to_one',
                        'many_to_many': 'many_to_many',
                        'logical_entity': 'logical_entity'
                    }
                    
                    normalized_type = type_mapping.get(rel_type_str, 'logical_entity')
                    
                    relationship = TableRelationship(
                        table1=rel_data['table1'],
                        column1=rel_data['column1'],
                        table2=rel_data['table2'],
                        column2=rel_data['column2'],
                        relationship_type=RelationshipType(normalized_type),
                        confidence=float(rel_data['confidence']),
                        reasoning=rel_data['reasoning'],
                        join_condition=rel_data['join_condition'],
                        is_primary_key=rel_data.get('is_primary_key', False),
                        is_foreign_key=rel_data.get('is_foreign_key', False)
                    )
                    relationships.append(relationship)
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Skipping invalid relationship: {e} - {rel_data}")
                    continue
            
            # Log completion summary
            self.logger.info("✅ RELATIONSHIP DISCOVERY COMPLETED")
            self.logger.info(f"📊 DISCOVERY SUMMARY:")
            self.logger.info(f"   - Total relationships found: {len(relationships)}")
            
            # Group by relationship type
            type_counts = {}
            for rel in relationships:
                rel_type = rel.relationship_type.value if hasattr(rel.relationship_type, 'value') else str(rel.relationship_type)
                type_counts[rel_type] = type_counts.get(rel_type, 0) + 1
            
            for rel_type, count in type_counts.items():
                self.logger.info(f"   - {rel_type}: {count} relationships")
            
            # Show top relationships by confidence
            if relationships:
                sorted_rels = sorted(relationships, key=lambda r: r.confidence, reverse=True)
                self.logger.info("🏆 TOP RELATIONSHIPS BY CONFIDENCE:")
                for i, rel in enumerate(sorted_rels[:3]):
                    self.logger.info(f"   {i+1}. {rel.table1}.{rel.column1} → {rel.table2}.{rel.column2} ({rel.confidence:.2f})")
            
            return relationships
            
        except Exception as e:
            self.logger.error("❌ RELATIONSHIP DISCOVERY FAILED")
            self.logger.error(f"💥 Error: {str(e)}")
            raise RuntimeError(f"LLM failed to discover relationships: {str(e)}")
    
    def discover_logical_entities(
        self, 
        customer_schema: Dict[str, Any], 
        relationships: List[TableRelationship]
    ) -> List[LogicalEntity]:
        """
        Discover logical entities that are split across multiple tables
        
        Args:
            customer_schema: Customer's database schema
            relationships: Previously discovered relationships
            
        Returns:
            List of logical entities
        """
        try:
            self.logger.info("Starting logical entity discovery")
            
            schema_context = self._build_comprehensive_schema_context(customer_schema)
            relationships_context = self._build_relationships_context(relationships)
            
            prompt = f"""
You are a database expert specializing in logical entity modeling and schema normalization analysis.

## Customer Database Schema
{schema_context}

## Discovered Relationships
{relationships_context}

## Your Task
Identify logical business entities that are split across multiple tables. Look for:

1. **Contract Entity**: Tables that together represent a "contract" (headers, status, details, lifecycle, etc.)
2. **Party Entity**: Tables that together represent parties (buyers, suppliers, etc.)
3. **Transaction Entity**: Tables that together represent financial transactions
4. **Document Entity**: Tables that together represent documents and attachments

For each logical entity, determine:
- The primary table (main table for the entity)
- Related tables that are part of the same logical entity
- The relationships that connect them
- Confidence in the entity grouping
- Reasoning for the grouping

Respond with a JSON array of logical entities:

```json
[
    {{
        "entity_name": "contract",
        "primary_table": "contract_headers",
        "related_tables": ["contract_status_history", "renewal_schedule", "contract_details"],
        "relationships": [
            {{
                "table1": "contract_headers",
                "column1": "id",
                "table2": "contract_status_history",
                "column2": "contract_id",
                "relationship_type": "one_to_many"
            }}
        ],
        "confidence": 0.90,
        "reasoning": "These tables together represent a complete contract entity with headers, status history, renewal information, and details"
    }}
]
```
"""
            
            response_text = self.llm_adapter.generate_completion(prompt)
            entities_data = self.llm_adapter.parse_json_response(response_text)
            
            logical_entities = []
            for entity_data in entities_data:
                entity = LogicalEntity(
                    entity_name=entity_data['entity_name'],
                    primary_table=entity_data['primary_table'],
                    related_tables=entity_data['related_tables'],
                    relationships=[],  # Would be populated from relationships list
                    confidence=float(entity_data['confidence']),
                    reasoning=entity_data['reasoning']
                )
                logical_entities.append(entity)
            
            self.logger.info(f"Discovered {len(logical_entities)} logical entities")
            return logical_entities
            
        except Exception as e:
            self.logger.error(f"Logical entity discovery failed: {str(e)}")
            return []
    
    def suggest_join_strategy(
        self, 
        logical_entity: LogicalEntity, 
        required_fields: List[str],
        query_complexity: str
    ) -> Dict[str, Any]:
        """
        Suggest optimal JOIN strategy for a logical entity
        
        Args:
            logical_entity: The logical entity to join
            required_fields: Fields required by the query
            query_complexity: Complexity level of the query
            
        Returns:
            JOIN strategy suggestions
        """
        try:
            self.logger.info(f"Generating JOIN strategy for {logical_entity.entity_name}")
            
            prompt = f"""
You are a database performance expert specializing in JOIN optimization.

## Logical Entity: {logical_entity.entity_name}
- Primary Table: {logical_entity.primary_table}
- Related Tables: {', '.join(logical_entity.related_tables)}
- Confidence: {logical_entity.confidence}

## Required Fields
{', '.join(required_fields)}

## Query Complexity
{query_complexity}

## Your Task
Suggest the optimal JOIN strategy for this logical entity. Consider:

1. **Performance**: Which JOIN order minimizes data transfer?
2. **Selectivity**: Which tables should be filtered first?
3. **Indexes**: What indexes would improve performance?
4. **Join Types**: When to use INNER vs LEFT JOIN?

Provide a JSON response with:

```json
{{
    "primary_table": "contract_headers",
    "join_order": ["contract_headers", "contract_status_history", "renewal_schedule"],
    "join_conditions": [
        "contract_headers.id = contract_status_history.contract_id",
        "contract_headers.id = renewal_schedule.contract_id"
    ],
    "join_types": ["INNER", "LEFT"],
    "performance_score": 0.85,
    "reasoning": "Start with contract_headers as it's the primary table. Join status history with INNER JOIN for active records, then LEFT JOIN renewal schedule for optional data.",
    "optimization_suggestions": [
        "Add index on contract_headers.id",
        "Add index on contract_status_history.contract_id",
        "Filter by status early to reduce JOIN size"
    ]
}}
```
"""
            
            response = self.llm_adapter.generate_completion(prompt)
            strategy_data = self.llm_adapter.parse_json_response(response)
            
            return strategy_data
            
        except Exception as e:
            self.logger.error(f"JOIN strategy generation failed: {str(e)}")
            return self._create_fallback_join_strategy(logical_entity)
    
    def _build_comprehensive_schema_context(self, customer_schema: Dict[str, Any]) -> str:
        """Build comprehensive context for schema analysis"""
        context_parts = []
        
        for table_name, table_info in customer_schema.get('tables', {}).items():
            context_parts.append(f"\n=== Table: {table_name} ===")
            context_parts.append(f"Description: {table_info.get('description', 'No description')}")
            context_parts.append("Columns:")
            
            for col_name, col_info in table_info.get('columns', {}).items():
                col_type = col_info.get('type', 'unknown')
                col_desc = col_info.get('description', 'No description')
                is_nullable = col_info.get('nullable', True)
                is_primary = col_info.get('is_primary_key', False)
                is_foreign = col_info.get('is_foreign_key', False)
                
                flags = []
                if is_primary:
                    flags.append("PRIMARY KEY")
                if is_foreign:
                    flags.append("FOREIGN KEY")
                if not is_nullable:
                    flags.append("NOT NULL")
                
                flag_str = f" ({', '.join(flags)})" if flags else ""
                context_parts.append(f"  - {col_name} ({col_type}): {col_desc}{flag_str}")
        
        return "\n".join(context_parts)
    
    def _build_field_requirements_context(self, required_fields: List[str]) -> str:
        """Build context for field requirements"""
        if not required_fields:
            return "No specific field requirements"
        
        return f"""
Required Fields: {', '.join(required_fields)}

Focus the relationship discovery on tables that contain these fields or are closely related to them.
"""
    
    def _build_relationships_context(self, relationships: List[TableRelationship]) -> str:
        """Build context for discovered relationships"""
        if not relationships:
            return "No relationships discovered yet"
        
        context_parts = []
        for rel in relationships:
            context_parts.append(
                f"- {rel.table1}.{rel.column1} -> {rel.table2}.{rel.column2} "
                f"({rel.relationship_type.value}, confidence: {rel.confidence})"
            )
            context_parts.append(f"  Reasoning: {rel.reasoning}")
        
        return "\n".join(context_parts)
    
