"""
Dynamic Field Mapper
Uses configuration-driven field mappings instead of hardcoded logic
"""

import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from .config_manager import config_manager

@dataclass
class FieldMapping:
    """Represents a field mapping result"""
    canonical: str
    target: str
    success: bool
    description: str
    mapping_type: str  # 'direct', 'derived', 'semantic'

class FieldMapper:
    """Handles dynamic field mapping based on configuration"""
    
    def __init__(self):
        self.config_manager = config_manager
    
    def analyze_field_mapping(self, canonical_query: str, translated_query: str, tenant_id: str) -> List[FieldMapping]:
        """Analyze field mappings between canonical and translated queries"""
        mappings = []
        
        # Get tenant configuration
        tenant_config = self.config_manager.get_tenant_config(tenant_id)
        if not tenant_config:
            return self._fallback_field_mapping(canonical_query, translated_query, tenant_id)
        
        # Extract fields from canonical query
        canonical_fields = self._extract_select_fields(canonical_query)
        
        # Analyze each canonical field
        for canonical_field in canonical_fields:
            mapping = self._map_canonical_field(
                canonical_field, 
                translated_query, 
                tenant_config
            )
            if mapping:
                mappings.append(mapping)
        
        # Analyze JOIN complexity
        join_mapping = self._analyze_join_complexity(canonical_query, translated_query)
        if join_mapping:
            mappings.append(join_mapping)
        
        return mappings
    
    def _map_canonical_field(self, canonical_field: str, translated_query: str, tenant_config) -> Optional[FieldMapping]:
        """Map a canonical field using tenant configuration"""
        # Clean field name (remove table prefixes, aliases)
        clean_field = self._clean_field_name(canonical_field)
        
        # Get field mapping from configuration
        field_mapping_config = tenant_config.field_mappings.get(clean_field)
        
        if not field_mapping_config:
            # Try to find the field in the translated query anyway
            return self._detect_field_in_query(canonical_field, translated_query)
        
        # Handle different mapping types
        if isinstance(field_mapping_config, str):
            # Direct mapping
            return self._handle_direct_mapping(canonical_field, field_mapping_config, translated_query)
        elif isinstance(field_mapping_config, dict):
            # Complex mapping (derived, conditional, etc.)
            return self._handle_complex_mapping(canonical_field, field_mapping_config, translated_query)
        
        return None
    
    def _handle_direct_mapping(self, canonical_field: str, target_field: str, translated_query: str) -> FieldMapping:
        """Handle direct field mapping"""
        if target_field in translated_query:
            return FieldMapping(
                canonical=canonical_field,
                target=target_field,
                success=True,
                description=f"Direct mapping: {canonical_field} → {target_field}",
                mapping_type="direct"
            )
        else:
            return FieldMapping(
                canonical=canonical_field,
                target=target_field,
                success=False,
                description=f"Configured mapping not found in query: {target_field}",
                mapping_type="direct"
            )
    
    def _handle_complex_mapping(self, canonical_field: str, mapping_config: Dict[str, Any], translated_query: str) -> FieldMapping:
        """Handle complex field mapping (derived, conditional, etc.)"""
        mapping_type = mapping_config.get('type', 'unknown')
        
        if mapping_type == 'derived':
            logic = mapping_config.get('logic', '')
            source_fields = mapping_config.get('source_fields', [])
            
            # Check if the derived logic appears in the query
            if 'CASE' in translated_query and any(field in translated_query for field in source_fields):
                return FieldMapping(
                    canonical=canonical_field,
                    target=f"Derived: {logic[:50]}...",
                    success=True,
                    description=f"Derived from {', '.join(source_fields)} using conditional logic",
                    mapping_type="derived"
                )
        
        elif mapping_type == 'conditional':
            conditions = mapping_config.get('conditions', [])
            for condition in conditions:
                if condition.get('when') in translated_query:
                    return FieldMapping(
                        canonical=canonical_field,
                        target=condition.get('then', 'unknown'),
                        success=True,
                        description=f"Conditional mapping: {condition.get('description', '')}",
                        mapping_type="conditional"
                    )
        
        return FieldMapping(
            canonical=canonical_field,
            target="Complex mapping",
            success=False,
            description=f"Complex mapping type '{mapping_type}' not fully resolved",
            mapping_type="complex"
        )
    
    def _detect_field_in_query(self, canonical_field: str, translated_query: str) -> Optional[FieldMapping]:
        """Try to detect field mapping by analyzing the query structure"""
        clean_field = self._clean_field_name(canonical_field)
        
        # Look for the field name in the translated query
        if clean_field in translated_query.lower():
            return FieldMapping(
                canonical=canonical_field,
                target=f"Detected: {clean_field}",
                success=True,
                description=f"Field detected in translated query",
                mapping_type="detected"
            )
        
        # Look for semantic equivalents
        semantic_mappings = {
            'contract_id': ['id', 'contract_number', 'agreement_id', 'award_id'],
            'status': ['state', 'condition', 'phase', 'stage'],
            'value': ['amount', 'cost', 'price', 'total'],
            'party_name': ['name', 'organization', 'company', 'entity']
        }
        
        if clean_field in semantic_mappings:
            for semantic_field in semantic_mappings[clean_field]:
                if semantic_field in translated_query.lower():
                    return FieldMapping(
                        canonical=canonical_field,
                        target=f"Semantic: {semantic_field}",
                        success=True,
                        description=f"Semantic equivalent found: {semantic_field}",
                        mapping_type="semantic"
                    )
        
        return None
    
    def _analyze_join_complexity(self, canonical_query: str, translated_query: str) -> Optional[FieldMapping]:
        """Analyze JOIN complexity changes"""
        canonical_joins = len(re.findall(r'\bJOIN\b', canonical_query, re.IGNORECASE))
        translated_joins = len(re.findall(r'\bJOIN\b', translated_query, re.IGNORECASE))
        
        if translated_joins != canonical_joins:
            return FieldMapping(
                canonical=f"Query Complexity: {canonical_joins} JOIN(s)",
                target=f"{translated_joins} JOIN(s)",
                success=translated_joins >= canonical_joins,
                description=f"JOIN complexity {'increased' if translated_joins > canonical_joins else 'decreased'} for multi-table reconstruction",
                mapping_type="structural"
            )
        
        return None
    
    def _extract_select_fields(self, query: str) -> List[str]:
        """Extract SELECT fields from SQL query"""
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return []
        
        select_clause = select_match.group(1)
        fields = []
        
        # Split by comma, but handle nested functions and expressions
        field_parts = []
        paren_count = 0
        current_field = ""
        
        for char in select_clause:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                field_parts.append(current_field.strip())
                current_field = ""
                continue
            current_field += char
        
        if current_field.strip():
            field_parts.append(current_field.strip())
        
        # Process each field part
        for field in field_parts:
            # Handle aliases (AS keyword)
            as_match = re.search(r'(.+?)\s+AS\s+(\w+)', field, re.IGNORECASE)
            if as_match:
                fields.append(as_match.group(2))  # Use the alias
            else:
                # Extract the main field name
                field_name = re.sub(r'^.*\.', '', field)  # Remove table prefix
                field_name = re.sub(r'\s.*$', '', field_name)  # Remove everything after space
                fields.append(field_name)
        
        return fields
    
    def _clean_field_name(self, field: str) -> str:
        """Clean field name by removing prefixes and aliases"""
        # Remove table prefix (e.g., "c.contract_id" -> "contract_id")
        field = re.sub(r'^[a-zA-Z_]+\.', '', field)
        # Remove AS aliases and extra whitespace
        field = re.sub(r'\s+AS\s+.*$', '', field, flags=re.IGNORECASE)
        return field.strip().lower()
    
    def _fallback_field_mapping(self, canonical_query: str, translated_query: str, tenant_id: str) -> List[FieldMapping]:
        """Fallback field mapping when configuration is not available"""
        mappings = []
        
        canonical_fields = self._extract_select_fields(canonical_query)
        translated_fields = self._extract_select_fields(translated_query)
        
        # Basic positional mapping
        for i, canonical_field in enumerate(canonical_fields):
            if i < len(translated_fields):
                translated_field = translated_fields[i]
                mappings.append(FieldMapping(
                    canonical=canonical_field,
                    target=translated_field,
                    success=True,
                    description=f"Positional mapping (fallback): {canonical_field} → {translated_field}",
                    mapping_type="positional"
                ))
        
        return mappings

# Global field mapper instance
field_mapper = FieldMapper()
