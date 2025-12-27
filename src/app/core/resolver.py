"""Resolver for combining LLM proposals with heuristic scoring."""

import re
from typing import Dict, List, Optional, Tuple

from fuzzywuzzy import fuzz

from ..core.config import settings
from ..core.llm_mapper import LLMMapper
from ..shared.logging import logger
from ..shared.models import (
    ColumnMapping,
    ColumnProfile,
    ColumnType,
    HITLRequest,
    LLMResponse,
    MappingProposal,
    MappingScore,
    SourceColumn,
)


class MappingResolver:
    """Combines LLM proposals with heuristic scoring to make mapping decisions."""
    
    def __init__(self, llm_mapper: LLMMapper):
        self.llm_mapper = llm_mapper
        self.canonical_fields = llm_mapper.get_schema_field_names()
        
        # Scoring weights from config
        self.weight_llm = settings.weight_llm
        self.weight_name = settings.weight_name
        self.weight_type = settings.weight_type
        self.weight_profile = settings.weight_profile
        
        # Decision thresholds
        self.auto_accept_threshold = settings.auto_accept_threshold
        self.hitl_threshold = settings.hitl_threshold
    
    def resolve_column_mapping(
        self, 
        column_profile: ColumnProfile,
        additional_context: Optional[str] = None
    ) -> ColumnMapping:
        """
        Resolve mapping for a single column using LLM + heuristics.
        
        Args:
            column_profile: Statistical profile of the source column
            additional_context: Optional additional context
            
        Returns:
            ColumnMapping with final decision
        """
        
        # Get LLM proposal
        llm_response = self.llm_mapper.map_column(column_profile, additional_context)
        
        # If no proposals from LLM, try heuristic-only approach
        if not llm_response.proposed_mappings:
            return self._heuristic_only_mapping(column_profile, llm_response)
        
        # Score each LLM proposal with heuristics
        scored_proposals = []
        for proposal in llm_response.proposed_mappings:
            score = self._calculate_mapping_score(
                column_profile, 
                proposal.canonical_field,
                proposal.confidence
            )
            scored_proposals.append((proposal, score))
        
        # Select best proposal
        best_proposal, best_score = max(scored_proposals, key=lambda x: x[1].final_score)
        
        # Make decision based on score
        mapping = ColumnMapping(
            source_column=column_profile.source_column,
            canonical_field=best_proposal.canonical_field,
            mapping_score=best_score,
            llm_response=llm_response,
            transform_rule=best_proposal.transform_hint
        )
        
        if best_score.auto_accept:
            mapping.status = "accepted"
            logger.info(
                f"Auto-accepted mapping: {column_profile.source_column.column} -> "
                f"{best_proposal.canonical_field} (score: {best_score.final_score:.3f})"
            )
        elif best_score.needs_hitl:
            mapping.status = "hitl_required"
            logger.info(
                f"HITL required: {column_profile.source_column.column} -> "
                f"{best_proposal.canonical_field} (score: {best_score.final_score:.3f})"
            )
        else:
            mapping.status = "rejected"
            mapping.canonical_field = None
            logger.info(
                f"Rejected mapping: {column_profile.source_column.column} "
                f"(score: {best_score.final_score:.3f})"
            )
        
        return mapping
    
    def resolve_batch_mappings(
        self,
        column_profiles: List[ColumnProfile],
        additional_context: Optional[str] = None
    ) -> List[ColumnMapping]:
        """Resolve mappings for multiple columns."""
        mappings = []
        
        for profile in column_profiles:
            mapping = self.resolve_column_mapping(profile, additional_context)
            mappings.append(mapping)
        
        # Post-process for conflicts and dependencies
        mappings = self._resolve_conflicts(mappings)
        
        return mappings
    
    def _calculate_mapping_score(
        self,
        column_profile: ColumnProfile,
        canonical_field: str,
        llm_confidence: float
    ) -> MappingScore:
        """Calculate combined score for a mapping proposal."""
        
        # LLM confidence score (already 0-1)
        llm_score = llm_confidence
        
        # Name similarity score
        name_score = self._calculate_name_similarity(
            column_profile.source_column.column,
            canonical_field
        )
        
        # Type compatibility score
        type_score = self._calculate_type_compatibility(
            column_profile.inferred_type,
            canonical_field
        )
        
        # Value profile score (based on sample values and patterns)
        profile_score = self._calculate_profile_score(
            column_profile,
            canonical_field
        )
        
        # Weighted final score
        final_score = (
            self.weight_llm * llm_score +
            self.weight_name * name_score +
            self.weight_type * type_score +
            self.weight_profile * profile_score
        )
        
        # Make decision
        auto_accept = final_score >= self.auto_accept_threshold
        needs_hitl = (
            final_score >= self.hitl_threshold and 
            final_score < self.auto_accept_threshold
        )
        
        return MappingScore(
            llm_confidence=llm_score,
            name_similarity=name_score,
            type_compatibility=type_score,
            value_range_match=profile_score,
            final_score=final_score,
            auto_accept=auto_accept,
            needs_hitl=needs_hitl
        )
    
    def _calculate_name_similarity(self, source_column: str, canonical_field: str) -> float:
        """Calculate name similarity between source and canonical field."""
        # Normalize names (lowercase, remove underscores)
        source_norm = re.sub(r'[_\s]+', '', source_column.lower())
        canonical_norm = re.sub(r'[_\s]+', '', canonical_field.lower())
        
        # Use fuzzy string matching
        ratio = fuzz.ratio(source_norm, canonical_norm) / 100.0
        partial_ratio = fuzz.partial_ratio(source_norm, canonical_norm) / 100.0
        token_ratio = fuzz.token_sort_ratio(source_column.lower(), canonical_field.lower()) / 100.0
        
        # Return best score
        return max(ratio, partial_ratio, token_ratio)
    
    def _calculate_type_compatibility(
        self, 
        source_type: ColumnType, 
        canonical_field: str
    ) -> float:
        """Calculate type compatibility score."""
        
        # Get expected type for canonical field
        field_def = self.llm_mapper.get_schema_field_by_name(canonical_field)
        if not field_def:
            return 0.0
        
        expected_type = field_def['type']
        
        # Type compatibility matrix
        compatibility = {
            # source_type -> {canonical_type: score}
            ColumnType.STRING: {
                'string': 1.0,
                'enum': 0.8,
                'date': 0.6,  # String can be parsed as date
                'decimal': 0.3,
                'int': 0.3,
                'bool': 0.2
            },
            ColumnType.INT: {
                'int': 1.0,
                'decimal': 0.9,
                'bool': 0.7,  # 0/1 can be bool
                'string': 0.3,
                'enum': 0.2,
                'date': 0.0
            },
            ColumnType.DECIMAL: {
                'decimal': 1.0,
                'int': 0.8,
                'string': 0.3,
                'bool': 0.1,
                'enum': 0.1,
                'date': 0.0
            },
            ColumnType.BOOL: {
                'bool': 1.0,
                'int': 0.7,
                'string': 0.5,
                'enum': 0.4,
                'decimal': 0.1,
                'date': 0.0
            },
            ColumnType.DATE: {
                'date': 1.0,
                'string': 0.6,
                'datetime': 0.9,
                'int': 0.1,
                'decimal': 0.0,
                'bool': 0.0,
                'enum': 0.0
            },
            ColumnType.ENUM: {
                'enum': 1.0,
                'string': 0.8,
                'bool': 0.6,  # If only 2 values
                'int': 0.3,
                'decimal': 0.1,
                'date': 0.0
            }
        }
        
        return compatibility.get(source_type, {}).get(expected_type, 0.0)
    
    def _calculate_profile_score(
        self,
        column_profile: ColumnProfile,
        canonical_field: str
    ) -> float:
        """Calculate score based on value patterns and statistical profile."""
        score = 0.0
        
        # Field-specific pattern matching
        if canonical_field in ['contract_id']:
            # ID fields should have high distinctness
            if column_profile.distinct_ratio > 0.9:
                score += 0.5
            # ID-like patterns in samples
            id_patterns = [r'\d+', r'[A-Z]+\d+', r'[A-Z0-9\-]+']
            for pattern in id_patterns:
                matches = sum(1 for val in column_profile.sample_values 
                             if re.match(pattern, str(val)))
                if matches > len(column_profile.sample_values) * 0.7:
                    score += 0.3
                    break
        
        elif canonical_field in ['effective_date', 'expiry_date']:
            # Date fields should match date patterns
            if column_profile.date_patterns:
                score += 0.6
            # Check for date-like values in samples
            date_like_count = 0
            for val in column_profile.sample_values:
                if re.match(r'\d{4}-\d{2}-\d{2}', str(val)) or \
                   re.match(r'\d{1,2}/\d{1,2}/\d{4}', str(val)):
                    date_like_count += 1
            if date_like_count > len(column_profile.sample_values) * 0.5:
                score += 0.4
        
        elif canonical_field in ['contract_value_ltv', 'contract_value_arr']:
            # Value fields should be numeric with currency symbols
            if column_profile.inferred_type in [ColumnType.DECIMAL, ColumnType.INT]:
                score += 0.4
            if column_profile.currency_symbols:
                score += 0.4
            # Check for numeric patterns in samples
            numeric_count = 0
            for val in column_profile.sample_values:
                if re.match(r'[\$£€¥]?[\d,]+\.?\d*', str(val)):
                    numeric_count += 1
            if numeric_count > len(column_profile.sample_values) * 0.7:
                score += 0.2
        
        elif canonical_field in ['status', 'contract_type']:
            # Enum fields should have low distinctness and recognizable values
            if column_profile.distinct_ratio < 0.3:
                score += 0.4
            
            # Check for status-like values
            if canonical_field == 'status':
                status_keywords = ['active', 'draft', 'expired', 'terminated', 'suspended']
                for val in column_profile.sample_values:
                    if any(keyword in str(val).lower() for keyword in status_keywords):
                        score += 0.3
                        break
            
            # Check for contract type values
            elif canonical_field == 'contract_type':
                type_keywords = ['msa', 'nda', 'sow', 'order', 'agreement']
                for val in column_profile.sample_values:
                    if any(keyword in str(val).lower() for keyword in type_keywords):
                        score += 0.3
                        break
        
        elif canonical_field in ['auto_renew']:
            # Boolean fields
            if column_profile.inferred_type == ColumnType.BOOL:
                score += 0.5
            bool_values = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f'}
            sample_values_lower = {str(val).lower() for val in column_profile.sample_values}
            if sample_values_lower.issubset(bool_values):
                score += 0.4
        
        elif canonical_field in ['renewal_term_months']:
            # Should be integer, likely small values (1-60 months)
            if column_profile.inferred_type == ColumnType.INT:
                score += 0.4
            # Check if values are in reasonable range for months
            try:
                numeric_vals = [float(val) for val in column_profile.sample_values if str(val).isdigit()]
                if numeric_vals and all(1 <= val <= 60 for val in numeric_vals):
                    score += 0.3
            except (ValueError, TypeError):
                pass
        
        return min(score, 1.0)  # Cap at 1.0
    
    def _heuristic_only_mapping(
        self,
        column_profile: ColumnProfile,
        llm_response: LLMResponse
    ) -> ColumnMapping:
        """Fallback mapping using only heuristics when LLM provides no proposals."""
        
        best_field = None
        best_score = 0.0
        
        # Try all canonical fields
        for field_name in self.canonical_fields:
            score = self._calculate_mapping_score(
                column_profile,
                field_name,
                0.0  # No LLM confidence
            )
            
            # Only consider non-LLM components for heuristic-only
            heuristic_score = (
                self.weight_name * score.name_similarity +
                self.weight_type * score.type_compatibility +
                self.weight_profile * score.value_range_match
            ) / (self.weight_name + self.weight_type + self.weight_profile)
            
            if heuristic_score > best_score:
                best_score = heuristic_score
                best_field = field_name
        
        # Create mapping
        mapping = ColumnMapping(
            source_column=column_profile.source_column,
            llm_response=llm_response
        )
        
        # Only accept if heuristic score is reasonable
        if best_score > 0.6:
            mapping.canonical_field = best_field
            mapping.status = "hitl_required"  # Always require human review for heuristic-only
            logger.info(
                f"Heuristic mapping: {column_profile.source_column.column} -> "
                f"{best_field} (score: {best_score:.3f})"
            )
        else:
            mapping.status = "rejected"
            logger.info(
                f"No suitable mapping found for {column_profile.source_column.column}"
            )
        
        return mapping
    
    def _resolve_conflicts(self, mappings: List[ColumnMapping]) -> List[ColumnMapping]:
        """Resolve conflicts where multiple source columns map to same canonical field."""
        
        # Group mappings by canonical field
        field_mappings: Dict[str, List[ColumnMapping]] = {}
        for mapping in mappings:
            if mapping.canonical_field and mapping.status != "rejected":
                if mapping.canonical_field not in field_mappings:
                    field_mappings[mapping.canonical_field] = []
                field_mappings[mapping.canonical_field].append(mapping)
        
        # Resolve conflicts
        resolved_mappings = []
        for mapping in mappings:
            if not mapping.canonical_field or mapping.status == "rejected":
                resolved_mappings.append(mapping)
                continue
            
            field_candidates = field_mappings.get(mapping.canonical_field, [])
            if len(field_candidates) <= 1:
                # No conflict
                resolved_mappings.append(mapping)
            else:
                # Conflict - keep only the best scoring one
                best_mapping = max(
                    field_candidates, 
                    key=lambda m: m.mapping_score.final_score if m.mapping_score else 0.0
                )
                
                if mapping == best_mapping:
                    resolved_mappings.append(mapping)
                else:
                    # Mark as conflict and require HITL
                    mapping.status = "hitl_required"
                    mapping.human_feedback = f"Conflict with {best_mapping.source_column.column}"
                    resolved_mappings.append(mapping)
        
        return resolved_mappings
    
    def create_hitl_request(self, mapping: ColumnMapping, column_profile: ColumnProfile) -> HITLRequest:
        """Create a human-in-the-loop review request."""
        
        proposed_mappings = []
        if mapping.llm_response and mapping.llm_response.proposed_mappings:
            proposed_mappings = mapping.llm_response.proposed_mappings
        
        reasoning = mapping.llm_response.reasoning if mapping.llm_response else "No LLM reasoning available"
        
        return HITLRequest(
            tenant=column_profile.source_column.tenant,
            source_column=column_profile.source_column,
            proposed_mappings=proposed_mappings,
            column_profile=column_profile,
            reasoning=reasoning,
            priority="normal"
        )

