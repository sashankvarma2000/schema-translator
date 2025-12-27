"""Deterministic data transformations for mapping execution."""

import re
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from dateutil import parser as date_parser

from ..shared.logging import logger
from ..shared.models import (
    ColumnMapping,
    LineageRecord,
    MappingPlan,
    SourceColumn,
    TransformResult,
)


class DataTransformer:
    """Executes deterministic transformations based on mapping plans."""
    
    def __init__(self):
        self.currency_symbols = {
            '$': 'USD',
            '€': 'EUR', 
            '£': 'GBP',
            '¥': 'JPY',
            '₹': 'INR',
            '₽': 'RUB',
            '₩': 'KRW'
        }
        
    def apply_mapping_plan(
        self,
        tenant: str,
        mapping_plan: MappingPlan,
        source_data: Dict[str, pd.DataFrame]
    ) -> TransformResult:
        """
        Apply a complete mapping plan to transform source data.
        
        Args:
            tenant: Tenant identifier
            mapping_plan: Approved mapping plan
            source_data: Dictionary of table_name -> DataFrame
            
        Returns:
            TransformResult with output path and statistics
        """
        
        logger.info(f"Applying mapping plan v{mapping_plan.version} for tenant {tenant}")
        
        # Initialize output DataFrame with canonical schema structure
        output_df = pd.DataFrame()
        lineage_records = []
        errors = []
        rows_processed = 0
        rows_successful = 0
        
        # Group mappings by source table
        table_mappings = self._group_mappings_by_table(mapping_plan.mappings)
        
        for table_name, mappings in table_mappings.items():
            if table_name not in source_data:
                logger.warning(f"Source table {table_name} not found in data")
                continue
            
            table_df = source_data[table_name]
            rows_processed += len(table_df)
            
            try:
                # Transform this table's data
                transformed_df, table_lineage, table_errors = self._transform_table(
                    table_df, mappings, mapping_plan.version
                )
                
                if not transformed_df.empty:
                    if output_df.empty:
                        output_df = transformed_df
                    else:
                        # Merge/concat with existing output
                        output_df = self._merge_table_data(output_df, transformed_df)
                    
                    rows_successful += len(transformed_df)
                
                lineage_records.extend(table_lineage)
                errors.extend(table_errors)
                
            except Exception as e:
                error_msg = f"Failed to transform table {table_name}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        # Apply derived field calculations
        output_df, derived_lineage = self._apply_derived_fields(output_df, mapping_plan.version)
        lineage_records.extend(derived_lineage)
        
        # Save output
        output_path = self._save_output(tenant, output_df, mapping_plan.version)
        
        return TransformResult(
            tenant=tenant,
            source_table="multiple",
            output_path=output_path,
            rows_processed=rows_processed,
            rows_successful=rows_successful,
            errors=errors,
            lineage={
                "records": [record.dict() for record in lineage_records],
                "mapping_version": mapping_plan.version,
                "canonical_schema_version": mapping_plan.canonical_schema_version
            }
        )
    
    def _group_mappings_by_table(
        self, 
        mappings: List[ColumnMapping]
    ) -> Dict[str, List[ColumnMapping]]:
        """Group mappings by source table."""
        table_mappings = {}
        
        for mapping in mappings:
            if mapping.status == "accepted" and mapping.canonical_field:
                table = mapping.source_column.table
                if table not in table_mappings:
                    table_mappings[table] = []
                table_mappings[table].append(mapping)
        
        return table_mappings
    
    def _transform_table(
        self,
        source_df: pd.DataFrame,
        mappings: List[ColumnMapping],
        mapping_version: str
    ) -> Tuple[pd.DataFrame, List[LineageRecord], List[str]]:
        """Transform a single source table."""
        
        output_df = pd.DataFrame()
        lineage_records = []
        errors = []
        
        for mapping in mappings:
            source_col = mapping.source_column.column
            canonical_field = mapping.canonical_field
            
            if source_col not in source_df.columns:
                errors.append(f"Source column {source_col} not found in data")
                continue
            
            try:
                # Apply transformation
                transformed_values, transform_applied = self._transform_column(
                    source_df[source_col],
                    canonical_field,
                    mapping.transform_rule,
                    source_df  # Pass full DataFrame for derived calculations
                )
                
                output_df[canonical_field] = transformed_values
                
                # Create lineage record
                lineage_record = LineageRecord(
                    output_field=canonical_field,
                    source_columns=[mapping.source_column],
                    transform_applied=transform_applied,
                    mapping_version=mapping_version,
                    prompt_version="v1",  # Should come from config
                    confidence_score=mapping.mapping_score.final_score if mapping.mapping_score else 0.0
                )
                lineage_records.append(lineage_record)
                
            except Exception as e:
                error_msg = f"Failed to transform {source_col} -> {canonical_field}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        return output_df, lineage_records, errors
    
    def _transform_column(
        self,
        source_series: pd.Series,
        canonical_field: str,
        transform_rule: Optional[str],
        full_df: pd.DataFrame
    ) -> Tuple[pd.Series, str]:
        """Transform a single column based on its canonical field type."""
        
        if transform_rule:
            # Apply custom transformation rule
            return self._apply_transform_rule(source_series, transform_rule, full_df)
        
        # Apply default transformations based on canonical field
        if canonical_field in ['contract_id', 'party_buyer', 'party_seller', 'governing_law', 'jurisdiction']:
            return self._transform_string(source_series), "string_normalization"
        
        elif canonical_field in ['effective_date', 'expiry_date']:
            return self._transform_date(source_series), "date_parsing"
        
        elif canonical_field in ['contract_value_ltv', 'contract_value_arr']:
            return self._transform_currency(source_series), "currency_normalization"
        
        elif canonical_field == 'auto_renew':
            return self._transform_boolean(source_series), "boolean_conversion"
        
        elif canonical_field == 'renewal_term_months':
            return self._transform_integer(source_series), "integer_conversion"
        
        elif canonical_field in ['status', 'contract_type']:
            return self._transform_enum(source_series, canonical_field), "enum_normalization"
        
        else:
            # Default: treat as string
            return self._transform_string(source_series), "default_string"
    
    def _apply_transform_rule(
        self,
        source_series: pd.Series,
        transform_rule: str,
        full_df: pd.DataFrame
    ) -> Tuple[pd.Series, str]:
        """Apply a custom transformation rule."""
        
        if "parse_date" in transform_rule:
            # Extract date format if specified
            format_match = re.search(r"format='([^']+)'", transform_rule)
            date_format = format_match.group(1) if format_match else None
            return self._transform_date(source_series, date_format), f"custom_date_parse: {transform_rule}"
        
        elif "parse_currency" in transform_rule:
            # Extract default currency if specified
            currency_match = re.search(r"default_currency='([^']+)'", transform_rule)
            default_currency = currency_match.group(1) if currency_match else 'USD'
            return self._transform_currency(source_series, default_currency), f"custom_currency_parse: {transform_rule}"
        
        elif "+" in transform_rule and "days" in transform_rule:
            # Date arithmetic: effective_date + (term_months * 30) days
            return self._apply_date_arithmetic(transform_rule, full_df), f"date_arithmetic: {transform_rule}"
        
        else:
            # Unknown rule, treat as string
            logger.warning(f"Unknown transform rule: {transform_rule}")
            return self._transform_string(source_series), f"unknown_rule: {transform_rule}"
    
    def _transform_string(self, series: pd.Series) -> pd.Series:
        """Transform to string with basic normalization."""
        return series.astype(str).str.strip()
    
    def _transform_date(
        self, 
        series: pd.Series, 
        date_format: Optional[str] = None
    ) -> pd.Series:
        """Transform to date."""
        def parse_date(value):
            if pd.isna(value):
                return None
            
            try:
                if date_format:
                    return datetime.strptime(str(value), date_format).date()
                else:
                    return date_parser.parse(str(value)).date()
            except (ValueError, TypeError):
                return None
        
        return series.apply(parse_date)
    
    def _transform_currency(
        self, 
        series: pd.Series, 
        default_currency: str = 'USD'
    ) -> pd.Series:
        """Transform currency values to decimal."""
        def parse_currency(value):
            if pd.isna(value):
                return None
            
            try:
                # Remove currency symbols and commas
                clean_value = re.sub(r'[^\d.-]', '', str(value))
                if clean_value:
                    return Decimal(clean_value)
                return None
            except (InvalidOperation, ValueError):
                return None
        
        return series.apply(parse_currency)
    
    def _transform_boolean(self, series: pd.Series) -> pd.Series:
        """Transform to boolean."""
        def parse_boolean(value):
            if pd.isna(value):
                return None
            
            str_value = str(value).lower().strip()
            if str_value in ['true', '1', 'yes', 't', 'y']:
                return True
            elif str_value in ['false', '0', 'no', 'f', 'n']:
                return False
            else:
                return None
        
        return series.apply(parse_boolean)
    
    def _transform_integer(self, series: pd.Series) -> pd.Series:
        """Transform to integer."""
        def parse_integer(value):
            if pd.isna(value):
                return None
            
            try:
                return int(float(str(value)))
            except (ValueError, TypeError):
                return None
        
        return series.apply(parse_integer)
    
    def _transform_enum(self, series: pd.Series, canonical_field: str) -> pd.Series:
        """Transform to enum values."""
        
        # Define enum mappings
        enum_mappings = {
            'status': {
                'draft': 'DRAFT',
                'active': 'ACTIVE', 
                'live': 'ACTIVE',
                'suspended': 'SUSPENDED',
                'paused': 'SUSPENDED',
                'terminated': 'TERMINATED',
                'cancelled': 'TERMINATED',
                'expired': 'EXPIRED',
                'ended': 'EXPIRED'
            },
            'contract_type': {
                'msa': 'MSA',
                'master service agreement': 'MSA',
                'nda': 'NDA',
                'non disclosure agreement': 'NDA',
                'sow': 'SOW',
                'statement of work': 'SOW',
                'order form': 'ORDER_FORM',
                'purchase order': 'ORDER_FORM',
                'other': 'OTHER'
            }
        }
        
        def map_enum(value):
            if pd.isna(value):
                return None
            
            str_value = str(value).lower().strip()
            mapping = enum_mappings.get(canonical_field, {})
            
            # Direct lookup
            if str_value in mapping:
                return mapping[str_value]
            
            # Partial match
            for key, mapped_value in mapping.items():
                if key in str_value or str_value in key:
                    return mapped_value
            
            # Default to OTHER for contract_type, None for others
            if canonical_field == 'contract_type':
                return 'OTHER'
            
            return None
        
        return series.apply(map_enum)
    
    def _apply_date_arithmetic(
        self, 
        transform_rule: str, 
        full_df: pd.DataFrame
    ) -> pd.Series:
        """Apply date arithmetic transformations."""
        
        # Example: "effective_date + (renewal_term_months * 30) days"
        # Example: "status_date + days_remaining days"
        
        if "effective_date" in transform_rule and "renewal_term_months" in transform_rule:
            # Calculate expiry from effective date and term
            if 'effective_date' in full_df.columns and 'renewal_term_months' in full_df.columns:
                def calc_expiry(row):
                    try:
                        effective = row['effective_date']
                        term_months = row['renewal_term_months']
                        
                        if pd.isna(effective) or pd.isna(term_months):
                            return None
                        
                        if isinstance(effective, str):
                            effective = date_parser.parse(effective).date()
                        
                        # Approximate: 30 days per month
                        days_to_add = int(term_months) * 30
                        return effective + timedelta(days=days_to_add)
                    
                    except (ValueError, TypeError):
                        return None
                
                return full_df.apply(calc_expiry, axis=1)
        
        elif "status_date" in transform_rule and "days_remaining" in transform_rule:
            # Calculate expiry from status date and days remaining
            if 'status_date' in full_df.columns and 'days_remaining' in full_df.columns:
                def calc_expiry_from_remaining(row):
                    try:
                        status_date = row['status_date']
                        days_remaining = row['days_remaining']
                        
                        if pd.isna(status_date) or pd.isna(days_remaining):
                            return None
                        
                        if isinstance(status_date, str):
                            status_date = date_parser.parse(status_date).date()
                        
                        return status_date + timedelta(days=int(days_remaining))
                    
                    except (ValueError, TypeError):
                        return None
                
                return full_df.apply(calc_expiry_from_remaining, axis=1)
        
        # If we can't parse the rule, return None series
        logger.warning(f"Could not apply date arithmetic rule: {transform_rule}")
        return pd.Series([None] * len(full_df))
    
    def _apply_derived_fields(
        self, 
        df: pd.DataFrame, 
        mapping_version: str
    ) -> Tuple[pd.DataFrame, List[LineageRecord]]:
        """Apply derived field calculations."""
        
        lineage_records = []
        
        # Derive contract_value_arr from contract_value_ltv if possible
        if ('contract_value_ltv' in df.columns and 
            'renewal_term_months' in df.columns and
            'contract_value_arr' not in df.columns):
            
            def calc_arr(row):
                try:
                    ltv = row['contract_value_ltv']
                    term_months = row['renewal_term_months']
                    
                    if pd.isna(ltv) or pd.isna(term_months) or term_months == 0:
                        return None
                    
                    return ltv / (term_months / 12)  # Convert to annual
                
                except (ValueError, TypeError, ZeroDivisionError):
                    return None
            
            df['contract_value_arr'] = df.apply(calc_arr, axis=1)
            
            # Add lineage
            lineage_records.append(LineageRecord(
                output_field='contract_value_arr',
                source_columns=[],  # Derived from other canonical fields
                transform_applied='derived: contract_value_ltv / (renewal_term_months / 12)',
                mapping_version=mapping_version,
                prompt_version="v1",
                confidence_score=1.0
            ))
        
        return df, lineage_records
    
    def _merge_table_data(
        self, 
        existing_df: pd.DataFrame, 
        new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge data from multiple source tables."""
        
        # Simple concatenation for now
        # In a real system, you'd want more sophisticated merging logic
        # based on common keys like contract_id
        
        try:
            return pd.concat([existing_df, new_df], ignore_index=True, sort=False)
        except Exception as e:
            logger.error(f"Failed to merge table data: {e}")
            return existing_df
    
    def _save_output(
        self, 
        tenant: str, 
        df: pd.DataFrame, 
        mapping_version: str
    ) -> str:
        """Save transformed data to output file."""
        
        from ..core.config import settings
        
        output_dir = settings.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{tenant}_canonical_v{mapping_version}_{timestamp}.parquet"
        output_path = output_dir / filename
        
        # Save as Parquet for efficiency
        df.to_parquet(output_path, index=False)
        
        logger.info(f"Saved {len(df)} rows to {output_path}")
        return str(output_path)

