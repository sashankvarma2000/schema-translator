"""Schema discovery and column profiling."""

import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import yaml
from dateutil import parser as date_parser

from ..shared.logging import logger
from ..shared.models import (
    ColumnProfile,
    ColumnType,
    SourceColumn,
)


class SchemaDiscoverer:
    """Discovers and profiles database schemas from YAML definitions and CSV samples."""
    
    def __init__(self, schemas_dir: Path, samples_dir: Path):
        self.schemas_dir = schemas_dir
        self.samples_dir = samples_dir
        
    def discover_tenant_schema(self, tenant: str) -> Dict[str, List[SourceColumn]]:
        """Discover schema for a specific tenant."""
        schema_path = self.schemas_dir / tenant / "schema.yaml"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r') as f:
            schema_data = yaml.safe_load(f)
        
        tables = {}
        for table_name, table_def in schema_data.get('tables', {}).items():
            columns = []
            for col_name, col_def in table_def.get('columns', {}).items():
                column = SourceColumn(
                    tenant=tenant,
                    table=table_name,
                    column=col_name,
                    type=col_def.get('type'),
                    description=col_def.get('description'),
                    nullable=col_def.get('nullable', True)
                )
                columns.append(column)
            tables[table_name] = columns
            
        logger.info(f"Discovered {len(tables)} tables for tenant {tenant}")
        return tables
    
    def profile_column(
        self, 
        source_column: SourceColumn, 
        sample_data: Optional[pd.DataFrame] = None
    ) -> ColumnProfile:
        """Generate statistical profile for a source column."""
        
        if sample_data is None:
            sample_data = self._load_sample_data(source_column.tenant, source_column.table)
        
        if source_column.column not in sample_data.columns:
            logger.warning(
                f"Column {source_column.column} not found in sample data for "
                f"{source_column.tenant}.{source_column.table}"
            )
            return self._empty_profile(source_column)
        
        col_data = sample_data[source_column.column]
        
        # Basic statistics
        total_rows = len(col_data)
        non_null_count = col_data.notna().sum()
        distinct_count = col_data.nunique()
        distinct_ratio = distinct_count / total_rows if total_rows > 0 else 0.0
        
        # Sample values (non-null, converted to string)
        sample_values = (
            col_data.dropna()
            .astype(str)
            .unique()[:10]
            .tolist()
        )
        
        # Type inference
        inferred_type = self._infer_column_type(col_data)
        
        # Pattern detection
        date_patterns = self._detect_date_patterns(col_data)
        currency_symbols = self._detect_currency_symbols(col_data)
        
        # Co-occurring columns (other columns in the same table)
        cooccurring_columns = [
            col for col in sample_data.columns 
            if col != source_column.column
        ][:5]  # Limit to top 5
        
        return ColumnProfile(
            source_column=source_column,
            total_rows=total_rows,
            non_null_count=non_null_count,
            distinct_count=distinct_count,
            distinct_ratio=distinct_ratio,
            sample_values=sample_values,
            inferred_type=inferred_type,
            date_patterns=date_patterns,
            currency_symbols=currency_symbols,
            cooccurring_columns=cooccurring_columns
        )
    
    def profile_tenant_columns(self, tenant: str) -> List[ColumnProfile]:
        """Profile all columns for a tenant."""
        schema_tables = self.discover_tenant_schema(tenant)
        profiles = []
        
        for table_name, columns in schema_tables.items():
            try:
                sample_data = self._load_sample_data(tenant, table_name)
                for column in columns:
                    profile = self.profile_column(column, sample_data)
                    profiles.append(profile)
            except Exception as e:
                logger.error(f"Error profiling table {table_name}: {e}")
                # Create empty profiles for columns we can't sample
                for column in columns:
                    profiles.append(self._empty_profile(column))
        
        logger.info(f"Generated {len(profiles)} column profiles for tenant {tenant}")
        return profiles
    
    def _load_sample_data(self, tenant: str, table: str) -> pd.DataFrame:
        """Load sample CSV data for a table."""
        # Try multiple possible file names
        possible_files = [
            f"{table}.csv",
            f"{table}_sample.csv",
            f"{table}_data.csv"
        ]
        
        tenant_dir = self.samples_dir / tenant
        if not tenant_dir.exists():
            raise FileNotFoundError(f"Sample directory not found: {tenant_dir}")
        
        for filename in possible_files:
            file_path = tenant_dir / filename
            if file_path.exists():
                logger.debug(f"Loading sample data from {file_path}")
                return pd.read_csv(file_path)
        
        # If no exact match, look for any CSV files
        csv_files = list(tenant_dir.glob("*.csv"))
        if csv_files:
            logger.warning(
                f"No exact match for table {table}, using {csv_files[0].name}"
            )
            return pd.read_csv(csv_files[0])
        
        raise FileNotFoundError(
            f"No sample data found for {tenant}.{table} in {tenant_dir}"
        )
    
    def _empty_profile(self, source_column: SourceColumn) -> ColumnProfile:
        """Create an empty profile when data is unavailable."""
        return ColumnProfile(
            source_column=source_column,
            total_rows=0,
            non_null_count=0,
            distinct_count=0,
            distinct_ratio=0.0,
            sample_values=[],
            inferred_type=ColumnType.STRING,
            date_patterns=[],
            currency_symbols=[],
            cooccurring_columns=[]
        )
    
    def _infer_column_type(self, col_data: pd.Series) -> ColumnType:
        """Infer the most likely column type from sample data."""
        # Remove null values for type inference
        non_null_data = col_data.dropna()
        
        if len(non_null_data) == 0:
            return ColumnType.STRING
        
        # Convert to string for pattern matching
        str_data = non_null_data.astype(str)
        
        # Check for boolean patterns
        bool_values = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f'}
        if set(str_data.str.lower().unique()).issubset(bool_values):
            return ColumnType.BOOL
        
        # Check for integer patterns
        if self._is_numeric_type(non_null_data, int):
            return ColumnType.INT
        
        # Check for decimal patterns
        if self._is_numeric_type(non_null_data, float):
            return ColumnType.DECIMAL
        
        # Check for date patterns
        if self._is_date_type(str_data):
            return ColumnType.DATE
        
        # Check for enum-like patterns (low cardinality)
        unique_count = non_null_data.nunique()
        total_count = len(non_null_data)
        if unique_count <= 10 and unique_count / total_count < 0.5:
            return ColumnType.ENUM
        
        return ColumnType.STRING
    
    def _is_numeric_type(self, data: pd.Series, numeric_type: type) -> bool:
        """Check if data can be converted to a numeric type."""
        try:
            if numeric_type == int:
                pd.to_numeric(data, errors='raise', downcast='integer')
            else:
                pd.to_numeric(data, errors='raise')
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_date_type(self, str_data: pd.Series) -> bool:
        """Check if string data represents dates."""
        sample_size = min(10, len(str_data))
        sample = str_data.head(sample_size)
        
        parsed_count = 0
        for value in sample:
            try:
                date_parser.parse(str(value))
                parsed_count += 1
            except (ValueError, TypeError):
                continue
        
        # If more than 70% can be parsed as dates, consider it a date column
        return parsed_count / len(sample) > 0.7
    
    def _detect_date_patterns(self, col_data: pd.Series) -> List[str]:
        """Detect common date patterns in the data."""
        patterns = []
        str_data = col_data.dropna().astype(str)
        
        if len(str_data) == 0:
            return patterns
        
        # Common date regex patterns
        date_regexes = {
            'YYYY-MM-DD': r'^\d{4}-\d{2}-\d{2}$',
            'MM/DD/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',
            'DD/MM/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',
            'YYYY/MM/DD': r'^\d{4}/\d{1,2}/\d{1,2}$',
            'ISO_DATETIME': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
        }
        
        for pattern_name, regex in date_regexes.items():
            matches = str_data.str.match(regex).sum()
            if matches / len(str_data) > 0.5:  # More than 50% match
                patterns.append(pattern_name)
        
        return patterns
    
    def _detect_currency_symbols(self, col_data: pd.Series) -> List[str]:
        """Detect currency symbols in the data."""
        symbols = set()
        str_data = col_data.dropna().astype(str)
        
        currency_regex = r'[\$£€¥₹₽₩]'
        for value in str_data.head(20):  # Check first 20 values
            matches = re.findall(currency_regex, str(value))
            symbols.update(matches)
        
        return list(symbols)

