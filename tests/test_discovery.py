"""Tests for schema discovery module."""

import pytest
import pandas as pd
from pathlib import Path

from src.app.core.discovery import SchemaDiscoverer
from src.app.shared.models import ColumnType


class TestSchemaDiscoverer:
    """Test schema discovery functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Use actual test directories
        self.schemas_dir = Path("customer_schemas")
        self.samples_dir = Path("customer_samples")
        self.discoverer = SchemaDiscoverer(self.schemas_dir, self.samples_dir)
    
    def test_discover_tenant_schema(self):
        """Test tenant schema discovery."""
        # Test with tenant_A
        schema_tables = self.discoverer.discover_tenant_schema("tenant_A")
        
        assert "contracts" in schema_tables
        contracts_columns = schema_tables["contracts"]
        
        # Check we have expected columns
        column_names = [col.column for col in contracts_columns]
        assert "contract_id" in column_names
        assert "customer_name" in column_names
        assert "contract_status" in column_names
    
    def test_column_profiling(self):
        """Test column profiling functionality."""
        profiles = self.discoverer.profile_tenant_columns("tenant_A")
        
        assert len(profiles) > 0
        
        # Find contract_id profile
        contract_id_profile = next(
            (p for p in profiles if p.source_column.column == "contract_id"),
            None
        )
        
        assert contract_id_profile is not None
        assert contract_id_profile.distinct_ratio > 0.8  # Should be highly distinct
        assert contract_id_profile.inferred_type == ColumnType.STRING
    
    def test_type_inference(self):
        """Test column type inference."""
        # Create test data
        test_data = pd.DataFrame({
            'string_col': ['a', 'b', 'c'],
            'int_col': [1, 2, 3],
            'decimal_col': [1.5, 2.5, 3.5],
            'bool_col': [True, False, True],
            'date_col': ['2023-01-01', '2023-02-01', '2023-03-01']
        })
        
        # Test string
        inferred = self.discoverer._infer_column_type(test_data['string_col'])
        assert inferred == ColumnType.STRING
        
        # Test integer
        inferred = self.discoverer._infer_column_type(test_data['int_col'])
        assert inferred == ColumnType.INT
        
        # Test decimal
        inferred = self.discoverer._infer_column_type(test_data['decimal_col'])
        assert inferred == ColumnType.DECIMAL
        
        # Test boolean
        inferred = self.discoverer._infer_column_type(test_data['bool_col'])
        assert inferred == ColumnType.BOOL
    
    def test_date_pattern_detection(self):
        """Test date pattern detection."""
        # Create test series with different date formats
        date_series = pd.Series(['2023-01-01', '2023-02-01', '2023-03-01'])
        patterns = self.discoverer._detect_date_patterns(date_series)
        assert 'YYYY-MM-DD' in patterns
        
        us_date_series = pd.Series(['01/01/2023', '02/01/2023', '03/01/2023'])
        patterns = self.discoverer._detect_date_patterns(us_date_series)
        assert 'MM/DD/YYYY' in patterns
    
    def test_currency_detection(self):
        """Test currency symbol detection."""
        currency_series = pd.Series(['$100.00', '$200.50', '$300.75'])
        symbols = self.discoverer._detect_currency_symbols(currency_series)
        assert '$' in symbols
        
        mixed_currency_series = pd.Series(['£100.00', '€200.50', '$300.75'])
        symbols = self.discoverer._detect_currency_symbols(mixed_currency_series)
        assert '£' in symbols
        assert '€' in symbols
        assert '$' in symbols


if __name__ == "__main__":
    pytest.main([__file__])
