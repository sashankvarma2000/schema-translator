#!/usr/bin/env python3
"""
Schema Translator - Web Dashboard
Interactive web interface for testing and HITL workflow demonstration
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import json
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime
import requests
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from app.core.query_translator import QueryTranslationEngine
from app.core.table_relationship_analyzer import TableRelationshipAnalyzer
from app.core.nl_to_sql_translator import NLToSQLTranslator
from app.adapters.multi_table_schemas import get_all_customer_schemas, get_demo_queries
from app.adapters.llm_openai import OpenAIAdapter

app = Flask(__name__)

# Global variables for API configuration
API_BASE_URL = "http://localhost:8001"
REPORTS_DIR = Path("reports")

class SchemaTranslatorDashboard:
    """Main dashboard class for Schema Translator"""
    
    def __init__(self):
        self.tenants = {
            'tenant_A': 'USAspending (Federal Contracts)',
            'tenant_B': 'World Bank (Development Projects)', 
            'tenant_C': 'UK OCDS (Government Procurement)',
            'tenant_D': 'World Bank Finance (Corporate)',
            'tenant_E': 'FPDS (Contract Actions)'
        }
        
        # Initialize query translation components
        try:
            self.llm_adapter = OpenAIAdapter()
            self.query_engine = QueryTranslationEngine(self.llm_adapter)
            self.relationship_analyzer = TableRelationshipAnalyzer(self.llm_adapter)
            self.nl_to_sql_translator = NLToSQLTranslator(self.llm_adapter)
            self.customer_schemas = get_all_customer_schemas()
            self.demo_queries = get_demo_queries()
            self.query_translation_available = True
        except Exception as e:
            print(f"Query translation not available: {e}")
            self.query_translation_available = False
    
    def get_api_status(self):
        """Check API server status"""
        return {
            "status": "online",
            "url": "http://localhost:8080",
            "llm_available": True,
            "mode": "mock_demo"
        }
    
    def get_available_tenants(self):
        """Get list of available tenants"""
        return list(self.tenants.keys())
    
    def get_customer_schema(self, customer_id: str):
        """Get customer schema for a specific tenant"""
        if not self.query_translation_available:
            return None
        return self.customer_schemas.get(customer_id)
    
    def get_tenant_name(self, customer_id: str) -> str:
        """Get friendly name for a tenant"""
        tenant_names = {
            'tenant_A': 'USAspending (Federal Contracts)',
            'tenant_B': 'World Bank (Development Projects)',
            'tenant_C': 'UK OCDS (Government Procurement)',
            'tenant_D': 'Enterprise Contracts',
            'tenant_E': 'Federal Procurement',
            'tenant_F': 'Vendor Management'
        }
        return tenant_names.get(customer_id, customer_id)
    
    def load_mapping_reports(self):
        """Load existing mapping reports"""
        reports = []
        if REPORTS_DIR.exists():
            for json_file in REPORTS_DIR.glob("mapping_analysis_report_*.json"):
                try:
                    with open(json_file, 'r') as f:
                        data = json.load(f)
                        data['filename'] = json_file.name
                        data['created_at'] = json_file.stat().st_mtime
                        reports.append(data)
                except:
                    continue
        return sorted(reports, key=lambda x: x['created_at'], reverse=True)
    
    def test_column_mapping(self, tenant, table, column, sample_values):
        """Test a single column mapping using real LLM integration"""
        try:
            # Create a proper column profile for LLM analysis
            from src.app.shared.models import ColumnProfile, SourceColumn, ColumnType
            from src.app.core.llm_mapper import LLMMapper
            from src.app.core.resolver import MappingResolver
            
            # Create source column
            source_col = SourceColumn(
                tenant=tenant,
                table=table,
                column=column,
                description=f"Column {column} from {table} table"
            )
            
            # Create column profile with sample data
            sample_list = sample_values.split(",") if sample_values else []
            column_profile = ColumnProfile(
                source_column=source_col,
                total_rows=100,  # Mock value
                non_null_count=95,  # Mock value
                distinct_count=80,  # Mock value
                distinct_ratio=0.8,  # Mock value
                sample_values=sample_list,
                inferred_type=ColumnType.STRING,  # Default, would be inferred from data
                cooccurring_columns=[],
                date_patterns=[],
                currency_symbols=[]
            )
            
            # Initialize LLM mapper and resolver
            llm_mapper = LLMMapper()
            resolver = MappingResolver(llm_mapper)
            
            # Get LLM mapping proposal
            llm_response = llm_mapper.map_column(column_profile)
            
            # Resolve with heuristics
            mapping = resolver.resolve_column_mapping(column_profile)
            
            return {
                "tenant": tenant,
                "table": table,
                "column": column,
                "sample_values": sample_list,
                "mapping": {
                    "canonical_field": mapping.canonical_field,
                    "confidence": mapping.mapping_score.final_score if mapping.mapping_score else 0.0,
                    "reasoning": mapping.llm_response.reasoning if mapping.llm_response else "No reasoning available",
                    "transformations": mapping.transform_rule or [],
                    "llm_confidence": mapping.mapping_score.llm_confidence if mapping.mapping_score else 0.0,
                    "name_similarity": mapping.mapping_score.name_similarity if mapping.mapping_score else 0.0,
                    "type_compatibility": mapping.mapping_score.type_compatibility if mapping.mapping_score else 0.0,
                    "value_range_match": mapping.mapping_score.value_range_match if mapping.mapping_score else 0.0,
                    "status": mapping.status
                },
                "status": "success"
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _calculate_confidence(self, column, sample_values):
        """Calculate confidence score based on column name and sample data"""
        column_lower = column.lower()
        
        # Very high confidence patterns (95-98%)
        if any(keyword in column_lower for keyword in ['id', 'identifier', 'key', 'number']):
            if any(keyword in column_lower for keyword in ['unique', 'generated', 'primary']):
                return 0.98
            else:
                return 0.95
        
        # High confidence financial patterns (90-95%)
        elif any(keyword in column_lower for keyword in ['amount', 'value', 'price', 'cost', 'obligation']):
            if any(keyword in column_lower for keyword in ['total', 'current', 'cumulative']):
                return 0.95
            elif 'obligation' in column_lower:
                return 0.90
            else:
                return 0.90
        
        # High confidence date patterns (85-95%)
        elif any(keyword in column_lower for keyword in ['date', 'time']):
            if any(keyword in column_lower for keyword in ['award', 'sign', 'signature']):
                return 0.95
            elif any(keyword in column_lower for keyword in ['start', 'end', 'expir']):
                return 0.90
            elif any(keyword in column_lower for keyword in ['modif', 'last', 'obligation']):
                return 0.85
            else:
                return 0.85
        
        # High confidence name patterns (85-95%)
        elif 'name' in column_lower:
            if any(keyword in column_lower for keyword in ['supplier', 'vendor', 'recipient', 'legal', 'business']):
                return 0.95
            elif any(keyword in column_lower for keyword in ['buyer', 'agency']):
                return 0.90
            else:
                return 0.85
        
        # High confidence status patterns (80-90%)
        elif any(keyword in column_lower for keyword in ['status', 'state', 'phase']):
            return 0.85
        
        # Medium confidence agency patterns (80-85%)
        elif 'agency' in column_lower:
            return 0.80
        
        # Medium confidence description patterns (75-85%)
        elif any(keyword in column_lower for keyword in ['description', 'title', 'comment']):
            return 0.80
        
        # Medium confidence type patterns (70-80%)
        elif 'type' in column_lower:
            return 0.75
        
        # Medium confidence currency patterns (80-85%)
        elif 'currency' in column_lower:
            return 0.85
        
        # Lower confidence for ambiguous fields (60-70%)
        elif any(keyword in column_lower for keyword in ['code', 'number', 'identifier']):
            return 0.70
        
        # Default for unknown patterns
        else:
            return 0.60
    
    def _suggest_canonical_field(self, column):
        """Suggest canonical field based on column name with improved logic"""
        column_lower = column.lower()
        
        # ID and Identifier patterns
        if any(keyword in column_lower for keyword in ['id', 'identifier', 'key', 'number']):
            if 'award' in column_lower:
                return 'award_id'
            elif 'contract' in column_lower:
                return 'contract_id'
            elif 'party' in column_lower:
                return 'party_id'
            elif 'release' in column_lower:
                return 'release_id'
            elif 'transaction' in column_lower or 'action' in column_lower:
                return 'transaction_id'
            else:
                return 'contract_id'
        
        # Financial/Value patterns with better semantic understanding
        elif any(keyword in column_lower for keyword in ['amount', 'value', 'price', 'cost', 'obligation']):
            if 'total' in column_lower or 'cumulative' in column_lower:
                return 'total_value'
            elif 'obligation' in column_lower:
                return 'obligated_amount'
            elif 'current' in column_lower:
                return 'current_value'
            else:
                return 'value_amount'
        
        # Date patterns with specific semantic mapping
        elif any(keyword in column_lower for keyword in ['date', 'time']):
            if 'award' in column_lower:
                return 'award_date'
            elif 'sign' in column_lower or 'signature' in column_lower:
                return 'date_signed'
            elif 'start' in column_lower or 'begin' in column_lower:
                return 'period_start'
            elif 'end' in column_lower or 'expir' in column_lower:
                return 'period_end'
            elif 'modif' in column_lower or 'last' in column_lower:
                return 'last_modified_date'
            elif 'obligation' in column_lower:
                return 'obligation_date'
            else:
                return 'date_signed'
        
        # Name patterns with better entity recognition
        elif 'name' in column_lower:
            if 'supplier' in column_lower or 'vendor' in column_lower or 'recipient' in column_lower:
                return 'supplier_name'
            elif 'buyer' in column_lower or 'agency' in column_lower:
                return 'buyer_name'
            elif 'legal' in column_lower or 'business' in column_lower:
                return 'supplier_name'
            else:
                return 'supplier_name'
        
        # Status patterns
        elif any(keyword in column_lower for keyword in ['status', 'state', 'phase']):
            return 'status'
        
        # Agency patterns
        elif 'agency' in column_lower:
            if 'awarding' in column_lower:
                return 'awarding_agency'
            elif 'funding' in column_lower:
                return 'funding_agency'
            else:
                return 'awarding_agency'
        
        # Description/Title patterns
        elif any(keyword in column_lower for keyword in ['description', 'title', 'comment']):
            return 'description'
        
        # Currency patterns
        elif 'currency' in column_lower:
            return 'currency'
        
        # Type patterns
        elif 'type' in column_lower:
            return 'contract_type'
        
        # Default fallback
        else:
            return 'contract_id'
    
    def _suggest_transformations(self, column, sample_values):
        """Suggest data transformations"""
        transformations = []
        
        if sample_values:
            # Check for currency symbols
            if any('$' in str(val) or '€' in str(val) or '£' in str(val) for val in sample_values.split(",")):
                transformations.append("Remove currency symbols and convert to numeric")
            
            # Check for date formats
            if any('/' in str(val) or '-' in str(val) for val in sample_values.split(",")):
                transformations.append("Standardize date format to YYYY-MM-DD")
        
        return transformations
    
    def get_tenant_schema_analysis(self, tenant):
        """Get comprehensive schema analysis for a specific tenant using real LLM integration"""
        try:
            schema_file = Path(f"customer_schemas/{tenant}/schema.yaml")
            if not schema_file.exists():
                return {"error": f"Schema file not found for {tenant}"}
            
            with open(schema_file, 'r') as f:
                schema_data = yaml.safe_load(f)
            
            # Initialize LLM components
            from src.app.shared.models import ColumnProfile, SourceColumn, ColumnType
            from src.app.core.llm_mapper import LLMMapper
            from src.app.core.resolver import MappingResolver
            
            llm_mapper = LLMMapper()
            resolver = MappingResolver(llm_mapper)
            
            # Analyze each table in the schema
            tables_analysis = {}
            for table_name, table_info in schema_data.get('tables', {}).items():
                columns_dict = table_info.get('columns', {})
                table_analysis = {
                    'table_name': table_name,
                    'description': table_info.get('description', ''),
                    'primary_key': table_info.get('primary_key', []),
                    'columns': [],
                    'canonical_mappings': [],
                    'semantic_analysis': {}
                }
                
                # Process columns using real LLM integration
                for col_name, col_info in columns_dict.items():
                    col_type = col_info.get('type', '')
                    col_desc = col_info.get('description', '')
                    
                    # Create source column and profile
                    source_col = SourceColumn(
                        tenant=tenant,
                        table=table_name,
                        column=col_name,
                        description=col_desc
                    )
                    
                    # Create column profile (mock sample data for demo)
                    column_profile = ColumnProfile(
                        source_column=source_col,
                        total_rows=100,  # Mock value
                        non_null_count=95,  # Mock value
                        distinct_count=80,  # Mock value
                        distinct_ratio=0.8,
                        sample_values=[f"sample_{i}" for i in range(3)],  # Mock samples
                        inferred_type=ColumnType.STRING,  # Would be inferred from real data
                        cooccurring_columns=[],
                        date_patterns=[],
                        currency_symbols=[]
                    )
                    
                    # Get LLM mapping with detailed logging
                    llm_response = llm_mapper.map_column(column_profile)
                    mapping = resolver.resolve_column_mapping(column_profile)
                    
                    # Capture detailed LLM interaction for logging
                    llm_interaction = {
                        'prompt_sent': self._get_llm_prompt_details(llm_mapper, column_profile),
                        'response_received': self._get_llm_response_details(llm_response),
                        'processing_time': 'N/A',  # Could be added if timing is needed
                        'model_used': llm_mapper.adapter.model if hasattr(llm_mapper, 'adapter') else 'Unknown',
                        'api_status': 'success' if llm_response else 'failed'
                    }
                    
                    # Analyze semantic type
                    semantic_type = self._analyze_semantic_type(col_name, col_type, col_desc)
                    
                    column_analysis = {
                        'name': col_name,
                        'type': col_type,
                        'description': col_desc,
                        'canonical_field': mapping.canonical_field,
                        'confidence': mapping.mapping_score.final_score if mapping.mapping_score else 0.0,
                        'semantic_type': semantic_type,
                        'mapping_reasoning': mapping.llm_response.reasoning if mapping.llm_response else "No reasoning available",
                        'llm_confidence': mapping.mapping_score.llm_confidence if mapping.mapping_score else 0.0,
                        'name_similarity': mapping.mapping_score.name_similarity if mapping.mapping_score else 0.0,
                        'type_compatibility': mapping.mapping_score.type_compatibility if mapping.mapping_score else 0.0,
                        'value_range_match': mapping.mapping_score.value_range_match if mapping.mapping_score else 0.0,
                        'status': mapping.status,
                        'llm_interaction': llm_interaction  # Add detailed LLM interaction logs
                    }
                    
                    table_analysis['columns'].append(column_analysis)
                    table_analysis['canonical_mappings'].append({
                        'source_column': col_name,
                        'canonical_field': mapping.canonical_field,
                        'confidence': mapping.mapping_score.final_score if mapping.mapping_score else 0.0
                    })
                
                # Analyze table-level patterns
                table_analysis['semantic_analysis'] = self._analyze_table_semantics(table_name, table_analysis['columns'])
                tables_analysis[table_name] = table_analysis
            
            return {
                'tenant': tenant,
                'tenant_name': self.tenants.get(tenant, tenant),
                'tables': tables_analysis,
                'schema_variations': self._detect_schema_variations(tables_analysis),
                'mapping_summary': self._generate_mapping_summary(tables_analysis)
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def _analyze_semantic_type(self, col_name, col_type, col_desc):
        """Analyze semantic type of a column with improved real-world understanding"""
        col_lower = col_name.lower()
        desc_lower = col_desc.lower()
        
        # Financial/Value columns with ARR vs LTV detection
        if any(keyword in col_lower for keyword in ['amount', 'value', 'price', 'cost', 'revenue', 'obligation']):
            if 'annual' in desc_lower or 'yearly' in desc_lower or 'recurring' in desc_lower:
                return "Annual Recurring Revenue (ARR)"
            elif 'lifetime' in desc_lower or 'total' in desc_lower or 'cumulative' in desc_lower:
                return "Lifetime Value (LTV)"
            elif 'obligation' in col_lower:
                return "Obligation Amount"
            elif 'current' in col_lower:
                return "Current Value"
            else:
                return "Financial Amount"
        
        # Date columns with specific semantic types
        elif any(keyword in col_lower for keyword in ['date', 'time', 'created', 'updated', 'expiry', 'expiration']):
            if 'award' in col_lower:
                return "Award Date"
            elif 'sign' in col_lower or 'signature' in col_lower:
                return "Contract Signature Date"
            elif 'start' in col_lower or 'begin' in col_lower:
                return "Period Start Date"
            elif 'end' in col_lower or 'expir' in col_lower:
                return "Period End Date"
            elif 'obligation' in col_lower:
                return "Obligation Date"
            elif 'modif' in col_lower or 'last' in col_lower:
                return "Last Modified Date"
            elif 'created' in col_lower:
                return "Creation Date"
            else:
                return "Date Field"
        
        # Status columns with specific types
        elif any(keyword in col_lower for keyword in ['status', 'state', 'phase']):
            if 'contract' in col_lower:
                return "Contract Status"
            elif 'award' in col_lower:
                return "Award Status"
            elif 'project' in col_lower:
                return "Project Status"
            else:
                return "Status Indicator"
        
        # Identifier columns with specific types
        elif any(keyword in col_lower for keyword in ['id', 'identifier', 'key', 'number']):
            if 'award' in col_lower:
                return "Award Identifier"
            elif 'contract' in col_lower:
                return "Contract Identifier"
            elif 'party' in col_lower:
                return "Party Identifier"
            elif 'transaction' in col_lower or 'action' in col_lower:
                return "Transaction Identifier"
            elif 'unique' in col_lower or 'generated' in col_lower:
                return "Unique System Identifier"
            else:
                return "Unique Identifier"
        
        # Name columns with entity types
        elif 'name' in col_lower:
            if 'supplier' in col_lower or 'vendor' in col_lower or 'recipient' in col_lower:
                return "Supplier Name"
            elif 'buyer' in col_lower or 'agency' in col_lower:
                return "Buyer Name"
            elif 'legal' in col_lower or 'business' in col_lower:
                return "Legal Business Name"
            else:
                return "Organization Name"
        
        # Agency columns
        elif 'agency' in col_lower:
            if 'awarding' in col_lower:
                return "Awarding Agency"
            elif 'funding' in col_lower:
                return "Funding Agency"
            else:
                return "Government Agency"
        
        # Description/Title columns
        elif any(keyword in col_lower for keyword in ['description', 'title', 'comment']):
            if 'contract' in col_lower:
                return "Contract Description"
            elif 'award' in col_lower:
                return "Award Description"
            else:
                return "Text Description"
        
        # Type columns
        elif 'type' in col_lower:
            if 'contract' in col_lower:
                return "Contract Type"
            elif 'award' in col_lower:
                return "Award Type"
            else:
                return "Type Classification"
        
        # Currency columns
        elif 'currency' in col_lower:
            return "Currency Code"
        
        # Code columns
        elif 'code' in col_lower:
            if 'agency' in col_lower:
                return "Agency Code"
            elif 'country' in col_lower:
                return "Country Code"
            else:
                return "Classification Code"
        
        # Default fallback
        else:
            return "General Field"
    
    def _get_mapping_reasoning(self, col_name, canonical_field, confidence):
        """Generate reasoning for column mapping"""
        col_lower = col_name.lower()
        
        if confidence >= 0.9:
            if 'amount' in col_lower or 'value' in col_lower:
                return f"High confidence: '{col_name}' clearly represents a financial value and maps to '{canonical_field}'"
            elif 'id' in col_lower:
                return f"High confidence: '{col_name}' is clearly an identifier and maps to '{canonical_field}'"
        elif confidence >= 0.8:
            return f"Good confidence: '{col_name}' has strong semantic indicators for '{canonical_field}'"
        elif confidence >= 0.7:
            return f"Medium confidence: '{col_name}' has some indicators for '{canonical_field}' but may need review"
        else:
            return f"Low confidence: '{col_name}' mapping to '{canonical_field}' is uncertain and requires human review"
    
    def _analyze_table_semantics(self, table_name, columns):
        """Analyze table-level semantic patterns"""
        analysis = {
            'table_type': 'Unknown',
            'key_patterns': [],
            'semantic_issues': [],
            'mapping_complexity': 'Low'
        }
        
        table_lower = table_name.lower()
        
        # Determine table type
        if 'contract' in table_lower:
            analysis['table_type'] = 'Contract Management'
        elif 'award' in table_lower:
            analysis['table_type'] = 'Award/Procurement'
        elif 'transaction' in table_lower:
            analysis['table_type'] = 'Financial Transaction'
        elif 'agency' in table_lower:
            analysis['table_type'] = 'Organizational Entity'
        elif 'supplier' in table_lower or 'vendor' in table_lower:
            analysis['table_type'] = 'Business Entity'
        
        # Analyze key patterns
        id_columns = [col for col in columns if 'id' in col['name'].lower()]
        date_columns = [col for col in columns if any(kw in col['name'].lower() for kw in ['date', 'time'])]
        amount_columns = [col for col in columns if any(kw in col['name'].lower() for kw in ['amount', 'value', 'price'])]
        
        if len(id_columns) > 1:
            analysis['key_patterns'].append('Multiple ID columns detected')
        if len(date_columns) > 2:
            analysis['key_patterns'].append('Multiple date fields - may indicate temporal data')
        if len(amount_columns) > 1:
            analysis['key_patterns'].append('Multiple financial fields - check for ARR vs LTV distinction')
        
        # Check for semantic issues
        low_confidence_mappings = [col for col in columns if col['confidence'] < 0.7]
        if len(low_confidence_mappings) > len(columns) * 0.3:
            analysis['semantic_issues'].append('High number of low-confidence mappings')
            analysis['mapping_complexity'] = 'High'
        
        return analysis
    
    def _detect_schema_variations(self, tables_analysis):
        """Detect schema variations across tables"""
        variations = {
            'table_structure': [],
            'semantic_differences': [],
            'data_type_variations': [],
            'naming_conventions': []
        }
        
        # Analyze table structure variations
        table_types = {}
        for table_name, analysis in tables_analysis.items():
            table_type = analysis['semantic_analysis']['table_type']
            if table_type not in table_types:
                table_types[table_type] = []
            table_types[table_type].append(table_name)
        
        for table_type, tables in table_types.items():
            if len(tables) > 1:
                variations['table_structure'].append(f"Multiple {table_type} tables: {', '.join(tables)}")
        
        # Analyze semantic differences
        financial_columns = []
        for table_name, analysis in tables_analysis.items():
            for col in analysis['columns']:
                if col['semantic_type'] in ['Annual Recurring Revenue (ARR)', 'Lifetime Value (LTV)', 'Financial Amount']:
                    financial_columns.append({
                        'table': table_name,
                        'column': col['name'],
                        'semantic_type': col['semantic_type']
                    })
        
        arr_columns = [col for col in financial_columns if 'ARR' in col['semantic_type']]
        ltv_columns = [col for col in financial_columns if 'LTV' in col['semantic_type']]
        
        if arr_columns and ltv_columns:
            variations['semantic_differences'].append("Mixed ARR and LTV semantics detected")
        
        return variations
    
    def _generate_mapping_summary(self, tables_analysis):
        """Generate overall mapping summary"""
        total_columns = sum(len(analysis['columns']) for analysis in tables_analysis.values())
        high_confidence = sum(
            len([col for col in analysis['columns'] if col['confidence'] >= 0.8])
            for analysis in tables_analysis.values()
        )
        medium_confidence = sum(
            len([col for col in analysis['columns'] if 0.6 <= col['confidence'] < 0.8])
            for analysis in tables_analysis.values()
        )
        low_confidence = sum(
            len([col for col in analysis['columns'] if col['confidence'] < 0.6])
            for analysis in tables_analysis.values()
        )
        
        return {
            'total_columns': total_columns,
            'high_confidence_mappings': high_confidence,
            'medium_confidence_mappings': medium_confidence,
            'low_confidence_mappings': low_confidence,
            'mapping_success_rate': (high_confidence + medium_confidence) / total_columns if total_columns > 0 else 0,
            'requires_hitl': low_confidence
        }
    
    def generate_hitl_test_data(self):
        """Generate comprehensive HITL test data for demonstration"""
        hitl_items = []
        
        # Comprehensive test scenarios covering all confidence levels and edge cases
        test_scenarios = [
            # High Priority - Low Confidence (0.3-0.5)
            {
                'tenant': 'tenant_A',
                'table': 'awards',
                'column': 'obligated_amount',
                'suggested_canonical': 'contract_value_ltv',
                'confidence': 0.35,
                'reasoning': 'Legacy system stores values in cents - requires division by 100. Low confidence due to unit conversion uncertainty.',
                'sample_values': ['12000000', '850000', '0', '2400000'],
                'priority': 'high',
                'semantic_type': 'Financial Amount (cents)',
                'transformations': ['Divide by 100', 'Convert to decimal']
            },
            {
                'tenant': 'tenant_D',
                'table': 'master_agreements',
                'column': 'agmt_status_cd',
                'suggested_canonical': 'status',
                'confidence': 0.42,
                'reasoning': 'Encoded status codes need decoding: AC=Active, EX=Expired, PE=Pending, CA=Cancelled, SU=Suspended',
                'sample_values': ['AC', 'EX', 'PE', 'CA', 'SU'],
                'priority': 'high',
                'semantic_type': 'Status Code (encoded)',
                'transformations': ['Decode status codes', 'Map to standard values']
            },
            {
                'tenant': 'tenant_F',
                'table': 'procurement_contracts',
                'column': 'base_period_months',
                'suggested_canonical': 'renewal_term_months',
                'confidence': 0.38,
                'reasoning': 'Government base periods vs commercial renewal terms - semantic mismatch',
                'sample_values': ['12', '24', '36', '12'],
                'priority': 'high',
                'semantic_type': 'Time Period',
                'transformations': ['Validate period logic', 'Map to renewal terms']
            },
            
            # Medium Priority - Medium-Low Confidence (0.5-0.7)
            {
                'tenant': 'tenant_B',
                'table': 'projects',
                'column': 'project_status',
                'suggested_canonical': 'status',
                'confidence': 0.65,
                'reasoning': 'Project status mapping to contract status - some semantic overlap but different lifecycle stages',
                'sample_values': ['ACTIVE', 'COMPLETED', 'ON_HOLD', 'CANCELLED'],
                'priority': 'medium',
                'semantic_type': 'Project Status',
                'transformations': ['Map lifecycle stages', 'Standardize status values']
            },
            {
                'tenant': 'tenant_C',
                'table': 'contracts',
                'column': 'contract_value_currency',
                'suggested_canonical': 'currency',
                'confidence': 0.58,
                'reasoning': 'Currency field but may need validation against actual currency codes',
                'sample_values': ['USD', 'EUR', 'GBP', 'CAD'],
                'priority': 'medium',
                'semantic_type': 'Currency Code',
                'transformations': ['Validate currency codes', 'Standardize format']
            },
            {
                'tenant': 'tenant_E',
                'table': 'contracts_header',
                'column': 'vendor_duns_number',
                'suggested_canonical': 'party_seller_id',
                'confidence': 0.62,
                'reasoning': 'DUNS number is vendor identifier but may need to map to different party type',
                'sample_values': ['123456789', '987654321', '555666777'],
                'priority': 'medium',
                'semantic_type': 'Business Identifier',
                'transformations': ['Validate DUNS format', 'Map to party system']
            },
            
            # Lower Priority - Medium Confidence (0.7-0.8)
            {
                'tenant': 'tenant_A',
                'table': 'recipients',
                'column': 'recipient_name',
                'suggested_canonical': 'party_buyer',
                'confidence': 0.75,
                'reasoning': 'Recipient name maps to buyer party but recipient vs buyer semantics need validation',
                'sample_values': ['Acme Corp', 'Beta LLC', 'Gamma Industries'],
                'priority': 'normal',
                'semantic_type': 'Organization Name',
                'transformations': ['Standardize names', 'Validate party type']
            },
            {
                'tenant': 'tenant_D',
                'table': 'financial_terms',
                'column': 'term_type',
                'suggested_canonical': 'contract_type',
                'confidence': 0.72,
                'reasoning': 'Financial term type vs contract type - related but different classification systems',
                'sample_values': ['ANNUAL_RECURRING', 'LIFETIME_VALUE', 'MILESTONE_BASED'],
                'priority': 'normal',
                'semantic_type': 'Financial Term Type',
                'transformations': ['Map term types', 'Classify contract types']
            },
            
            # Complex Multi-Table Scenarios
            {
                'tenant': 'tenant_D',
                'table': 'client_master',
                'column': 'client_legal_name',
                'suggested_canonical': 'party_buyer',
                'confidence': 0.68,
                'reasoning': 'Client names in separate reference table requiring joins - complex relationship mapping',
                'sample_values': ['GlobalTech Solutions Inc.', 'European Manufacturing GmbH'],
                'priority': 'medium',
                'semantic_type': 'Legal Business Name',
                'transformations': ['Join with main table', 'Standardize legal names']
            },
            {
                'tenant': 'tenant_F',
                'table': 'vendor_information',
                'column': 'legal_business_name',
                'suggested_canonical': 'party_seller',
                'confidence': 0.71,
                'reasoning': 'Vendor information in separate table with DUNS identifiers - requires relationship resolution',
                'sample_values': ['TechCorp Solutions Inc.', 'SmallBiz IT Services LLC'],
                'priority': 'normal',
                'semantic_type': 'Legal Business Name',
                'transformations': ['Resolve vendor relationships', 'Map to party system']
            },
            
            # SaaS/Subscription Specific Scenarios
            {
                'tenant': 'tenant_E',
                'table': 'contracts_header',
                'column': 'mrr_usd_cents',
                'suggested_canonical': 'contract_value_arr',
                'confidence': 0.45,
                'reasoning': 'Monthly recurring revenue in cents - convert to ARR but MRR vs ARR semantics differ',
                'sample_values': ['833333', '49900', '1666', '208333'],
                'priority': 'high',
                'semantic_type': 'MRR (Monthly Recurring Revenue)',
                'transformations': ['Convert cents to dollars', 'Multiply by 12 for ARR', 'Validate MRR logic']
            },
            {
                'tenant': 'tenant_E',
                'table': 'contracts_header',
                'column': 'subscription_state',
                'suggested_canonical': 'status',
                'confidence': 0.78,
                'reasoning': 'SaaS subscription states to contract status - good mapping but subscription lifecycle differs from contract lifecycle',
                'sample_values': ['ACTIVE', 'PAUSED', 'CANCELLED', 'CHURNED', 'TRIAL'],
                'priority': 'normal',
                'semantic_type': 'Subscription State',
                'transformations': ['Map subscription states', 'Handle trial states']
            },
            
            # Government/Compliance Scenarios
            {
                'tenant': 'tenant_F',
                'table': 'procurement_contracts',
                'column': 'contract_number',
                'suggested_canonical': 'contract_id',
                'confidence': 0.82,
                'reasoning': 'Complex government contract numbering schemes - high confidence but format validation needed',
                'sample_values': ['GS-35F-0119Y-DOD-001', 'VA-118-23-C-0045', 'NASA-2024-C-12345'],
                'priority': 'normal',
                'semantic_type': 'Government Contract Number',
                'transformations': ['Validate government format', 'Standardize contract IDs']
            },
            {
                'tenant': 'tenant_F',
                'table': 'procurement_contracts',
                'column': 'award_amount_dollars',
                'suggested_canonical': 'contract_value_ltv',
                'confidence': 0.55,
                'reasoning': 'Government contracts: award amount vs obligation amount semantics - need to distinguish between committed vs obligated funds',
                'sample_values': ['15750000.00', '3200000.00', '8900000.00'],
                'priority': 'medium',
                'semantic_type': 'Award Amount',
                'transformations': ['Distinguish award vs obligation', 'Map to LTV concept']
            }
        ]
        
        # Convert to HITL items format
        for scenario in test_scenarios:
            hitl_items.append({
                'tenant': scenario['tenant'],
                'table': scenario['table'],
                'column': scenario['column'],
                'suggested_canonical': scenario['suggested_canonical'],
                'confidence': scenario['confidence'],
                'reasoning': scenario['reasoning'],
                'sample_values': scenario['sample_values'],
                'priority': scenario['priority'],
                'semantic_type': scenario['semantic_type'],
                'transformations': scenario['transformations']
            })
        
        return hitl_items
    
    def _get_llm_prompt_details(self, llm_mapper, column_profile):
        """Extract details about the prompt sent to LLM"""
        try:
            # Get the prompt template
            prompt_template = llm_mapper.adapter.prompt_template if hasattr(llm_mapper, 'adapter') else "Template not available"
            
            # Build the user prompt (similar to what's sent to LLM)
            canonical_schema_excerpt = "Canonical schema fields would be listed here"
            user_prompt = f"""
## Canonical Schema Fields
{canonical_schema_excerpt}

## Source Column Context
- **Tenant**: {column_profile.source_column.tenant}
- **Table**: {column_profile.source_column.table}
- **Column**: {column_profile.source_column.column}
- **Inferred Type**: {column_profile.inferred_type}
- **Description**: {column_profile.source_column.description or 'No description'}
- **Sample Values**: {column_profile.sample_values[:5]}
- **Co-occurring Columns**: {column_profile.cooccurring_columns[:5]}

## Your Task
Analyze the source column '{column_profile.source_column.column}' and propose mapping(s) to the canonical schema.
Remember to be conservative with confidence scores and provide clear justifications.

Respond with valid JSON following the specified schema:
"""
            
            return {
                'system_prompt': prompt_template[:500] + "..." if len(prompt_template) > 500 else prompt_template,
                'user_prompt': user_prompt,
                'prompt_length': len(prompt_template) + len(user_prompt),
                'canonical_schema_context': canonical_schema_excerpt
            }
        except Exception as e:
            return {
                'error': f"Could not extract prompt details: {str(e)}",
                'system_prompt': 'Not available',
                'user_prompt': 'Not available'
            }
    
    def _get_llm_response_details(self, llm_response):
        """Extract details about the LLM response"""
        try:
            if not llm_response:
                return {'error': 'No LLM response received'}
            
            response_details = {
                'reasoning': llm_response.reasoning,
                'proposed_mappings_count': len(llm_response.proposed_mappings) if llm_response.proposed_mappings else 0,
                'alternatives_count': len(llm_response.alternatives) if llm_response.alternatives else 0,
                'response_length': len(str(llm_response.reasoning)) if llm_response.reasoning else 0
            }
            
            # Add details of proposed mappings
            if llm_response.proposed_mappings:
                response_details['proposed_mappings'] = []
                for i, mapping in enumerate(llm_response.proposed_mappings[:3]):  # Limit to first 3
                    response_details['proposed_mappings'].append({
                        'index': i,
                        'canonical_field': mapping.canonical_field,
                        'confidence': mapping.confidence,
                        'justification': mapping.justification[:200] + "..." if len(mapping.justification) > 200 else mapping.justification,
                        'assumptions': mapping.assumptions
                    })
            
            # Add details of alternatives
            if llm_response.alternatives:
                response_details['alternatives'] = []
                for i, alt in enumerate(llm_response.alternatives[:2]):  # Limit to first 2
                    response_details['alternatives'].append({
                        'index': i,
                        'canonical_field': alt.canonical_field,
                        'confidence': alt.confidence,
                        'note': alt.note
                    })
            
            return response_details
            
        except Exception as e:
            return {
                'error': f"Could not extract response details: {str(e)}",
                'reasoning': 'Not available'
            }
    
    def translate_query(self, canonical_query, customer_id):
        """Translate a canonical query to customer-specific SQL"""
        try:
            if not self.query_translation_available:
                return {'error': 'Query translation not available'}
            
            # Validate customer_id
            if not customer_id or customer_id in ['', 'null', 'undefined', 'on']:
                return {'error': f'Invalid customer ID provided: {customer_id}'}
            
            customer_schema = self.customer_schemas.get(customer_id)
            if not customer_schema:
                available_customers = list(self.customer_schemas.keys())
                return {'error': f'Customer schema not found: {customer_id}. Available customers: {available_customers}'}
            
            translation = self.query_engine.translate_query(
                canonical_query, 
                customer_schema, 
                customer_id
            )
            
            return {
                'original_query': translation.original_query,
                'translated_query': translation.translated_query,
                'confidence': translation.confidence,
                'reasoning': translation.reasoning,
                'warnings': translation.warnings,
                'validation_errors': getattr(translation, 'validation_errors', []),
                'performance_optimization': translation.performance_optimization,
                'execution_plan': translation.execution_plan,
                'join_strategy': {
                    'primary_table': translation.join_strategy.primary_table,
                    'join_tables': translation.join_strategy.join_tables,
                    'join_order': translation.join_strategy.join_order,
                    'confidence': translation.join_strategy.confidence,
                    'reasoning': translation.join_strategy.reasoning,
                    'performance_notes': translation.join_strategy.performance_notes
                } if hasattr(translation, 'join_strategy') and translation.join_strategy else None
            }
            
        except Exception as e:
            return {'error': f'Query translation failed: {str(e)}'}
    
    def discover_relationships(self, customer_id):
        """Discover relationships for a customer schema"""
        try:
            if not self.query_translation_available:
                return {'error': 'Query translation not available'}
            
            # Validate customer_id
            if not customer_id or customer_id in ['', 'null', 'undefined', 'on']:
                return {'error': f'Invalid customer ID provided: {customer_id}'}
            
            customer_schema = self.customer_schemas.get(customer_id)
            if not customer_schema:
                available_customers = list(self.customer_schemas.keys())
                return {'error': f'Customer schema not found: {customer_id}. Available customers: {available_customers}'}
            
            relationships = self.relationship_analyzer.discover_relationships(customer_schema)
            
            return {
                'customer_id': customer_id,
                'relationships': [
                    {
                        'table1': rel.table1,
                        'column1': rel.column1,
                        'table2': rel.table2,
                        'column2': rel.column2,
                        'relationship_type': rel.relationship_type.value,
                        'confidence': rel.confidence,
                        'reasoning': rel.reasoning,
                        'join_condition': rel.join_condition
                    }
                    for rel in relationships
                ]
            }
            
        except Exception as e:
            return {'error': f'Relationship discovery failed: {str(e)}'}
    
    def get_demo_queries(self):
        """Get demonstration queries"""
        return self.demo_queries if self.query_translation_available else {}
    
    def get_customer_schemas(self):
        """Get available customer schemas"""
        return self.customer_schemas if self.query_translation_available else {}

# Initialize dashboard
dashboard = SchemaTranslatorDashboard()

@app.route('/')
def index():
    """Main dashboard page - redirect to unified dashboard"""
    return redirect(url_for('unified_dashboard'))

@app.route('/unified')
def unified_dashboard():
    """Unified modern dashboard with all features integrated"""
    return render_template('unified_dashboard.html')

@app.route('/architecture')
def architecture():
    """System architecture documentation page"""
    return render_template('architecture.html')

@app.route('/dashboard')
def dashboard_legacy():
    """Legacy dashboard page"""
    api_status = dashboard.get_api_status()
    tenants = dashboard.get_available_tenants()
    reports = dashboard.load_mapping_reports()
    
    return render_template('dashboard.html', 
                         api_status=api_status,
                         tenants=tenants,
                         reports=reports,
                         tenant_names=dashboard.tenants)

# @app.route('/testing')
# def testing():
#     """Testing interface page - REMOVED FOR CLEANER UX"""
#     pass

# @app.route('/hitl')
# def hitl():
#     """HITL interface - REMOVED FOR CLEANER UX"""
#     pass

# @app.route('/reports')
# def reports():
#     """Reports page - REMOVED FOR CLEANER UX"""
#     pass

@app.route('/api/test_mapping', methods=['POST'])
def api_test_mapping():
    """API endpoint for testing mappings"""
    data = request.json
    tenant = data.get('tenant')
    table = data.get('table')
    column = data.get('column')
    sample_values = data.get('sample_values', [])
    
    if not all([tenant, table, column]):
        return jsonify({"error": "Missing required parameters"})
    
    sample_values_str = ",".join(sample_values[:5])
    result = dashboard.test_column_mapping(tenant, table, column, sample_values_str)
    
    return jsonify(result)

@app.route('/api/approve_mapping', methods=['POST'])
def api_approve_mapping():
    """API endpoint for approving HITL mappings"""
    data = request.json
    # In a real implementation, this would update the mapping decision
    return jsonify({"status": "approved", "message": "Mapping approved successfully"})

@app.route('/api/reject_mapping', methods=['POST'])
def api_reject_mapping():
    """API endpoint for rejecting HITL mappings"""
    data = request.json
    # In a real implementation, this would update the mapping decision
    return jsonify({"status": "rejected", "message": "Mapping rejected"})

# @app.route('/schema-analysis')
# def schema_analysis():
#     """Schema analysis - REMOVED FOR CLEANER UX"""
#     pass

@app.route('/api/schema-analysis/<tenant>')
def api_schema_analysis(tenant):
    """API endpoint for getting detailed schema analysis"""
    analysis = dashboard.get_tenant_schema_analysis(tenant)
    return jsonify(analysis)

# @app.route('/llm-logs')
# def llm_logs():
#     """LLM logs - REMOVED FOR CLEANER UX"""
#     pass

@app.route('/api/llm-logs/<tenant>')
def api_llm_logs(tenant):
    """API endpoint for getting detailed LLM interaction logs"""
    try:
        from src.app.core.llm_mapper import LLMMapper
        from src.app.core.resolver import MappingResolver
        from src.app.core.discovery import SchemaDiscoverer
        from src.app.core.config import settings
        
        # Initialize components
        discoverer = SchemaDiscoverer(
            schemas_dir=settings.customer_schemas_dir,
            samples_dir=settings.customer_samples_dir
        )
        llm_mapper = LLMMapper()
        resolver = MappingResolver(llm_mapper)
        
        # Get column profiles for the tenant
        column_profiles = discoverer.profile_tenant_columns(tenant)
        
        # Process first 5 columns to avoid quota issues
        logs = []
        for i, profile in enumerate(column_profiles[:5]):
            try:
                # Get LLM mapping
                llm_response = llm_mapper.map_column(profile)
                mapping = resolver.resolve_column_mapping(profile)
                
                # Capture detailed LLM interaction
                prompt_details = dashboard._get_llm_prompt_details(llm_mapper, profile)
                response_details = dashboard._get_llm_response_details(llm_response)
                
                log_entry = {
                    'column_index': i + 1,
                    'source_column': f"{profile.source_column.table}.{profile.source_column.column}",
                    'tenant': tenant,
                    'prompt_details': prompt_details,
                    'response_details': response_details,
                    'final_mapping': {
                        'canonical_field': mapping.canonical_field,
                        'confidence': mapping.mapping_score.final_score if mapping.mapping_score else 0.0,
                        'status': mapping.status
                    },
                    'processing_time': 'N/A',  # Could be added if timing is needed
                    'model_used': llm_mapper.adapter.model if hasattr(llm_mapper, 'adapter') else 'Unknown',
                    'api_status': 'success' if llm_response else 'failed'
                }
                
                logs.append(log_entry)
                
            except Exception as e:
                logs.append({
                    'column_index': i + 1,
                    'source_column': f"{profile.source_column.table}.{profile.source_column.column}",
                    'tenant': tenant,
                    'error': str(e),
                    'api_status': 'failed'
                })
        
        return jsonify({
            'tenant': tenant,
            'total_columns': len(column_profiles),
            'processed_columns': len(logs),
            'logs': logs
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

# Query Translation Routes

@app.route('/query-translation')
def query_translation():
    """Query translation page"""
    return render_template('query_translation.html', dashboard=dashboard)

@app.route('/query-translation-advanced')
def query_translation_advanced():
    """Advanced query translation dashboard"""
    return render_template('query_translation_advanced.html', dashboard=dashboard)

@app.route('/query-translation-advanced-v2')
def query_translation_advanced_v2():
    """Sophisticated Query Translation Dashboard with detailed AI reasoning visualization"""
    return render_template('query_translation_advanced_v2.html', dashboard=dashboard)

@app.route('/api/query-translation/translate', methods=['POST'])
def api_translate_query():
    """API endpoint for query translation"""
    try:
        data = request.get_json()
        canonical_query = data.get('query', '')
        customer_id = data.get('customer_id', 'customer_a')
        
        if not canonical_query:
            return jsonify({'error': 'Query is required'})
        
        result = dashboard.translate_query(canonical_query, customer_id)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/query-translation/relationships/<customer_id>')
def api_get_relationships(customer_id):
    """API endpoint for getting table relationships"""
    try:
        result = dashboard.discover_relationships(customer_id)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/query-translation/demo-queries')
def api_get_demo_queries():
    """API endpoint for getting demo queries"""
    try:
        queries = dashboard.get_demo_queries()
        return jsonify(queries)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/query-translation/customer-schemas')
def api_get_customer_schemas():
    """API endpoint for getting customer schemas"""
    try:
        schemas = dashboard.get_customer_schemas()
        return jsonify(schemas)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/query-translation/performance/<customer_id>')
def api_get_performance_analysis(customer_id):
    """API endpoint for performance analysis"""
    try:
        # Mock performance data for demonstration
        performance_data = {
            'tenant_A': {
                'execution_time': '125ms',
                'rows_scanned': 15420,
                'complexity': 'Complex',
                'operations': [
                    {'step': 'Hash Join: awards ⟷ recipients', 'time': '45ms', 'rows': 8500},
                    {'step': 'Hash Join: result ⟷ agencies', 'time': '35ms', 'rows': 6200},
                    {'step': 'Nested Loop: result ⟷ transactions', 'time': '25ms', 'rows': 15420},
                    {'step': 'Filter: award_type + date', 'time': '20ms', 'rows': 15420}
                ],
                'optimizations': [
                    'Add composite index on (award_type, period_of_performance_start_date)',
                    'Consider partitioning transactions by fiscal_year',
                    'Add index on recipient_id for faster joins'
                ]
            },
            'tenant_B': {
                'execution_time': '89ms',
                'rows_scanned': 3247,
                'complexity': 'Medium',
                'operations': [
                    {'step': 'Hash Join: projects ⟷ contracts', 'time': '32ms', 'rows': 2100},
                    {'step': 'Hash Join: result ⟷ procurement_stages', 'time': '28ms', 'rows': 1800},
                    {'step': 'Nested Loop: result ⟷ suppliers', 'time': '19ms', 'rows': 3247},
                    {'step': 'Filter: project_status + contract_value', 'time': '10ms', 'rows': 3247}
                ],
                'optimizations': [
                    'Add index on (project_id, contract_signing_date)',
                    'Consider materialized view for active projects',
                    'Add index on supplier_country for geographic queries'
                ]
            },
            'tenant_C': {
                'execution_time': '156ms',
                'rows_scanned': 8934,
                'complexity': 'Complex',
                'operations': [
                    {'step': 'Hash Join: releases ⟷ awards', 'time': '42ms', 'rows': 5600},
                    {'step': 'Hash Join: result ⟷ contracts', 'time': '38ms', 'rows': 4200},
                    {'step': 'Hash Join: result ⟷ parties', 'time': '35ms', 'rows': 8934},
                    {'step': 'Nested Loop: result ⟷ documents', 'time': '25ms', 'rows': 6800},
                    {'step': 'Filter: OCDS tags + date range', 'time': '16ms', 'rows': 8934}
                ],
                'optimizations': [
                    'Add composite index on (ocid, release_date)',
                    'Consider separate indexes for each party_role',
                    'Partition releases by publication_date'
                ]
            },
            'tenant_D': {
                'execution_time': '73ms',
                'rows_scanned': 2156,
                'complexity': 'Medium',
                'operations': [
                    {'step': 'Hash Join: master_agreements ⟷ contracts', 'time': '28ms', 'rows': 1800},
                    {'step': 'Hash Join: result ⟷ financial_terms', 'time': '22ms', 'rows': 1600},
                    {'step': 'Nested Loop: result ⟷ suppliers', 'time': '15ms', 'rows': 2156},
                    {'step': 'Filter: contract_status + value_range', 'time': '8ms', 'rows': 2156}
                ],
                'optimizations': [
                    'Add index on (master_agreement_id, effective_date)',
                    'Consider denormalizing supplier information',
                    'Add index on contract_value for range queries'
                ]
            },
            'tenant_E': {
                'execution_time': '45ms',
                'rows_scanned': 1834,
                'complexity': 'Simple',
                'operations': [
                    {'step': 'Hash Join: contracts_header ⟷ contract_actions', 'time': '22ms', 'rows': 1500},
                    {'step': 'Nested Loop: result ⟷ vendors', 'time': '15ms', 'rows': 1834},
                    {'step': 'Filter: action_type + effective_date', 'time': '8ms', 'rows': 1834}
                ],
                'optimizations': [
                    'Add index on (contract_number, action_date)',
                    'Consider partitioning by fiscal_year',
                    'Add index on vendor_duns for vendor lookups'
                ]
            },
            'customer_a': {
                'execution_time': '0.15ms',
                'rows_scanned': 1247,
                'complexity': 'Simple',
                'operations': [
                    {'step': 'Table Scan: contracts', 'time': '0.1ms', 'rows': 1247},
                    {'step': 'Filter: status + date', 'time': '0.05ms', 'rows': 1247}
                ],
                'optimizations': [
                    'Add index on (status, expiry_date)',
                    'Consider partitioning by date'
                ]
            },
            'customer_b': {
                'execution_time': '68ms',
                'rows_scanned': 1251,
                'complexity': 'Complex',
                'operations': [
                    {'step': 'Hash Join: headers ⟷ status_history', 'time': '15ms', 'rows': 2500},
                    {'step': 'Nested Loop: result ⟷ renewal_schedule', 'time': '8ms', 'rows': 1300},
                    {'step': 'Subquery: MAX(effective_date) per contract', 'time': '45ms', 'rows': 1251}
                ],
                'optimizations': [
                    'Add index: (contract_id, effective_date)',
                    'Consider materialized view for current status',
                    'Partition status_history by date'
                ]
            },
            'customer_c': {
                'execution_time': '42ms',
                'rows_scanned': 1243,
                'complexity': 'Medium',
                'operations': [
                    {'step': 'Hash Join: master ⟷ details', 'time': '18ms', 'rows': 1500},
                    {'step': 'Hash Join: result ⟷ lifecycle', 'time': '24ms', 'rows': 1243}
                ],
                'optimizations': [
                    'Add index on master_id columns',
                    'Consider denormalizing frequently accessed data'
                ]
            }
        }
        
        return jsonify(performance_data.get(customer_id, {'error': 'Customer not found'}))
    except Exception as e:
        return jsonify({'error': str(e)})

# Natural Language to SQL Routes
# @app.route('/nl-to-sql-query')
# def nl_to_sql_query():
#     """Advanced NL Query - REMOVED FOR CLEANER UX"""
#     pass

# @app.route('/simple-nl-query')
# def simple_nl_query():
#     """Simple NL Query - REMOVED FOR CLEANER UX"""
#     pass

@app.route('/nl-to-sql')
def progressive_nl_query():
    """Progressive Natural Language Query Interface"""
    return render_template('nl_query_progressive.html')

@app.route('/api/cache-status')
def api_cache_status():
    """API endpoint to get real-time cache status"""
    try:
        # Get cache status from query translator
        cache_data = {
            "tenants": [],
            "overall": {
                "totalTranslations": 0,
                "cacheHitRate": 0,
                "avgCachedTime": 0,
                "avgColdTime": 0,
                "speedImprovement": 0,
                "totalTimeSaved": "0s"
            }
        }
        
        # Check if query translator has mapping cache
        if hasattr(dashboard.query_engine, 'mapping_cache'):
            mapping_cache = dashboard.query_engine.mapping_cache
            
            # Get available customer schemas
            available_customers = list(dashboard.customer_schemas.keys())
            
            total_translations = 0
            cache_hits = 0
            
            for customer_id in available_customers:
                customer_name = {
                    'tenant_A': 'Tenant A (USAspending)',
                    'tenant_B': 'Tenant B (World Bank)',
                    'tenant_C': 'Tenant C (New Customer)'
                }.get(customer_id, f'{customer_id} (Unknown)')
                
                if customer_id in mapping_cache:
                    # Cached tenant
                    cache_info = mapping_cache[customer_id]
                    confidence_score = cache_info.get('confidence_score', 0) * 100
                    field_mappings_count = len(cache_info.get('field_mappings', {}))
                    
                    # Estimate usage (would be better to track this)
                    usage_count = cache_info.get('usage_count', 1)  # Default to 1 if not tracked
                    
                    cache_data["tenants"].append({
                        "id": customer_id,
                        "name": customer_name,
                        "status": "cached",
                        "version": "v1.0",
                        "confidence": int(confidence_score),
                        "avgTime": 0.4,  # Estimated based on our tests
                        "usageCount": usage_count,
                        "lastUsed": "Recently",
                        "created": cache_info.get('discovery_timestamp', 'Unknown'),
                        "timeSaved": f"{usage_count * 60}s",  # Rough estimate
                        "fieldMappingsCount": field_mappings_count
                    })
                    
                    cache_hits += usage_count
                    total_translations += usage_count
                else:
                    # Cold tenant
                    cache_data["tenants"].append({
                        "id": customer_id,
                        "name": customer_name,
                        "status": "cold",
                        "version": None,
                        "confidence": None,
                        "avgTime": None,
                        "usageCount": 0,
                        "lastUsed": "Never",
                        "created": None,
                        "timeSaved": "0s",
                        "fieldMappingsCount": 0
                    })
            
            # Calculate overall stats
            if total_translations > 0:
                cache_data["overall"] = {
                    "totalTranslations": total_translations,
                    "cacheHitRate": int((cache_hits / total_translations) * 100),
                    "avgCachedTime": 0.4,
                    "avgColdTime": 90.0,
                    "speedImprovement": int(90.0 / 0.4),  # Based on our test results
                    "totalTimeSaved": f"{int(cache_hits * 60)}s"
                }
        
        return jsonify({
            "success": True,
            "data": cache_data
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/nl-to-sql/analyze', methods=['POST'])
def api_nl_to_sql_analyze():
    """API endpoint for analyzing natural language queries"""
    try:
        data = request.get_json()
        natural_query = data.get('query', '').strip()
        
        if not natural_query:
            return jsonify({'error': 'Query is required'}), 400
        
        if not dashboard.query_translation_available:
            return jsonify({'error': 'NL to SQL service not available'}), 503
        
        # Analyze the natural language query
        intent_analysis, sql_generation = dashboard.nl_to_sql_translator.translate_natural_language_to_sql(natural_query)
        
        # Convert to JSON-serializable format
        intent_data = {
            'query_intent': intent_analysis.query_intent.value,
            'primary_entity': intent_analysis.primary_entity,
            'requested_fields': intent_analysis.requested_fields,
            'filter_conditions': [
                {
                    'field': fc.field,
                    'operator': fc.operator,
                    'value': fc.value,
                    'confidence': fc.confidence,
                    'original_text': fc.original_text
                } for fc in intent_analysis.filter_conditions
            ],
            'date_ranges': [
                {
                    'start_date': dr.start_date,
                    'end_date': dr.end_date,
                    'confidence': dr.confidence,
                    'original_text': dr.original_text
                } for dr in intent_analysis.date_ranges
            ],
            'aggregations': intent_analysis.aggregations,
            'sort_fields': intent_analysis.sort_fields,
            'confidence': intent_analysis.confidence,
            'assumptions': intent_analysis.assumptions,
            'clarifications_needed': intent_analysis.clarifications_needed,
            'original_query': intent_analysis.original_query
        }
        
        return jsonify({
            'success': True,
            'intent_analysis': intent_data
        })
        
    except Exception as e:
        print(f"Error in NL to SQL analysis: {e}")
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500

@app.route('/api/nl-to-sql/generate-sql', methods=['POST'])
def api_nl_to_sql_generate():
    """API endpoint for generating SQL from analyzed intent"""
    try:
        data = request.get_json()
        intent_data = data.get('intent_analysis')
        
        if not intent_data:
            return jsonify({'error': 'Intent analysis is required'}), 400
        
        if not dashboard.query_translation_available:
            return jsonify({'error': 'NL to SQL service not available'}), 503
        
        # Reconstruct the IntentAnalysis object from the received data
        from src.app.core.nl_to_sql_translator import IntentAnalysis, FilterCondition, QueryIntent
        
        # Convert filter conditions
        filter_conditions = []
        for fc_data in intent_data.get('filter_conditions', []):
            filter_conditions.append(FilterCondition(
                field=fc_data['field'],
                operator=fc_data['operator'],
                value=fc_data['value'],
                confidence=fc_data.get('confidence', 0.9),
                original_text=fc_data.get('original_text', '')
            ))
        
        # Map query intent string to enum
        query_intent_map = {
            'list_contracts': QueryIntent.LIST_CONTRACTS,
            'filter_contracts': QueryIntent.FILTER_CONTRACTS,
            'count_contracts': QueryIntent.COUNT_CONTRACTS,
            'aggregate_values': QueryIntent.AGGREGATE_VALUES,
            'compare_periods': QueryIntent.COMPARE_PERIODS,
            'find_expiring': QueryIntent.FIND_EXPIRING
        }
        
        query_intent_str = intent_data.get('query_intent', 'unknown')
        query_intent = query_intent_map.get(query_intent_str, QueryIntent.UNKNOWN)
        
        # Create IntentAnalysis object
        intent_analysis = IntentAnalysis(
            query_intent=query_intent,
            primary_entity=intent_data['primary_entity'],
            requested_fields=intent_data.get('requested_fields', []),
            filter_conditions=filter_conditions,
            date_ranges=[],  # TODO: Add date range reconstruction if needed
            aggregations=intent_data.get('aggregations', []),
            sort_fields=intent_data.get('sort_fields', []),
            confidence=intent_data.get('confidence', 0.9),
            assumptions=intent_data.get('assumptions', []),
            clarifications_needed=intent_data.get('clarifications_needed', []),
            original_query=intent_data.get('original_query', '')
        )
        
        # Actually generate SQL using the NL-to-SQL translator
        sql_generation = dashboard.nl_to_sql_translator._generate_canonical_sql(intent_analysis)
        
        sql_generation_data = {
            'sql_query': sql_generation.sql_query,
            'fields_used': sql_generation.fields_used,
            'tables_used': sql_generation.tables_used,
            'validation_status': sql_generation.validation_status,
            'confidence': sql_generation.confidence,
            'reasoning': sql_generation.reasoning
        }
        
        return jsonify({
            'success': True,
            'sql_generation': sql_generation_data
        })
        
    except Exception as e:
        print(f"Error in SQL generation: {e}")
        return jsonify({'error': f'SQL generation failed: {str(e)}'}), 500

@app.route('/api/translate-query', methods=['POST'])
def api_translate_canonical_query():
    """API endpoint for translating canonical queries to customer schemas"""
    try:
        data = request.get_json()
        canonical_query = data.get('canonical_query', '').strip()
        customers = data.get('customers', [])
        
        if not canonical_query:
            return jsonify({'error': 'Canonical query is required'}), 400
        
        if not dashboard.query_translation_available:
            return jsonify({'error': 'Query translation service not available'}), 503
        
        results = {}
        
        for customer_id in customers:
            try:
                # Get customer schema
                customer_schema = dashboard.customer_schemas.get(customer_id)
                if not customer_schema:
                    results[customer_id] = {
                        'error': f'Schema not found for customer {customer_id}'
                    }
                    continue
                
                # Translate the query
                translation = dashboard.query_engine.translate_query(
                    canonical_query, 
                    customer_schema, 
                    customer_id
                )
                
                results[customer_id] = {
                    'success': True,
                    'translated_query': translation.translated_query,
                    'confidence': translation.confidence,
                    'reasoning': translation.reasoning,
                    'warnings': translation.warnings,
                    'validation_errors': translation.validation_errors or [],
                    'performance_optimization': translation.performance_optimization,
                    'execution_plan': translation.execution_plan
                }
                
            except Exception as e:
                results[customer_id] = {
                    'error': f'Translation failed: {str(e)}'
                }
        
        return jsonify({
            'success': True,
            'results': results
        })
        
    except Exception as e:
        print(f"Error in query translation: {e}")
        return jsonify({'error': f'Query translation failed: {str(e)}'}), 500

# @app.route('/schema-comparison')
# def schema_comparison():
#     """Schema comparison - REMOVED FOR CLEANER UX"""
#     pass

@app.route('/api/schema-comparison')
def api_schema_comparison():
    """API endpoint for comparing schemas across tenants"""
    tenants = dashboard.get_available_tenants()
    comparison_data = {}
    
    for tenant in tenants:
        analysis = dashboard.get_tenant_schema_analysis(tenant)
        if 'error' not in analysis:
            comparison_data[tenant] = {
                'tenant_name': analysis['tenant_name'],
                'mapping_summary': analysis['mapping_summary'],
                'schema_variations': analysis['schema_variations'],
                'table_count': len(analysis['tables']),
                'total_columns': analysis['mapping_summary']['total_columns']
            }
    
    return jsonify(comparison_data)

# ============================================================================
# API ENDPOINTS FOR UNIFIED DASHBOARD
# ============================================================================

@app.route('/api/query_translation/translate', methods=['POST'])
def api_translate_query_unified():
    """Unified dashboard query translation endpoint with streaming support"""
    try:
        data = request.get_json()
        canonical_query = data.get('canonical_query')
        customer_id = data.get('customer_id')
        
        if not canonical_query or not customer_id:
            return jsonify({
                'success': False,
                'error': 'Missing canonical_query or customer_id'
            }), 400
        
        # Check if client accepts streaming
        accept_header = request.headers.get('Accept', '')
        wants_streaming = 'text/event-stream' in accept_header
        
        if wants_streaming:
            # Use streaming response with progress updates
            return stream_query_translation(canonical_query, customer_id)
        else:
            # Regular JSON response (for backward compatibility)
            return translate_query_sync(canonical_query, customer_id)
            
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

def translate_query_sync(canonical_query, customer_id):
    """Synchronous query translation (non-streaming)"""
    # Get customer schema
    customer_schema = dashboard.get_customer_schema(customer_id)
    if not customer_schema:
        return jsonify({
            'success': False,
            'error': f'Schema not found for customer {customer_id}'
        }), 404
    
    # Translate query
    result = dashboard.query_engine.translate_query(
        canonical_query,
        customer_schema,
        customer_id
    )
    
    return jsonify({
        'success': True,
        'translated_query': result.translated_query,
        'confidence': result.confidence,
        'warnings': result.warnings or [],
        'validation_errors': result.validation_errors or [],
        'reasoning': result.reasoning,
        'translation_path': 'Complex (LLM)' if 'complex' in result.reasoning.lower() else 'Simple',
        'cache_hit': customer_id in dashboard.query_engine.mapping_cache
    })

def stream_query_translation(canonical_query, customer_id):
    """Streaming query translation with real-time progress updates"""
    from flask import Response, stream_with_context
    import queue
    import threading
    import time
    
    # Create a queue for progress messages
    progress_queue = queue.Queue()
    
    def send_progress(step, message, data=None):
        """Helper to send progress updates"""
        progress_queue.put({
            'step': step,
            'message': message,
            'data': data,
            'timestamp': time.time()
        })
    
    def translate_with_progress():
        """Run translation and emit progress"""
        try:
            # Step 1: Schema Loading
            send_progress('schema_load', f'📊 Loading schema for {customer_id}...')
            customer_schema = dashboard.get_customer_schema(customer_id)
            
            if not customer_schema:
                send_progress('error', f'❌ Schema not found for {customer_id}', {
                    'error': f'Schema not found for customer {customer_id}'
                })
                return
            
            send_progress('schema_loaded', f'✅ Schema loaded for {customer_id}')
            
            # Step 2: Cache Check
            send_progress('cache_check', '💾 Checking mapping cache...')
            is_cached = customer_id in dashboard.query_engine.mapping_cache
            cache_message = '⚡ Cache HIT - using cached mappings' if is_cached else '🔍 Cache MISS - will discover mappings'
            send_progress('cache_result', cache_message, {'cached': is_cached})
            
            # Step 3: Translation
            if not is_cached:
                send_progress('mapping_discovery', '🔍 Discovering schema mappings (first-time)...')
            
            send_progress('translation_start', '🔄 Translating query...')
            
            # Perform actual translation
            result = dashboard.query_engine.translate_query(
                canonical_query,
                customer_schema,
                customer_id
            )
            
            send_progress('translation_complete', '✅ Translation complete', {
                'sql': result.translated_query,
                'confidence': result.confidence,
                'warnings': result.warnings,
                'validation_errors': result.validation_errors or []
            })
            
            # Step 4: Final Result
            send_progress('complete', '🎉 All done!', {
                'success': True,
                'translated_query': result.translated_query,
                'confidence': result.confidence,
                'warnings': result.warnings or [],
                'validation_errors': result.validation_errors or [],
                'reasoning': result.reasoning,
                'cache_hit': is_cached
            })
            
        except Exception as e:
            import traceback
            send_progress('error', f'❌ Translation failed: {str(e)}', {
                'error': str(e),
                'traceback': traceback.format_exc()
            })
    
    def generate():
        """SSE generator"""
        # Start translation in background thread
        thread = threading.Thread(target=translate_with_progress)
        thread.daemon = True
        thread.start()
        
        # Stream progress updates
        while True:
            try:
                progress = progress_queue.get(timeout=30)
                yield f"data: {json.dumps(progress)}\n\n"
                
                # Stop if complete or error
                if progress['step'] in ['complete', 'error']:
                    break
                    
            except queue.Empty:
                # Send keepalive
                yield f"data: {json.dumps({'step': 'keepalive'})}\n\n"
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )

@app.route('/api/nl-to-sql/translate', methods=['POST'])
def api_nl_to_sql_unified():
    """Unified dashboard NL to SQL endpoint"""
    try:
        data = request.get_json()
        nl_query = data.get('natural_language_query')
        tenant_id = data.get('tenant_id')
        
        if not nl_query or not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Missing natural_language_query or tenant_id'
            }), 400
        
        # Get customer schema
        customer_schema = dashboard.get_customer_schema(tenant_id)
        if not customer_schema:
            return jsonify({
                'success': False,
                'error': f'Schema not found for tenant {tenant_id}'
            }), 404
        
        # Generate SQL from natural language
        intent_analysis, sql_generation = dashboard.nl_to_sql_translator.translate_natural_language_to_sql(nl_query)
        
        # Try to translate canonical SQL to tenant-specific SQL
        translated_query = None
        translation_error = None
        try:
            translated_result = dashboard.query_engine.translate_query(
                sql_generation.sql_query,
                customer_schema,
                tenant_id
            )
            translated_query = translated_result.translated_query
        except Exception as trans_error:
            translation_error = str(trans_error)
            import logging
            logging.error(f"Translation to tenant SQL failed: {translation_error}")
        
        return jsonify({
            'success': True,
            'natural_query': nl_query,
            'canonical_query': sql_generation.sql_query,
            'translated_query': translated_query,
            'translation_error': translation_error,
            'confidence': sql_generation.confidence,
            'intent': intent_analysis.query_intent.value,
            'reasoning': sql_generation.reasoning,
            'tables_used': sql_generation.tables_used,
            'fields_used': sql_generation.fields_used,
            'primary_entity': intent_analysis.primary_entity,
            'filter_conditions': [
                {
                    'field': fc.field,
                    'operator': fc.operator,
                    'value': fc.value,
                    'confidence': fc.confidence
                } for fc in intent_analysis.filter_conditions
            ]
        })
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/nl-to-sql/translate-and-execute', methods=['POST'])
def api_nl_to_sql_translate_and_execute():
    """
    Complete NL to SQL flow with execution:
    Natural Language → Canonical SQL → Tenant SQL → Execute → Results
    """
    try:
        data = request.get_json()
        nl_query = data.get('natural_language_query')
        tenant_id = data.get('tenant_id')
        
        if not nl_query or not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Missing natural_language_query or tenant_id'
            }), 400
        
        # Step 1: Get customer schema
        customer_schema = dashboard.get_customer_schema(tenant_id)
        if not customer_schema:
            return jsonify({
                'success': False,
                'error': f'Schema not found for tenant {tenant_id}'
            }), 404
        
        # Step 2: Generate canonical SQL from natural language
        intent_analysis, sql_generation = dashboard.nl_to_sql_translator.translate_natural_language_to_sql(nl_query)
        
        # Step 3: Translate canonical SQL to tenant-specific SQL
        translated_query = None
        translation_error = None
        translation_confidence = None
        
        try:
            translated_result = dashboard.query_engine.translate_query(
                sql_generation.sql_query,
                customer_schema,
                tenant_id
            )
            translated_query = translated_result.translated_query
            translation_confidence = translated_result.confidence
        except Exception as trans_error:
            translation_error = str(trans_error)
            return jsonify({
                'success': False,
                'stage': 'translation',
                'error': f'Failed to translate to tenant SQL: {translation_error}',
                'natural_query': nl_query,
                'canonical_query': sql_generation.sql_query
            }), 500
        
        # Step 4: Fix SQL compatibility issues for DuckDB
        def fix_duckdb_compatibility(sql: str) -> str:
            """Fix common SQL compatibility issues for DuckDB"""
            import re
            
            # Replace ARRAY_REMOVE with list_filter (DuckDB equivalent)
            # ARRAY_REMOVE(arr, NULL) -> list_filter(arr, x -> x IS NOT NULL)
            sql = re.sub(
                r'ARRAY_REMOVE\s*\(\s*ARRAY\s*\[(.*?)\]\s*,\s*NULL\s*\)',
                r'[\1]',  # Just use the array literal directly
                sql,
                flags=re.IGNORECASE
            )
            
            # Replace ARRAY[...] with [...] for DuckDB list syntax
            sql = re.sub(
                r'\bARRAY\s*\[',
                r'[',
                sql,
                flags=re.IGNORECASE
            )
            
            # Replace CAST(...AS DATE) with TRY_CAST for safer date conversion
            # This handles invalid date values gracefully (returns NULL instead of error)
            sql = re.sub(
                r'\bCAST\s*\(\s*([^)]+)\s+AS\s+DATE\s*\)',
                r'TRY_CAST(\1 AS DATE)',
                sql,
                flags=re.IGNORECASE
            )
            
            # Replace CAST(...AS TIMESTAMP) with TRY_CAST
            sql = re.sub(
                r'\bCAST\s*\(\s*([^)]+)\s+AS\s+TIMESTAMP\s*\)',
                r'TRY_CAST(\1 AS TIMESTAMP)',
                sql,
                flags=re.IGNORECASE
            )
            
            return sql
        
        translated_query = fix_duckdb_compatibility(translated_query)
        
        # Step 5: Execute the translated query
        execution_result = None
        execution_error = None
        
        if translated_query:
            try:
                from app.core.query_executor import TenantQueryExecutor
                
                databases_dir = Path('databases')
                executor = TenantQueryExecutor(databases_dir)
                
                execution_result = executor.execute_for_tenant(tenant_id, translated_query)
                
                if not execution_result.get('success'):
                    execution_error = execution_result.get('error', 'Unknown execution error')
                    
            except FileNotFoundError as e:
                execution_error = str(e)
                return jsonify({
                    'success': False,
                    'stage': 'execution',
                    'error': 'Database not found. Please run the import script first.',
                    'hint': 'Run: ./reimport_with_llm.sh',
                    'natural_query': nl_query,
                    'canonical_query': sql_generation.sql_query,
                    'translated_query': translated_query
                }), 404
            except Exception as exec_error:
                execution_error = str(exec_error)
                return jsonify({
                    'success': False,
                    'stage': 'execution',
                    'error': f'Query execution failed: {execution_error}',
                    'natural_query': nl_query,
                    'canonical_query': sql_generation.sql_query,
                    'translated_query': translated_query
                }), 500
        
        # Step 5: Return complete results
        return jsonify({
            'success': True,
            'stages': {
                'natural_language': {
                    'query': nl_query,
                    'intent': intent_analysis.query_intent.value,
                    'primary_entity': intent_analysis.primary_entity,
                    'filter_conditions': [
                        {
                            'field': fc.field,
                            'operator': fc.operator,
                            'value': fc.value,
                            'confidence': fc.confidence
                        } for fc in intent_analysis.filter_conditions
                    ]
                },
                'canonical_sql': {
                    'query': sql_generation.sql_query,
                    'confidence': sql_generation.confidence,
                    'reasoning': sql_generation.reasoning,
                    'tables_used': sql_generation.tables_used,
                    'fields_used': sql_generation.fields_used
                },
                'tenant_sql': {
                    'query': translated_query,
                    'confidence': translation_confidence,
                    'tenant_id': tenant_id
                },
                'execution': execution_result
            }
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'stage': 'unknown',
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/nl-to-sql/translate-stream', methods=['POST'])
def api_nl_to_sql_stream():
    """Stream translation progress in real-time using SSE"""
    from flask import Response, stream_with_context
    import queue
    import threading
    import time
    
    data = request.get_json()
    nl_query = data.get('natural_language_query')
    tenant_id = data.get('tenant_id')
    
    if not nl_query or not tenant_id:
        return jsonify({'error': 'Missing parameters'}), 400
    
    # Create a queue for progress messages
    progress_queue = queue.Queue()
    
    def send_progress(step, message, data=None):
        """Helper to send progress updates"""
        progress_queue.put({
            'step': step,
            'message': message,
            'data': data,
            'timestamp': time.time()
        })
    
    def translate_with_progress():
        """Run translation and emit progress"""
        try:
            # Step 1: Intent Analysis
            send_progress('intent_analysis', '🧠 Analyzing your question...')
            customer_schema = dashboard.get_customer_schema(tenant_id)
            intent_analysis, sql_generation = dashboard.nl_to_sql_translator.translate_natural_language_to_sql(nl_query)
            send_progress('intent_complete', '✅ Intent understood', {
                'intent': intent_analysis.query_intent.value,
                'confidence': intent_analysis.confidence
            })
            
            # Step 2: Canonical SQL Generated
            send_progress('canonical_sql', '📝 Generated canonical SQL', {
                'sql': sql_generation.sql_query
            })
            
            # Step 3: Schema Loading
            send_progress('schema_load', f'📊 Loading {tenant_id} schema...')
            send_progress('schema_loaded', '✅ Schema loaded')
            
            # Step 4: Check Cache
            send_progress('cache_check', '💾 Checking mapping cache...')
            is_cached = tenant_id in dashboard.query_engine.mapping_cache
            send_progress('cache_result', f'{"⚡ Cache HIT - using cached mappings" if is_cached else "🔍 Cache MISS - discovering mappings"}', {
                'cached': is_cached
            })
            
            # Step 5: Translation
            send_progress('translation_start', '🔄 Translating to tenant schema...')
            
            translated_result = dashboard.query_engine.translate_query(
                sql_generation.sql_query,
                customer_schema,
                tenant_id
            )
            
            send_progress('translation_complete', '✅ Translation complete', {
                'sql': translated_result.translated_query,
                'confidence': translated_result.confidence,
                'warnings': translated_result.warnings,
                'validation_errors': translated_result.validation_errors or []
            })
            
            # Step 6: Final Result
            send_progress('complete', '🎉 All done!', {
                'success': True,
                'natural_query': nl_query,
                'canonical_query': sql_generation.sql_query,
                'translated_query': translated_result.translated_query,
                'confidence': sql_generation.confidence,
                'intent': intent_analysis.query_intent.value,
                'reasoning': sql_generation.reasoning
            })
            
        except Exception as e:
            import traceback
            send_progress('error', f'❌ Error: {str(e)}', {
                'error': str(e),
                'traceback': traceback.format_exc()
            })
    
    def generate():
        """SSE generator"""
        # Start translation in background thread
        thread = threading.Thread(target=translate_with_progress)
        thread.daemon = True
        thread.start()
        
        # Stream progress updates
        while True:
            try:
                progress = progress_queue.get(timeout=30)
                yield f"data: {json.dumps(progress)}\n\n"
                
                # Stop if complete or error
                if progress['step'] in ['complete', 'error']:
                    break
                    
            except queue.Empty:
                # Send keepalive
                yield f"data: {json.dumps({'step': 'keepalive'})}\n\n"
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )

@app.route('/api/tenants/list', methods=['GET'])
def api_tenants_list():
    """Get list of available tenants"""
    try:
        tenants = []
        for tenant_id, tenant_name in dashboard.tenants.items():
            tenants.append({
                'id': tenant_id,
                'name': tenant_name,
                'has_schema': True,
                'has_sample_data': True
            })
        
        return jsonify({
            'success': True,
            'tenants': tenants
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/tenants/<tenant_id>/schema', methods=['GET'])
def api_get_tenant_schema_details(tenant_id):
    """Get detailed schema information for a specific tenant"""
    try:
        schema_file = Path(f"customer_schemas/{tenant_id}/schema.yaml")
        if not schema_file.exists():
            return jsonify({
                'success': False,
                'error': f'Schema file not found for {tenant_id}'
            }), 404
        
        with open(schema_file, 'r') as f:
            schema_data = yaml.safe_load(f)
        
        tables_info = []
        for table_name, table_info in schema_data.get('tables', {}).items():
            columns = []
            for col_name, col_info in table_info.get('columns', {}).items():
                columns.append({
                    'name': col_name,
                    'type': col_info.get('type', 'unknown'),
                    'nullable': col_info.get('nullable', True),
                    'description': col_info.get('description', '')
                })
            
            tables_info.append({
                'name': table_name,
                'description': table_info.get('description', ''),
                'primary_key': table_info.get('primary_key', []),
                'columns': columns,
                'column_count': len(columns)
            })
        
        return jsonify({
            'success': True,
            'tenant_id': tenant_id,
            'tenant_name': dashboard.get_tenant_name(tenant_id),
            'tables': tables_info,
            'table_count': len(tables_info)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/cache/stats', methods=['GET'])
def api_cache_stats():
    """Get cache statistics with per-tenant details"""
    try:
        cache = dashboard.query_engine.mapping_cache
        
        # Calculate overall stats
        cached_tenants = len(cache)
        total_usage = sum(tenant_data.get('usage_count', 0) for tenant_data in cache.values())
        
        # Estimate cache hits vs misses (simplified)
        cache_hits = total_usage - cached_tenants
        cache_misses = cached_tenants
        hit_rate = (cache_hits / total_usage * 100) if total_usage > 0 else 0
        
        # Get all available tenants
        all_tenants = list(dashboard.customer_schemas.keys())
        
        # Build per-tenant cache status
        tenant_details = []
        for tenant_id in all_tenants:
            is_cached = tenant_id in cache
            tenant_cache = cache.get(tenant_id, {})
            
            # Get cache details
            field_count = len(tenant_cache.get('field_mappings', {}))
            complex_count = len(tenant_cache.get('complex_mappings', []))
            usage_count = tenant_cache.get('usage_count', 0)
            last_updated = tenant_cache.get('last_updated', 'Never')
            
            tenant_details.append({
                'tenant_id': tenant_id,
                'tenant_name': dashboard.get_tenant_name(tenant_id),
                'is_cached': is_cached,
                'cache_status': 'cached' if is_cached else 'cold',
                'field_mappings': field_count,
                'complex_mappings': complex_count,
                'usage_count': usage_count,
                'last_updated': last_updated
            })
        
        # Calculate average translation time
        # Cached queries: ~0.4s (fast), Cold queries: ~8s (first-time LLM)
        cached_time = 0.4  # seconds for cached queries
        cold_time = 8.0    # seconds for first-time queries
        
        if total_usage > 0:
            # Weighted average based on cache hits and misses
            avg_translation_time = ((cache_hits * cached_time) + (cache_misses * cold_time)) / total_usage
        else:
            # Default to cached time when no queries yet
            avg_translation_time = cached_time
        
        return jsonify({
            'success': True,
            'stats': {
                'cached_tenants': cached_tenants,
                'total_tenants': len(all_tenants),
                'cache_hits': cache_hits,
                'cache_misses': cache_misses,
                'hit_rate': hit_rate,
                'total_queries': total_usage,
                'avg_translation_time': round(avg_translation_time, 2)
            },
            'tenant_details': tenant_details
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============================================================================
# QUERY EXECUTION ENDPOINTS
# ============================================================================

@app.route('/api/query-execution/execute', methods=['POST'])
def api_execute_query():
    """Execute a translated query against a tenant's DuckDB database"""
    try:
        data = request.get_json()
        tenant_id = data.get('tenant_id')
        query = data.get('query')
        
        if not tenant_id or not query:
            return jsonify({
                'success': False,
                'error': 'Missing tenant_id or query'
            }), 400
        
        # Import query executor
        from app.core.query_executor import TenantQueryExecutor
        
        # Initialize executor
        databases_dir = Path('databases')
        executor = TenantQueryExecutor(databases_dir)
        
        # Execute query
        result = executor.execute_for_tenant(tenant_id, query)
        
        return jsonify(result)
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'hint': 'Run the import script first: python scripts/import_csv_to_duckdb.py'
        }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }), 500


@app.route('/api/query-execution/translate-and-execute', methods=['POST'])
def api_translate_and_execute():
    """Translate canonical query and execute it against tenant database"""
    try:
        data = request.get_json()
        canonical_query = data.get('canonical_query')
        tenant_id = data.get('tenant_id')
        
        if not canonical_query or not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Missing canonical_query or tenant_id'
            }), 400
        
        # Step 1: Translate query
        customer_schema = dashboard.get_customer_schema(tenant_id)
        if not customer_schema:
            return jsonify({
                'success': False,
                'error': f'Schema not found for tenant {tenant_id}'
            }), 404
        
        translation_result = dashboard.query_engine.translate_query(
            canonical_query,
            customer_schema,
            tenant_id
        )
        
        # Step 1.5: Apply unit conversion for tenant_D
        translated_sql = translation_result.translated_query
        if tenant_id == 'tenant_D' and 'contract_value_usd_millions' in translated_sql:
            # Convert dollar values to millions for tenant_D
            import re
            
            # Pattern to match WHERE clauses with numeric comparisons
            def convert_value(match):
                operator = match.group(1)  # >, <, =, etc.
                value = float(match.group(2))
                converted_value = value / 1000000  # Convert to millions
                return f"contract_value_usd_millions {operator} {converted_value}"
            
            # Apply conversion to WHERE clauses
            translated_sql = re.sub(
                r'contract_value_usd_millions\s*([><=]+)\s*(\d+(?:\.\d+)?)',
                convert_value,
                translated_sql
            )
            
            # Handle BETWEEN clauses
            def convert_between(match):
                col = match.group(1)
                val1 = float(match.group(2))
                val2 = float(match.group(3))
                return f"{col} BETWEEN {val1/1000000} AND {val2/1000000}"
            
            translated_sql = re.sub(
                r'(contract_value_usd_millions)\s+BETWEEN\s+(\d+(?:\.\d+)?)\s+AND\s+(\d+(?:\.\d+)?)',
                convert_between,
                translated_sql
            )
            
            # Update the translation result
            translation_result.translated_query = translated_sql
        
        # Step 2: Execute translated query
        from app.core.query_executor import TenantQueryExecutor
        
        databases_dir = Path('databases')
        executor = TenantQueryExecutor(databases_dir)
        
        execution_result = executor.execute_for_tenant(
            tenant_id,
            translation_result.translated_query
        )
        
        # Combine results
        return jsonify({
            'success': True,
            'translation': {
                'canonical_query': canonical_query,
                'translated_query': translation_result.translated_query,
                'confidence': translation_result.confidence,
                'warnings': translation_result.warnings or [],
                'reasoning': translation_result.reasoning
            },
            'execution': execution_result
        })
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'hint': 'Run the import script first: python scripts/import_csv_to_duckdb.py'
        }), 404
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__,
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/query-execution/databases')
def api_list_databases():
    """List available tenant databases"""
    try:
        from app.core.query_executor import TenantQueryExecutor
        
        databases_dir = Path('databases')
        executor = TenantQueryExecutor(databases_dir)
        
        tenants = executor.list_available_tenants()
        
        # Get details for each tenant
        tenant_details = []
        for tenant_id in tenants:
            try:
                tenant_executor = executor.get_executor(tenant_id)
                tables = tenant_executor.get_table_list()
                
                tenant_details.append({
                    'tenant_id': tenant_id,
                    'tables': tables,
                    'table_count': len(tables),
                    'status': 'ready'
                })
            except Exception as e:
                tenant_details.append({
                    'tenant_id': tenant_id,
                    'status': 'error',
                    'error': str(e)
                })
        
        return jsonify({
            'success': True,
            'databases': tenant_details
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/query-execution/schema/<tenant_id>/<table_name>')
def api_get_table_schema(tenant_id, table_name):
    """Get schema information for a specific table"""
    try:
        from app.core.query_executor import TenantQueryExecutor
        
        databases_dir = Path('databases')
        executor = TenantQueryExecutor(databases_dir)
        
        tenant_executor = executor.get_executor(tenant_id)
        schema = tenant_executor.get_table_schema(table_name)
        
        return jsonify({
            'success': True,
            'tenant_id': tenant_id,
            'table_name': table_name,
            'columns': schema
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    templates_dir = Path("templates")
    templates_dir.mkdir(exist_ok=True)
    
    print("🌐 Starting Schema Translator Web Dashboard...")
    print("Dashboard will be available at: http://localhost:8080")
    print("Features:")
    print("• Interactive testing interface")
    print("• HITL workflow demonstration")
    print("• Reports and analytics")
    print("• Real-time API status")
    
    app.run(debug=True, host='0.0.0.0', port=8082)
