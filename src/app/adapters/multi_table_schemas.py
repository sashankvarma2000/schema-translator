"""
Multi-Table Customer Schemas for Query Translation Demonstration

This module provides realistic customer database schemas that demonstrate
different ways of splitting logical contract data across multiple tables.
These schemas are used to test and demonstrate the query translation system.
"""

from typing import Dict, Any, List
import yaml
import os
from pathlib import Path

def load_real_customer_schemas() -> Dict[str, Any]:
    """Load all real customer schemas from the customer_schemas directory"""
    schemas = {}
    customer_schemas_dir = Path.cwd() / "customer_schemas"
    
    # Load all tenant schemas
    for tenant_dir in customer_schemas_dir.iterdir():
        if tenant_dir.is_dir():
            schema_file = tenant_dir / "schema.yaml"
            if schema_file.exists():
                try:
                    with open(schema_file, 'r') as f:
                        schema_data = yaml.safe_load(f)
                        tenant_name = tenant_dir.name
                        schemas[tenant_name] = schema_data
                except Exception as e:
                    print(f"Error loading schema for {tenant_dir.name}: {e}")
    
    return schemas


def get_customer_a_schema() -> Dict[str, Any]:
    """
    Customer A: Simple single-table schema
    All contract data in one table - no translation needed
    """
    return {
        "customer_id": "customer_a",
        "description": "Simple single-table contract schema",
        "tables": {
            "contracts": {
                "description": "Main contracts table with all contract information",
                "columns": {
                    "id": {
                        "type": "integer",
                        "description": "Primary key contract identifier",
                        "nullable": False,
                        "is_primary_key": True,
                        "is_foreign_key": False
                    },
                    "contract_id": {
                        "type": "string",
                        "description": "External contract identifier",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "status": {
                        "type": "string",
                        "description": "Current contract status (active, expired, cancelled)",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "value": {
                        "type": "decimal",
                        "description": "Contract value in USD",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "expiry_date": {
                        "type": "date",
                        "description": "Contract expiry date",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "buyer_name": {
                        "type": "string",
                        "description": "Name of the buying organization",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "supplier_name": {
                        "type": "string",
                        "description": "Name of the supplier organization",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "contract_type": {
                        "type": "string",
                        "description": "Type of contract (service, goods, mixed)",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "created_date": {
                        "type": "datetime",
                        "description": "Contract creation date",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    }
                }
            }
        }
    }


def get_customer_b_schema() -> Dict[str, Any]:
    """
    Customer B: Multi-table split schema
    Contract data split across headers, status history, and renewal schedule
    """
    return {
        "customer_id": "customer_b",
        "description": "Multi-table contract schema with split data",
        "tables": {
            "contract_headers": {
                "description": "Main contract header information",
                "columns": {
                    "id": {
                        "type": "integer",
                        "description": "Primary key contract identifier",
                        "nullable": False,
                        "is_primary_key": True,
                        "is_foreign_key": False
                    },
                    "contract_id": {
                        "type": "string",
                        "description": "External contract identifier",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "value": {
                        "type": "decimal",
                        "description": "Contract value in USD",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "buyer_name": {
                        "type": "string",
                        "description": "Name of the buying organization",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "supplier_name": {
                        "type": "string",
                        "description": "Name of the supplier organization",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "contract_type": {
                        "type": "string",
                        "description": "Type of contract (service, goods, mixed)",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "created_date": {
                        "type": "datetime",
                        "description": "Contract creation date",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    }
                }
            },
            "contract_status_history": {
                "description": "Contract status changes over time",
                "columns": {
                    "id": {
                        "type": "integer",
                        "description": "Primary key for status record",
                        "nullable": False,
                        "is_primary_key": True,
                        "is_foreign_key": False
                    },
                    "contract_id": {
                        "type": "integer",
                        "description": "Foreign key to contract_headers.id",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": True
                    },
                    "status": {
                        "type": "string",
                        "description": "Contract status (active, expired, cancelled)",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "status_date": {
                        "type": "datetime",
                        "description": "Date when status was set",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "is_current": {
                        "type": "boolean",
                        "description": "Whether this is the current status",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "status_reason": {
                        "type": "string",
                        "description": "Reason for status change",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    }
                }
            },
            "renewal_schedule": {
                "description": "Contract renewal and expiry information",
                "columns": {
                    "id": {
                        "type": "integer",
                        "description": "Primary key for renewal record",
                        "nullable": False,
                        "is_primary_key": True,
                        "is_foreign_key": False
                    },
                    "contract_id": {
                        "type": "integer",
                        "description": "Foreign key to contract_headers.id",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": True
                    },
                    "expiry_date": {
                        "type": "date",
                        "description": "Contract expiry date",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "renewal_date": {
                        "type": "date",
                        "description": "Next renewal date",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "auto_renew": {
                        "type": "boolean",
                        "description": "Whether contract auto-renews",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "renewal_term_months": {
                        "type": "integer",
                        "description": "Renewal term in months",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    }
                }
            }
        }
    }


def get_customer_c_schema() -> Dict[str, Any]:
    """
    Customer C: Different multi-table split
    Contract data split across master, details, and lifecycle tables
    """
    return {
        "customer_id": "customer_c",
        "description": "Alternative multi-table contract schema",
        "tables": {
            "contract_master": {
                "description": "Master contract information",
                "columns": {
                    "contract_id": {
                        "type": "string",
                        "description": "Primary key contract identifier",
                        "nullable": False,
                        "is_primary_key": True,
                        "is_foreign_key": False
                    },
                    "buyer_name": {
                        "type": "string",
                        "description": "Name of the buying organization",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "supplier_name": {
                        "type": "string",
                        "description": "Name of the supplier organization",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "contract_type": {
                        "type": "string",
                        "description": "Type of contract (service, goods, mixed)",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "created_date": {
                        "type": "datetime",
                        "description": "Contract creation date",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    }
                }
            },
            "contract_details": {
                "description": "Contract financial and value details",
                "columns": {
                    "id": {
                        "type": "integer",
                        "description": "Primary key for detail record",
                        "nullable": False,
                        "is_primary_key": True,
                        "is_foreign_key": False
                    },
                    "contract_id": {
                        "type": "string",
                        "description": "Foreign key to contract_master.contract_id",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": True
                    },
                    "value": {
                        "type": "decimal",
                        "description": "Contract value in USD",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "currency": {
                        "type": "string",
                        "description": "Currency code (USD, EUR, GBP)",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "payment_terms": {
                        "type": "string",
                        "description": "Payment terms (net 30, net 60, etc.)",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "billing_frequency": {
                        "type": "string",
                        "description": "Billing frequency (monthly, quarterly, annual)",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    }
                }
            },
            "contract_lifecycle": {
                "description": "Contract lifecycle and status information",
                "columns": {
                    "id": {
                        "type": "integer",
                        "description": "Primary key for lifecycle record",
                        "nullable": False,
                        "is_primary_key": True,
                        "is_foreign_key": False
                    },
                    "contract_id": {
                        "type": "string",
                        "description": "Foreign key to contract_master.contract_id",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": True
                    },
                    "status": {
                        "type": "string",
                        "description": "Current contract status (active, expired, cancelled)",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "expiry_date": {
                        "type": "date",
                        "description": "Contract expiry date",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "effective_date": {
                        "type": "date",
                        "description": "Contract effective date",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "last_modified": {
                        "type": "datetime",
                        "description": "Last modification date",
                        "nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    },
                    "status_reason": {
                        "type": "string",
                        "description": "Reason for current status",
                        "nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False
                    }
                }
            }
        }
    }


def get_all_customer_schemas() -> Dict[str, Dict[str, Any]]:
    """Get all customer schemas - both real and mock data"""
    # Load real customer schemas
    real_schemas = load_real_customer_schemas()
    
    # Also include mock schemas for testing
    mock_schemas = {
        "customer_a": get_customer_a_schema(),
        "customer_b": get_customer_b_schema(),
        "customer_c": get_customer_c_schema()
    }
    
    # Combine real and mock schemas
    all_schemas = {}
    all_schemas.update(real_schemas)  # Real tenants: tenant_A, tenant_B, etc.
    all_schemas.update(mock_schemas)  # Mock customers: customer_a, customer_b, etc.
    
    return all_schemas


def get_demo_queries() -> Dict[str, List[str]]:
    """Get demonstration queries for testing query translation"""
    return {
        "simple_queries": [
            "SELECT ocid, release_id FROM releases WHERE tag LIKE '%award%'",
            "SELECT party_id, party_name FROM parties WHERE party_role = 'buyer'",
            "SELECT contract_id, contract_status FROM contracts WHERE contract_status = 'active'",
            "SELECT award_id, award_status FROM awards WHERE award_date > '2024-01-01'"
        ],
        "complex_queries": [
            "SELECT c.contract_id, c.contract_value_amount, p.party_name FROM contracts c JOIN parties p ON c.buyer_party_id = p.party_id WHERE c.contract_status = 'active' AND c.contract_value_amount > 100000",
            "SELECT a.award_id, a.award_value_amount, COUNT(c.contract_id) as contract_count FROM awards a LEFT JOIN contracts c ON a.award_id = c.related_award_id WHERE a.award_date BETWEEN '2024-01-01' AND '2024-12-31' GROUP BY a.award_id, a.award_value_amount",
            "SELECT p.party_name, SUM(c.contract_value_amount) as total_value FROM parties p JOIN contracts c ON p.party_id = c.supplier_party_id WHERE p.party_role = 'supplier' GROUP BY p.party_name HAVING SUM(c.contract_value_amount) > 500000"
        ],
        "aggregation_queries": [
            "SELECT contract_status, COUNT(*) as count, AVG(contract_value_amount) as avg_value FROM contracts GROUP BY contract_status",
            "SELECT YEAR(award_date) as year, COUNT(*) as award_count, SUM(award_value_amount) as total_value FROM awards GROUP BY YEAR(award_date)",
            "SELECT p.party_name, COUNT(c.contract_id) as contract_count, MAX(c.contract_value_amount) as max_contract FROM parties p JOIN contracts c ON p.party_id = c.supplier_party_id WHERE p.party_role = 'supplier' GROUP BY p.party_name"
        ],
        "ocds_specific_queries": [
            "SELECT r.ocid, r.release_date, COUNT(DISTINCT a.award_id) as award_count FROM releases r LEFT JOIN awards a ON r.ocid = a.ocid WHERE r.tag LIKE '%award%' GROUP BY r.ocid, r.release_date",
            "SELECT t.tender_id, t.tender_title, t.tender_value_amount, COUNT(a.award_id) as awards_made FROM tenders t LEFT JOIN awards a ON t.tender_id = a.related_tender_id GROUP BY t.tender_id, t.tender_title, t.tender_value_amount",
            "SELECT p.party_name, p.party_identifier_scheme, COUNT(DISTINCT c.contract_id) as contracts, SUM(c.contract_value_amount) as total_value FROM parties p JOIN contracts c ON p.party_id = c.supplier_party_id WHERE p.party_role = 'supplier' GROUP BY p.party_name, p.party_identifier_scheme"
        ]
    }


def get_canonical_schema_mapping() -> Dict[str, str]:
    """
    Mapping from canonical schema fields to customer-specific fields
    This helps the LLM understand how to translate queries
    """
    return {
        "contract_id": {
            "customer_a": "contracts.contract_id",
            "customer_b": "contract_headers.contract_id", 
            "customer_c": "contract_master.contract_id"
        },
        "status": {
            "customer_a": "contracts.status",
            "customer_b": "contract_status_history.status",
            "customer_c": "contract_lifecycle.status"
        },
        "value": {
            "customer_a": "contracts.value",
            "customer_b": "contract_headers.value",
            "customer_c": "contract_details.value"
        },
        "expiry_date": {
            "customer_a": "contracts.expiry_date",
            "customer_b": "renewal_schedule.expiry_date",
            "customer_c": "contract_lifecycle.expiry_date"
        },
        "buyer_name": {
            "customer_a": "contracts.buyer_name",
            "customer_b": "contract_headers.buyer_name",
            "customer_c": "contract_master.buyer_name"
        },
        "supplier_name": {
            "customer_a": "contracts.supplier_name",
            "customer_b": "contract_headers.supplier_name",
            "customer_c": "contract_master.supplier_name"
        },
        "contract_type": {
            "customer_a": "contracts.contract_type",
            "customer_b": "contract_headers.contract_type",
            "customer_c": "contract_master.contract_type"
        }
    }
