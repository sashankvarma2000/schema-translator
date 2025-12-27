# Project Structure

This document describes the organization of the Schema Translator project.

```
schema-translator/
├── README.md                    # Main project documentation
├── LICENSE                      # MIT License
├── CONTRIBUTING.md             # Contribution guidelines
├── requirements.txt             # Python dependencies
├── setup.py                     # Package setup
├── pyproject.toml              # Project metadata
├── .env.example                 # Environment variables template
├── .gitignore                  # Git ignore rules
│
├── src/                        # Source code
│   └── app/
│       ├── core/               # Core translation logic
│       │   ├── query_translator.py      # Main translation engine
│       │   ├── nl_to_sql_translator.py   # Natural language to SQL
│       │   ├── discovery.py              # Schema discovery
│       │   ├── query_executor.py         # Query execution
│       │   ├── table_relationship_analyzer.py
│       │   ├── config.py                 # Configuration
│       │   └── config_manager.py        # Config management
│       ├── adapters/            # External service adapters
│       │   ├── llm_openai.py            # OpenAI integration
│       │   └── multi_table_schemas.py   # Schema management
│       └── shared/              # Shared utilities
│           ├── models.py                # Data models
│           └── logging.py               # Logging setup
│
├── config/                     # Configuration files
│   ├── server_config.yaml      # Server settings
│   └── tenant_config.yaml       # Tenant configurations
│
├── customer_schemas/            # Tenant schema definitions
│   ├── tenant_A/schema.yaml
│   ├── tenant_B/schema.yaml
│   ├── tenant_C/schema.yaml
│   ├── tenant_D/schema.yaml
│   └── tenant_E/schema.yaml
│
├── prompts/                    # LLM prompt templates
│   ├── column_mapping_v1.txt
│   ├── nl_to_sql_prompts.py
│   └── query_translation_prompts.py
│
├── templates/                  # HTML templates
│   ├── unified_dashboard.html
│   ├── base.html
│   └── ... (other templates)
│
├── static/                     # Static assets
│   ├── css/
│   │   └── unified-style.css
│   └── js/
│       ├── unified-dashboard.js
│       └── nl_to_sql.js
│
├── scripts/                    # Utility scripts
│   └── import_csv_to_duckdb.py
│
├── tests/                      # Test files
│   ├── __init__.py
│   └── test_discovery.py
│
├── web_dashboard.py            # Flask web application
├── api_server.py               # FastAPI server (optional)
├── canonical_schema.yaml       # Canonical schema definition
└── canonical_schema_original.yaml
```

## Key Files

### Core Application
- `web_dashboard.py` - Main Flask web application
- `api_server.py` - FastAPI REST API server
- `src/app/core/query_translator.py` - Query translation engine
- `src/app/core/nl_to_sql_translator.py` - Natural language processing

### Configuration
- `config/server_config.yaml` - Server and LLM configuration
- `config/tenant_config.yaml` - Tenant-specific settings
- `.env.example` - Environment variables template

### Documentation
- `README.md` - Complete project documentation
- `SETUP_GUIDE.md` - Setup instructions
- `COMPLETE_SYSTEM_FLOW.md` - System architecture
- `DEMO_QUERIES.md` - Example queries

## Excluded from Repository

The following are excluded via `.gitignore`:
- `__pycache__/` - Python cache files
- `*.pyc` - Compiled Python files
- `cache/` - Mapping cache
- `databases/` - DuckDB database files
- `customer_samples/` - Sample CSV data (too large)
- `output/` - Generated outputs
- `reports/` - Generated reports
- `*.log` - Log files
- `.env` - Environment variables (contains API keys)

## Setup Instructions

1. Copy `.env.example` to `.env` and add your OpenAI API key
2. Install dependencies: `pip install -r requirements.txt`
3. Import sample data: `python scripts/import_csv_to_duckdb.py`
4. Start server: `python web_dashboard.py`

See `README.md` and `SETUP_GUIDE.md` for detailed instructions.

