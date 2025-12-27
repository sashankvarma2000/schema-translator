#!/usr/bin/env python3
"""
Import CSV files from tenant directories into DuckDB databases.

This script:
- Reads CSV files from customer_samples/{tenant_id}/
- Uses LLM to intelligently detect column types (dates, numbers, etc.)
- Creates DuckDB tables with appropriate types
- Imports CSV data into DuckDB
- Handles multiple CSV files per tenant (one per table)
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
import yaml
import json
import os
from typing import Dict, List, Optional
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

console = Console()

# Import OpenAI for LLM-based type detection
try:
    from openai import OpenAI
    LLM_AVAILABLE = True
    openai_client = None  # Initialize lazily
except ImportError:
    LLM_AVAILABLE = False
    openai_client = None


class CSVImporter:
    """Import CSV files into DuckDB databases."""
    
    def __init__(
        self,
        samples_dir: Path,
        schemas_dir: Path,
        output_dir: Path
    ):
        """
        Initialize CSV importer.
        
        Args:
            samples_dir: Directory containing tenant CSV files
            schemas_dir: Directory containing tenant schema YAML files
            output_dir: Directory to store DuckDB database files
        """
        self.samples_dir = Path(samples_dir)
        self.schemas_dir = Path(schemas_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_schema(self, tenant_id: str) -> Optional[Dict]:
        """Load schema YAML for tenant if exists."""
        schema_path = self.schemas_dir / tenant_id / "schema.yaml"
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                return yaml.safe_load(f)
        return None
    
    def _get_duckdb_type(self, pandas_dtype: str) -> str:
        """
        Map pandas dtype to DuckDB type.
        
        Args:
            pandas_dtype: Pandas data type string
            
        Returns:
            DuckDB type string
        """
        dtype_map = {
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'float64': 'DOUBLE',
            'float32': 'FLOAT',
            'object': 'VARCHAR',
            'string': 'VARCHAR',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'date': 'DATE'
        }
        return dtype_map.get(str(pandas_dtype), 'VARCHAR')
    
    def _llm_infer_column_type(self, column_name: str, sample_values: List) -> str:
        """
        Use LLM to intelligently infer column type from sample values.
        
        Args:
            column_name: Name of the column
            sample_values: Sample values from the column
            
        Returns:
            DuckDB type string
        """
        global openai_client
        
        if not LLM_AVAILABLE:
            return 'VARCHAR'
        
        # Initialize OpenAI client lazily
        if openai_client is None:
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                console.print("[yellow]⚠️  OPENAI_API_KEY not set - skipping LLM inference[/]")
                return 'VARCHAR'
            openai_client = OpenAI(api_key=api_key)
        
        # Prepare sample values (filter out nulls and limit to 10)
        samples = [str(v) for v in sample_values if pd.notna(v)][:10]
        
        if not samples:
            return 'VARCHAR'
        
        prompt = f"""Given the column name and sample values, determine the most appropriate DuckDB data type.

Column Name: {column_name}
Sample Values: {samples}

Available DuckDB types:
- DATE (for dates like '2023-01-15', '2023/01/15', 'Jan 15, 2023')
- TIMESTAMP (for datetime with time component)
- BIGINT (for whole numbers)
- DOUBLE (for decimal numbers)
- BOOLEAN (for true/false, yes/no, 0/1)
- VARCHAR (for text strings)

Rules:
1. If values look like dates (YYYY-MM-DD, DD/MM/YYYY, etc.) → DATE
2. If values contain time (HH:MM:SS) → TIMESTAMP
3. If values are all integers → BIGINT
4. If values are all decimals → DOUBLE
5. If values are true/false or yes/no → BOOLEAN
6. Otherwise → VARCHAR

Respond with ONLY the type name (DATE, TIMESTAMP, BIGINT, DOUBLE, BOOLEAN, or VARCHAR).
"""

        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a database type inference expert. Respond with only the type name."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=20
            )
            
            detected_type = response.choices[0].message.content.strip().upper()
            
            # Validate the type
            valid_types = ['DATE', 'TIMESTAMP', 'BIGINT', 'DOUBLE', 'BOOLEAN', 'VARCHAR']
            if detected_type in valid_types:
                return detected_type
            else:
                return 'VARCHAR'
                
        except Exception as e:
            console.print(f"[yellow]⚠️  LLM type inference failed for {column_name}: {e}[/]")
            return 'VARCHAR'
    
    def _infer_schema_from_csv(self, csv_path: Path, use_llm: bool = True) -> List[Dict[str, str]]:
        """
        Infer schema from CSV file using LLM for intelligent type detection.
        
        Args:
            csv_path: Path to CSV file
            use_llm: Whether to use LLM for type inference
            
        Returns:
            List of column dicts with name and type
        """
        df = pd.read_csv(csv_path, nrows=1000)  # Sample first 1000 rows
        
        columns = []
        for col_name in df.columns:
            if use_llm and LLM_AVAILABLE:
                # Use LLM for intelligent type detection
                sample_values = df[col_name].dropna().head(15).tolist()
                inferred_type = self._llm_infer_column_type(col_name, sample_values)
            else:
                # Fallback to pandas dtype
                inferred_type = self._get_duckdb_type(df[col_name].dtype)
            
            columns.append({
                'name': col_name,
                'type': inferred_type
            })
        
        return columns
    
    def _create_table_ddl(
        self,
        table_name: str,
        columns: List[Dict[str, str]]
    ) -> str:
        """
        Generate CREATE TABLE DDL.
        
        Args:
            table_name: Name of the table
            columns: List of column definitions
            
        Returns:
            DDL string
        """
        col_defs = [
            f"{col['name']} {col['type']}"
            for col in columns
        ]
        
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        ddl += ",\n".join(f"  {col_def}" for col_def in col_defs)
        ddl += "\n)"
        
        return ddl
    
    def import_tenant(
        self,
        tenant_id: str,
        drop_existing: bool = False
    ) -> Dict[str, any]:
        """
        Import all CSV files for a tenant.
        
        Args:
            tenant_id: Tenant identifier
            drop_existing: If True, drop existing database before import
            
        Returns:
            Import statistics dict
        """
        tenant_dir = self.samples_dir / tenant_id
        
        if not tenant_dir.exists():
            raise FileNotFoundError(
                f"Tenant directory not found: {tenant_dir}"
            )
        
        # Find all CSV files
        csv_files = list(tenant_dir.glob("*.csv"))
        
        if not csv_files:
            raise ValueError(
                f"No CSV files found in {tenant_dir}"
            )
        
        # Database path
        db_path = self.output_dir / f"{tenant_id}.duckdb"
        
        # Drop existing if requested
        if drop_existing and db_path.exists():
            db_path.unlink()
        
        console.print(f"\n[bold blue]Importing tenant: {tenant_id}[/]")
        console.print(f"Database: {db_path}")
        console.print(f"CSV files: {len(csv_files)}")
        
        # Load schema if available
        schema_yaml = self._load_schema(tenant_id)
        
        # Connect to database
        conn = duckdb.connect(str(db_path))
        
        stats = {
            'tenant_id': tenant_id,
            'tables': {},
            'total_rows': 0,
            'success': True
        }
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            for csv_file in csv_files:
                table_name = csv_file.stem
                task = progress.add_task(
                    f"Importing {table_name}...",
                    total=None
                )
                
                try:
                    # Infer schema from CSV with LLM
                    console.print(f"  [cyan]🤖 Using LLM to detect column types for {table_name}...[/]")
                    columns = self._infer_schema_from_csv(csv_file, use_llm=True)
                    
                    # Log detected types
                    date_cols = [c['name'] for c in columns if c['type'] == 'DATE']
                    if date_cols:
                        console.print(f"  [green]✓ Detected DATE columns: {', '.join(date_cols)}[/]")
                    
                    # Create table
                    ddl = self._create_table_ddl(table_name, columns)
                    conn.execute(ddl)
                    
                    # Build SELECT with proper type casting
                    select_cols = []
                    for col in columns:
                        col_name = col['name']
                        col_type = col['type']
                        
                        if col_type == 'DATE':
                            # Cast text to DATE
                            select_cols.append(f"TRY_CAST(\"{col_name}\" AS DATE) AS \"{col_name}\"")
                        elif col_type == 'TIMESTAMP':
                            select_cols.append(f"TRY_CAST(\"{col_name}\" AS TIMESTAMP) AS \"{col_name}\"")
                        elif col_type == 'BIGINT':
                            select_cols.append(f"TRY_CAST(\"{col_name}\" AS BIGINT) AS \"{col_name}\"")
                        elif col_type == 'DOUBLE':
                            select_cols.append(f"TRY_CAST(\"{col_name}\" AS DOUBLE) AS \"{col_name}\"")
                        elif col_type == 'BOOLEAN':
                            select_cols.append(f"TRY_CAST(\"{col_name}\" AS BOOLEAN) AS \"{col_name}\"")
                        else:
                            select_cols.append(f"\"{col_name}\"")
                    
                    # Import data with type conversion
                    import_query = f"""
                        INSERT INTO {table_name}
                        SELECT {', '.join(select_cols)}
                        FROM read_csv_auto('{csv_file}')
                    """
                    conn.execute(import_query)
                    
                    # Get row count
                    result = conn.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()
                    row_count = result[0]
                    
                    stats['tables'][table_name] = {
                        'rows': row_count,
                        'columns': len(columns),
                        'success': True
                    }
                    stats['total_rows'] += row_count
                    
                    progress.update(
                        task,
                        description=f"✓ {table_name} ({row_count:,} rows)"
                    )
                    
                except Exception as e:
                    stats['tables'][table_name] = {
                        'success': False,
                        'error': str(e)
                    }
                    stats['success'] = False
                    progress.update(
                        task,
                        description=f"✗ {table_name} (failed)"
                    )
                    console.print(f"[red]Error importing {table_name}: {e}[/]")
        
        conn.close()
        
        return stats
    
    def import_all_tenants(self, drop_existing: bool = False) -> Dict[str, any]:
        """
        Import CSV files for all tenants.
        
        Args:
            drop_existing: If True, drop existing databases before import
            
        Returns:
            Overall import statistics
        """
        # Find all tenant directories
        tenant_dirs = [
            d for d in self.samples_dir.iterdir()
            if d.is_dir() and not d.name.startswith('.')
        ]
        
        if not tenant_dirs:
            console.print("[yellow]No tenant directories found[/]")
            return {}
        
        console.print(
            f"\n[bold]Found {len(tenant_dirs)} tenants to import[/]\n"
        )
        
        all_stats = {}
        
        for tenant_dir in tenant_dirs:
            tenant_id = tenant_dir.name
            try:
                stats = self.import_tenant(tenant_id, drop_existing)
                all_stats[tenant_id] = stats
            except Exception as e:
                console.print(
                    f"[red]Failed to import {tenant_id}: {e}[/]"
                )
                all_stats[tenant_id] = {
                    'success': False,
                    'error': str(e)
                }
        
        return all_stats
    
    def print_summary(self, all_stats: Dict[str, any]):
        """Print import summary table."""
        table = Table(title="Import Summary")
        
        table.add_column("Tenant", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Tables", justify="right")
        table.add_column("Total Rows", justify="right")
        
        for tenant_id, stats in all_stats.items():
            if stats.get('success'):
                status = "✓ Success"
                tables = str(len(stats.get('tables', {})))
                rows = f"{stats.get('total_rows', 0):,}"
            else:
                status = "✗ Failed"
                tables = "-"
                rows = "-"
            
            table.add_row(tenant_id, status, tables, rows)
        
        console.print("\n")
        console.print(table)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Import CSV files into DuckDB databases"
    )
    parser.add_argument(
        '--tenant',
        help='Import specific tenant only'
    )
    parser.add_argument(
        '--samples-dir',
        default='customer_samples',
        help='Directory containing tenant CSV files'
    )
    parser.add_argument(
        '--schemas-dir',
        default='customer_schemas',
        help='Directory containing tenant schema YAML files'
    )
    parser.add_argument(
        '--output-dir',
        default='databases',
        help='Directory to store DuckDB database files'
    )
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing databases before import'
    )
    
    args = parser.parse_args()
    
    # Create importer
    importer = CSVImporter(
        samples_dir=Path(args.samples_dir),
        schemas_dir=Path(args.schemas_dir),
        output_dir=Path(args.output_dir)
    )
    
    try:
        if args.tenant:
            # Import specific tenant
            stats = importer.import_tenant(
                args.tenant,
                drop_existing=args.drop_existing
            )
            importer.print_summary({args.tenant: stats})
        else:
            # Import all tenants
            all_stats = importer.import_all_tenants(
                drop_existing=args.drop_existing
            )
            importer.print_summary(all_stats)
        
        console.print("\n[bold green]Import completed![/]")
        
    except Exception as e:
        console.print(f"\n[bold red]Error: {e}[/]")
        sys.exit(1)


if __name__ == '__main__':
    main()

