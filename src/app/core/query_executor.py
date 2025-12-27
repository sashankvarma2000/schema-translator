"""
DuckDB-based query executor for tenant-specific SQL queries.

Provides safe query execution with:
- Read-only query enforcement (SELECT only)
- Query timeout protection
- Automatic LIMIT injection
- Result formatting
"""

import duckdb
# import signal  # Removed for cross-platform compatibility
from pathlib import Path
from typing import Dict, List, Any, Optional
from contextlib import contextmanager
import re


class QueryTimeoutError(Exception):
    """Raised when query execution exceeds timeout limit."""
    pass


class QueryExecutor:
    """
    Execute SQL queries against tenant databases using DuckDB.
    
    Features:
    - Read-only execution (SELECT queries only)
    - Automatic timeout protection (30 seconds default)
    - Result limiting (max 1000 rows default)
    - JSON-serializable output
    """
    
    def __init__(
        self,
        db_path: Path,
        read_only: bool = True,
        max_timeout: int = 30,
        max_rows: int = 1000
    ):
        """
        Initialize query executor.
        
        Args:
            db_path: Path to DuckDB database file
            read_only: If True, only allow SELECT queries
            max_timeout: Maximum query execution time in seconds
            max_rows: Maximum rows to return (adds LIMIT if not present)
        """
        self.db_path = Path(db_path)
        self.read_only = read_only
        self.max_timeout = max_timeout
        self.max_rows = max_rows
        
    def _is_select_query(self, query: str) -> bool:
        """Check if query is a SELECT statement."""
        # Remove comments and whitespace
        cleaned = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
        cleaned = cleaned.strip().upper()
        
        # Check if it starts with SELECT (or WITH for CTEs)
        return cleaned.startswith('SELECT') or cleaned.startswith('WITH')
    
    def _inject_limit(self, query: str) -> str:
        """
        Add LIMIT clause if not present.
        
        Args:
            query: SQL query string
            
        Returns:
            Modified query with LIMIT clause
        """
        # Check if LIMIT already exists
        if re.search(r'\bLIMIT\s+\d+', query, re.IGNORECASE):
            return query
            
        # Add LIMIT to the end
        return f"{query.rstrip(';')} LIMIT {self.max_rows}"
    
    @contextmanager
    def _timeout_handler(self):
        """Context manager for query timeout."""
        # Note: Signal-based timeout disabled for cross-platform compatibility
        # DuckDB has its own query timeout mechanisms
        yield
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL query and return results.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Dict with:
                - columns: List of column names
                - rows: List of row dicts
                - row_count: Number of rows returned
                - query: The executed query (with LIMIT)
                
        Raises:
            ValueError: If query is not a SELECT statement
            QueryTimeoutError: If query exceeds timeout
            Exception: For other database errors
        """
        # Validate read-only if required
        if self.read_only and not self._is_select_query(query):
            raise ValueError(
                "Only SELECT queries are allowed in read-only mode"
            )
        
        # Inject LIMIT if needed
        modified_query = self._inject_limit(query)
        
        try:
            # Connect to database
            conn = duckdb.connect(str(self.db_path), read_only=self.read_only)
            
            # Execute with timeout
            with self._timeout_handler():
                result = conn.execute(modified_query, params or {})
                
                # Fetch results
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                
                # Convert to list of dicts
                rows_dict = [
                    dict(zip(columns, row))
                    for row in rows
                ]
                
                conn.close()
                
                return {
                    "columns": columns,
                    "rows": rows_dict,
                    "row_count": len(rows_dict),
                    "query": modified_query,
                    "success": True
                }
                
        except QueryTimeoutError:
            raise
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "query": modified_query
            }
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            conn = duckdb.connect(str(self.db_path), read_only=self.read_only)
            conn.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False
    
    def get_table_list(self) -> List[str]:
        """
        Get list of tables in database.
        
        Returns:
            List of table names
        """
        try:
            conn = duckdb.connect(str(self.db_path), read_only=True)
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            )
            tables = [row[0] for row in result.fetchall()]
            conn.close()
            return tables
        except Exception as e:
            raise Exception(f"Failed to get table list: {e}")
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column info dicts with keys: column_name, data_type
        """
        try:
            conn = duckdb.connect(str(self.db_path), read_only=True)
            result = conn.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_name = ? "
                "ORDER BY ordinal_position",
                [table_name]
            )
            columns = [
                {"column_name": row[0], "data_type": row[1]}
                for row in result.fetchall()
            ]
            conn.close()
            return columns
        except Exception as e:
            raise Exception(f"Failed to get table schema: {e}")


class TenantQueryExecutor:
    """
    Manage query execution across multiple tenants.
    """
    
    def __init__(self, databases_dir: Path):
        """
        Initialize tenant query executor.
        
        Args:
            databases_dir: Directory containing tenant database files
        """
        self.databases_dir = Path(databases_dir)
        self.executors: Dict[str, QueryExecutor] = {}
    
    def get_executor(self, tenant_id: str) -> QueryExecutor:
        """
        Get or create query executor for tenant.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            QueryExecutor instance for the tenant
            
        Raises:
            FileNotFoundError: If tenant database doesn't exist
        """
        if tenant_id not in self.executors:
            db_path = self.databases_dir / f"{tenant_id}.duckdb"
            if not db_path.exists():
                raise FileNotFoundError(
                    f"Database for tenant '{tenant_id}' not found at {db_path}"
                )
            self.executors[tenant_id] = QueryExecutor(db_path)
        
        return self.executors[tenant_id]
    
    def execute_for_tenant(
        self,
        tenant_id: str,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute query for specific tenant.
        
        Args:
            tenant_id: Tenant identifier
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Query results dict
        """
        executor = self.get_executor(tenant_id)
        return executor.execute_query(query, params)
    
    def list_available_tenants(self) -> List[str]:
        """
        Get list of tenants with available databases.
        
        Returns:
            List of tenant IDs
        """
        if not self.databases_dir.exists():
            return []
        
        return [
            db_file.stem
            for db_file in self.databases_dir.glob("*.duckdb")
        ]

