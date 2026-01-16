"""
Simple Database Loader Module (Non-Spark Version)
Loads pandas DataFrames into DuckDB or PostgreSQL with logging and verification.
Author: mrohitth
Date: 2026-01-13
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
import sys

import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    import psycopg2
    from psycopg2.pool import SimpleConnectionPool
except ImportError:
    psycopg2 = None


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleDatabaseLoader:
    """
    Handles loading pandas DataFrames into DuckDB or PostgreSQL databases.
    Supports table creation, incremental loading, and verification.
    """
    
    def __init__(self, db_type: str, connection_params: Optional[Dict[str, Any]] = None):
        """
        Initialize the DatabaseLoader.
        
        Args:
            db_type: Type of database ('duckdb' or 'postgresql')
            connection_params: Dictionary with connection parameters
                For DuckDB: {'database': 'path_to_db_file'}
                For PostgreSQL: {'host': 'host', 'port': 'port', 'user': 'user', 
                                'password': 'password', 'database': 'database'}
        """
        self.db_type = db_type.lower()
        self.connection_params = connection_params or {}
        self.connection = None
        
        if self.db_type not in ['duckdb', 'postgresql']:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        if self.db_type == 'duckdb' and duckdb is None:
            raise ImportError("DuckDB is not installed. Install with: pip install duckdb")
        
        if self.db_type == 'postgresql' and psycopg2 is None:
            raise ImportError("psycopg2 is not installed. Install with: pip install psycopg2-binary")
        
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection based on db_type."""
        try:
            if self.db_type == 'duckdb':
                db_file = self.connection_params.get('database', ':memory:')
                self.connection = duckdb.connect(db_file)
                logger.info(f"Connected to DuckDB: {db_file}")
            
            elif self.db_type == 'postgresql':
                self.connection = psycopg2.connect(
                    host=self.connection_params.get('host', 'localhost'),
                    port=self.connection_params.get('port', 5432),
                    user=self.connection_params.get('user', 'postgres'),
                    password=self.connection_params.get('password', ''),
                    database=self.connection_params.get('database', 'postgres')
                )
                logger.info(f"Connected to PostgreSQL: {self.connection_params.get('database')}")
        
        except Exception as e:
            logger.error(f"Failed to initialize connection: {str(e)}")
            raise
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = 'overwrite',
        verify: bool = True
    ) -> Dict[str, Any]:
        """
        Load a pandas DataFrame into the database.
        
        Args:
            df: pandas DataFrame to load
            table_name: Name of the target table
            mode: 'overwrite' to replace table, 'append' for incremental load
            verify: Whether to verify the load by querying back
        
        Returns:
            Dictionary with load statistics
        """
        load_stats = {
            'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'table_name': table_name,
            'mode': mode,
            'status': 'FAILED',
            'input_row_count': 0,
            'output_row_count': 0,
            'schema': None,
            'message': ''
        }
        
        try:
            # Get input row count
            load_stats['input_row_count'] = len(df)
            logger.info(f"Input DataFrame has {load_stats['input_row_count']} rows")
            
            # Get schema information
            load_stats['schema'] = {col: str(dtype) for col, dtype in df.dtypes.items()}
            logger.info(f"Schema: {load_stats['schema']}")
            
            if self.db_type == 'duckdb':
                self._load_to_duckdb(df, table_name, mode, load_stats)
            elif self.db_type == 'postgresql':
                self._load_to_postgresql(df, table_name, mode, load_stats)
            
            # Verify the load
            if verify:
                self._verify_load(table_name, load_stats)
            
            load_stats['status'] = 'SUCCESS'
            logger.info(f"Successfully loaded {load_stats['output_row_count']} rows to {table_name}")
        
        except Exception as e:
            load_stats['message'] = f"Load failed: {str(e)}"
            logger.error(load_stats['message'])
        
        return load_stats
    
    def _load_to_duckdb(self, df: pd.DataFrame, table_name: str, mode: str, stats: Dict):
        """Load DataFrame to DuckDB."""
        try:
            logger.info(f"Loading DataFrame to DuckDB table: {table_name}")
            
            if mode == 'overwrite':
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped existing table {table_name}")
                self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            elif mode == 'append':
                # Check if table exists using DuckDB's information_schema
                result = self.connection.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_name='{table_name}'"
                ).fetchall()
                
                if not result:
                    # Table doesn't exist, create it
                    self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                else:
                    # Table exists, insert data
                    self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            
            stats['output_row_count'] = len(df)
            logger.info(f"Successfully loaded {stats['output_row_count']} rows to DuckDB table {table_name}")
        
        except Exception as e:
            logger.error(f"DuckDB load failed: {str(e)}")
            raise
    
    def _load_to_postgresql(self, df: pd.DataFrame, table_name: str, mode: str, stats: Dict):
        """Load DataFrame to PostgreSQL."""
        try:
            logger.info(f"Loading DataFrame to PostgreSQL table: {table_name}")
            
            cursor = self.connection.cursor()
            
            # Create table if not exists or drop if overwrite mode
            if mode == 'overwrite':
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.connection.commit()
                logger.info(f"Dropped existing table {table_name}")
            
            # Create table schema
            columns_def = []
            for col_name, col_type in stats['schema'].items():
                pg_type = self._pandas_type_to_postgres(col_type)
                columns_def.append(f'"{col_name}" {pg_type}')
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns_def)}
                )
            """
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info(f"Table {table_name} created or already exists")
            
            # Insert data
            placeholders = ', '.join(['%s'] * len(df.columns))
            columns = ', '.join([f'"{col}"' for col in df.columns])
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            for _, row in df.iterrows():
                cursor.execute(insert_sql, tuple(row))
            
            self.connection.commit()
            stats['output_row_count'] = len(df)
            logger.info(f"Successfully loaded {stats['output_row_count']} rows to PostgreSQL table {table_name}")
            cursor.close()
        
        except Exception as e:
            self.connection.rollback()
            logger.error(f"PostgreSQL load failed: {str(e)}")
            raise
    
    def _verify_load(self, table_name: str, stats: Dict):
        """Verify the load by querying back the table."""
        try:
            if self.db_type == 'duckdb':
                result = self.connection.execute(f"SELECT COUNT(*) as cnt FROM {table_name}").fetchall()
                count = result[0][0] if result else 0
                
                logger.info(f"Verification - {table_name}: {count} rows")
            
            elif self.db_type == 'postgresql':
                cursor = self.connection.cursor()
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
                count = cursor.fetchone()[0]
                
                logger.info(f"Verification - {table_name}: {count} rows")
                cursor.close()
            
            stats['verified_row_count'] = count
        
        except Exception as e:
            logger.warning(f"Verification failed: {str(e)}")
    
    @staticmethod
    def _pandas_type_to_postgres(pandas_type: str) -> str:
        """Convert pandas data types to PostgreSQL types."""
        type_mapping = {
            'object': 'VARCHAR(255)',
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'int16': 'SMALLINT',
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'timedelta[ns]': 'INTERVAL',
        }
        return type_mapping.get(pandas_type, 'VARCHAR(255)')
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            pandas DataFrame with query results
        """
        try:
            if self.db_type == 'duckdb':
                result = self.connection.execute(query).fetchdf()
            elif self.db_type == 'postgresql':
                result = pd.read_sql(query, self.connection)
            
            logger.info(f"Query executed successfully, returned {len(result)} rows")
            return result
        
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.connection:
            try:
                if self.db_type == 'duckdb':
                    self.connection.close()
                elif self.db_type == 'postgresql':
                    self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")


def main():
    """
    Example usage of the SimpleDatabaseLoader class.
    """
    logger.info("=" * 80)
    logger.info("Simple Database Loader Example")
    logger.info("=" * 80)
    
    # Create sample data
    import numpy as np
    np.random.seed(42)
    sample_df = pd.DataFrame({
        'id': range(1, 11),
        'name': [f'Item_{i}' for i in range(1, 11)],
        'value': np.random.randint(10, 100, 10),
        'timestamp': pd.date_range('2024-01-01', periods=10, freq='D')
    })
    
    logger.info(f"Created sample DataFrame with shape: {sample_df.shape}")
    
    # Example: Loading to DuckDB
    try:
        logger.info("\nTesting DuckDB loader...")
        duckdb_loader = SimpleDatabaseLoader('duckdb', {'database': ':memory:'})
        
        stats = duckdb_loader.load_dataframe(sample_df, 'test_table', mode='overwrite')
        logger.info(f"DuckDB load stats: {stats}")
        
        # Query back the data
        result = duckdb_loader.execute_query("SELECT * FROM test_table LIMIT 5")
        logger.info(f"Query result shape: {result.shape}")
        
        duckdb_loader.close()
        logger.info("✅ DuckDB test completed successfully")
    except Exception as e:
        logger.error(f"DuckDB test failed: {str(e)}")
    
    logger.info("\n" + "=" * 80)
    logger.info("Simple Database Loader Example Complete!")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
