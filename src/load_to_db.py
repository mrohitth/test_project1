"""
Database Loader Module
Loads transformed Spark DataFrames into DuckDB or PostgreSQL with logging and verification.
Author: mrohitth
Date: 2026-01-13
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
import sys

try:
    from pyspark.sql import DataFrame, SparkSession
except ImportError:
    print("Warning: PySpark not installed. Some features may not work.")

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


class DatabaseLoader:
    """
    Handles loading Spark DataFrames into DuckDB or PostgreSQL databases.
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
        df: 'DataFrame',
        table_name: str,
        mode: str = 'overwrite',
        verify: bool = True
    ) -> Dict[str, Any]:
        """
        Load a Spark DataFrame into the database.
        
        Args:
            df: Spark DataFrame to load
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
            load_stats['input_row_count'] = df.count()
            logger.info(f"Input DataFrame has {load_stats['input_row_count']} rows")
            
            # Get schema information
            load_stats['schema'] = {col.name: str(col.dataType) for col in df.schema}
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
    
    def _load_to_duckdb(self, df: 'DataFrame', table_name: str, mode: str, stats: Dict):
        """Load DataFrame to DuckDB."""
        try:
            # Convert Spark DataFrame to Arrow and then to DuckDB
            temp_table = f"temp_{table_name}_{datetime.utcnow().strftime('%s')}"
            
            # Register as temporary view in Spark (if available)
            df.createOrReplaceTempView(temp_table)
            
            # For DuckDB, we'll use direct insertion
            if mode == 'overwrite':
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped existing table {table_name}")
            
            # Create table from Arrow batches
            arrow_batches = df.collect()
            
            if mode == 'overwrite':
                self.connection.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM (SELECT 1 WHERE FALSE)
                """)
            
            # Insert data
            row_count = 0
            for batch in arrow_batches:
                row_count += 1
            
            # Simplified approach: use from_df
            df_pd = df.toPandas()
            
            if mode == 'overwrite':
                self.connection.register(table_name, df_pd)
                self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")
            elif mode == 'append':
                temp_name = f"temp_{table_name}"
                self.connection.register(temp_name, df_pd)
                self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_name}")
                self.connection.execute(f"DROP TABLE {temp_name}")
            
            stats['output_row_count'] = len(df_pd)
            logger.info(f"Successfully loaded {stats['output_row_count']} rows to DuckDB table {table_name}")
        
        except Exception as e:
            logger.error(f"DuckDB load failed: {str(e)}")
            raise
    
    def _load_to_postgresql(self, df: 'DataFrame', table_name: str, mode: str, stats: Dict):
        """Load DataFrame to PostgreSQL."""
        try:
            # Convert to Pandas for easier handling
            df_pd = df.toPandas()
            
            cursor = self.connection.cursor()
            
            # Create table if not exists
            if mode == 'overwrite':
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.connection.commit()
                logger.info(f"Dropped existing table {table_name}")
            
            # Create table schema
            columns_def = []
            for col_name, col_type in stats['schema'].items():
                pg_type = self._spark_type_to_postgres(col_type)
                columns_def.append(f"{col_name} {pg_type}")
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns_def)}
                )
            """
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info(f"Table {table_name} created or already exists")
            
            # Insert data
            placeholders = ', '.join(['%s'] * len(df_pd.columns))
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            
            for _, row in df_pd.iterrows():
                cursor.execute(insert_sql, tuple(row))
            
            self.connection.commit()
            stats['output_row_count'] = len(df_pd)
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
                
                schema_result = self.connection.execute(f"DESCRIBE {table_name}").fetchall()
                logger.info(f"Verification - {table_name}: {count} rows")
                logger.info(f"Columns: {[row[0] for row in schema_result]}")
            
            elif self.db_type == 'postgresql':
                cursor = self.connection.cursor()
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
                count = cursor.fetchone()[0]
                
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                columns = cursor.fetchall()
                
                logger.info(f"Verification - {table_name}: {count} rows")
                logger.info(f"Columns: {[col[0] for col in columns]}")
                cursor.close()
            
            stats['verified_row_count'] = count
        
        except Exception as e:
            logger.warning(f"Verification failed: {str(e)}")
    
    @staticmethod
    def _spark_type_to_postgres(spark_type: str) -> str:
        """Convert Spark data types to PostgreSQL types."""
        type_mapping = {
            'StringType': 'VARCHAR(255)',
            'IntegerType': 'INTEGER',
            'LongType': 'BIGINT',
            'FloatType': 'FLOAT',
            'DoubleType': 'DOUBLE PRECISION',
            'BooleanType': 'BOOLEAN',
            'TimestampType': 'TIMESTAMP',
            'DateType': 'DATE',
            'DecimalType': 'DECIMAL',
            'ByteType': 'SMALLINT',
            'ShortType': 'SMALLINT'
        }
        return type_mapping.get(spark_type, 'VARCHAR(255)')
    
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
    Example usage of the DatabaseLoader class.
    """
    logger.info("=" * 80)
    logger.info("Database Loader Script")
    logger.info(f"Current Time (UTC): 2026-01-13 19:00:22")
    logger.info(f"User: mrohitth")
    logger.info("=" * 80)
    
    # Example: Loading to DuckDB
    try:
        duckdb_loader = DatabaseLoader(
            'duckdb',
            {'database': 'test_database.db'}
        )
        logger.info("DuckDB loader initialized successfully")
        duckdb_loader.close()
    except Exception as e:
        logger.error(f"DuckDB initialization failed: {str(e)}")
    
    # Example: Loading to PostgreSQL
    try:
        postgres_loader = DatabaseLoader(
            'postgresql',
            {
                'host': 'localhost',
                'port': 5432,
                'user': 'postgres',
                'password': 'password',
                'database': 'testdb'
            }
        )
        logger.info("PostgreSQL loader initialized successfully")
        postgres_loader.close()
    except Exception as e:
        logger.error(f"PostgreSQL initialization failed: {str(e)}")
    
    logger.info("=" * 80)
    logger.info("Script execution completed")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
