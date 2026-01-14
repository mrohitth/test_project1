"""
Database loader module for loading data to various database backends.

This module provides a flexible, production-ready solution for loading data
from various sources to databases using either Spark or Pandas DataFrames.
Credentials are loaded from environment variables for security.
"""

import argparse
import logging
import os
from typing import Optional, Union, Dict, Any
from abc import ABC, abstractmethod
from pathlib import Path

try:
    import pyspark.sql as spark_sql
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


# Configure logging
def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Set up logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured logger instance
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('load_to_db.log')
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


class DatabaseLoader(ABC):
    """Abstract base class for database loaders."""
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def load_data(self, data: Union['spark_sql.DataFrame', 'pd.DataFrame'], 
                  table_name: str, **kwargs) -> None:
        """Load data to database."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass


class PostgreSQLLoader(DatabaseLoader):
    """PostgreSQL database loader."""
    
    def __init__(self, host: str, port: int, database: str, 
                 user: str, password: str):
        """
        Initialize PostgreSQL loader with connection parameters.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        logger.info(f"Initialized PostgreSQL loader for {host}:{port}/{database}")
    
    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        try:
            import psycopg2
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info("Successfully connected to PostgreSQL")
        except ImportError:
            logger.error("psycopg2 not installed. Install with: pip install psycopg2-binary")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    
    def load_data(self, data: Union['spark_sql.DataFrame', 'pd.DataFrame'], 
                  table_name: str, mode: str = "replace", **kwargs) -> None:
        """
        Load data to PostgreSQL table.
        
        Args:
            data: Spark or Pandas DataFrame
            table_name: Target table name
            mode: Load mode (replace, append)
            **kwargs: Additional parameters for sqlalchemy
        """
        try:
            if not self.connection:
                self.connect()
            
            # Convert Spark DataFrame to Pandas if necessary
            if SPARK_AVAILABLE and isinstance(data, spark_sql.DataFrame):
                logger.info(f"Converting Spark DataFrame to Pandas for loading to {table_name}")
                data = data.toPandas()
            
            if not PANDAS_AVAILABLE:
                raise ImportError("pandas not installed. Install with: pip install pandas")
            
            # Use sqlalchemy for loading
            try:
                from sqlalchemy import create_engine
                connection_string = (
                    f"postgresql://{self.user}:{self.password}@"
                    f"{self.host}:{self.port}/{self.database}"
                )
                engine = create_engine(connection_string)
                
                logger.info(f"Loading {len(data)} rows to table '{table_name}'")
                data.to_sql(table_name, engine, if_exists=mode, index=False)
                logger.info(f"Successfully loaded data to {table_name}")
            except ImportError:
                logger.error("sqlalchemy not installed. Install with: pip install sqlalchemy")
                raise
        except Exception as e:
            logger.error(f"Failed to load data to PostgreSQL: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")


class MySQLLoader(DatabaseLoader):
    """MySQL database loader."""
    
    def __init__(self, host: str, port: int, database: str, 
                 user: str, password: str):
        """
        Initialize MySQL loader with connection parameters.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        logger.info(f"Initialized MySQL loader for {host}:{port}/{database}")
    
    def connect(self) -> None:
        """Establish connection to MySQL."""
        try:
            import mysql.connector
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info("Successfully connected to MySQL")
        except ImportError:
            logger.error("mysql-connector-python not installed. Install with: pip install mysql-connector-python")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {str(e)}")
            raise
    
    def load_data(self, data: Union['spark_sql.DataFrame', 'pd.DataFrame'], 
                  table_name: str, mode: str = "replace", **kwargs) -> None:
        """
        Load data to MySQL table.
        
        Args:
            data: Spark or Pandas DataFrame
            table_name: Target table name
            mode: Load mode (replace, append)
            **kwargs: Additional parameters
        """
        try:
            if not self.connection:
                self.connect()
            
            # Convert Spark DataFrame to Pandas if necessary
            if SPARK_AVAILABLE and isinstance(data, spark_sql.DataFrame):
                logger.info(f"Converting Spark DataFrame to Pandas for loading to {table_name}")
                data = data.toPandas()
            
            if not PANDAS_AVAILABLE:
                raise ImportError("pandas not installed. Install with: pip install pandas")
            
            # Use sqlalchemy for loading
            try:
                from sqlalchemy import create_engine
                connection_string = (
                    f"mysql+mysqlconnector://{self.user}:{self.password}@"
                    f"{self.host}:{self.port}/{self.database}"
                )
                engine = create_engine(connection_string)
                
                logger.info(f"Loading {len(data)} rows to table '{table_name}'")
                data.to_sql(table_name, engine, if_exists=mode, index=False)
                logger.info(f"Successfully loaded data to {table_name}")
            except ImportError:
                logger.error("sqlalchemy not installed. Install with: pip install sqlalchemy")
                raise
        except Exception as e:
            logger.error(f"Failed to load data to MySQL: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close MySQL connection."""
        if self.connection:
            self.connection.close()
            logger.info("MySQL connection closed")


class DatabaseLoaderFactory:
    """Factory for creating database loader instances."""
    
    _loaders: Dict[str, type] = {
        'postgresql': PostgreSQLLoader,
        'postgres': PostgreSQLLoader,
        'mysql': MySQLLoader,
    }
    
    @classmethod
    def create_loader(cls, db_type: str, **config) -> DatabaseLoader:
        """
        Create a database loader instance.
        
        Args:
            db_type: Type of database (postgresql, mysql, etc.)
            **config: Database configuration parameters
            
        Returns:
            DatabaseLoader instance
            
        Raises:
            ValueError: If database type is not supported
        """
        db_type_lower = db_type.lower()
        if db_type_lower not in cls._loaders:
            raise ValueError(
                f"Unsupported database type: {db_type}. "
                f"Supported types: {', '.join(cls._loaders.keys())}"
            )
        
        logger.info(f"Creating {db_type} database loader")
        return cls._loaders[db_type_lower](**config)


def load_credentials_from_env(db_type: str) -> Dict[str, str]:
    """
    Load database credentials from environment variables.
    
    Args:
        db_type: Type of database (postgresql, mysql, etc.)
        
    Returns:
        Dictionary with database credentials
        
    Raises:
        ValueError: If required environment variables are missing
    """
    required_vars = ['HOST', 'PORT', 'DATABASE', 'USER', 'PASSWORD']
    prefix = f"DB_{db_type.upper()}_"
    
    credentials = {}
    missing_vars = []
    
    for var in required_vars:
        env_var = f"{prefix}{var}"
        value = os.getenv(env_var)
        if not value:
            missing_vars.append(env_var)
        else:
            credentials[var.lower()] = value
    
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}\n"
            f"Expected format: {prefix}HOST, {prefix}PORT, {prefix}DATABASE, etc."
        )
    
    # Convert port to integer
    try:
        credentials['port'] = int(credentials['port'])
    except ValueError:
        raise ValueError(f"PORT must be an integer, got: {credentials['port']}")
    
    logger.info(f"Loaded credentials for {db_type} database from environment")
    return credentials


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Load data to various databases using Spark or Pandas DataFrames",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load from Parquet file to PostgreSQL
  python load_to_db.py --db-type postgresql --source data.parquet --table my_table
  
  # Load from CSV file to MySQL
  python load_to_db.py --db-type mysql --source data.csv --table my_table --format csv
  
Environment variables required (DB_<TYPE>_<PARAM>):
  - DB_POSTGRESQL_HOST
  - DB_POSTGRESQL_PORT
  - DB_POSTGRESQL_DATABASE
  - DB_POSTGRESQL_USER
  - DB_POSTGRESQL_PASSWORD
        """
    )
    
    parser.add_argument(
        '--db-type',
        required=True,
        choices=['postgresql', 'mysql'],
        help='Type of database to load to'
    )
    parser.add_argument(
        '--source',
        required=True,
        type=str,
        help='Path to source data file (parquet, csv, etc.)'
    )
    parser.add_argument(
        '--table',
        required=True,
        type=str,
        help='Target table name'
    )
    parser.add_argument(
        '--format',
        default='parquet',
        choices=['parquet', 'csv', 'json'],
        help='Source data format (default: parquet)'
    )
    parser.add_argument(
        '--mode',
        default='replace',
        choices=['replace', 'append'],
        help='Load mode (default: replace)'
    )
    parser.add_argument(
        '--use-spark',
        action='store_true',
        help='Use Spark DataFrame instead of Pandas'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Update logging level if specified
    if args.log_level != 'INFO':
        for handler in logger.handlers:
            handler.setLevel(getattr(logging, args.log_level))
        logger.setLevel(getattr(logging, args.log_level))
    
    try:
        logger.info(f"Starting data load: {args.source} -> {args.table}")
        
        # Validate source file exists
        source_path = Path(args.source)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {args.source}")
        
        logger.info(f"Reading {args.format} file: {args.source}")
        
        # Load data using specified engine
        if args.use_spark and SPARK_AVAILABLE:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("DataLoader").getOrCreate()
            
            if args.format == 'parquet':
                data = spark.read.parquet(args.source)
            elif args.format == 'csv':
                data = spark.read.csv(args.source, header=True, inferSchema=True)
            elif args.format == 'json':
                data = spark.read.json(args.source)
            else:
                raise ValueError(f"Unsupported format: {args.format}")
        else:
            if args.use_spark and not SPARK_AVAILABLE:
                logger.warning("Spark not available, falling back to Pandas")
            
            if not PANDAS_AVAILABLE:
                raise ImportError("pandas not installed. Install with: pip install pandas")
            
            if args.format == 'parquet':
                data = pd.read_parquet(args.source)
            elif args.format == 'csv':
                data = pd.read_csv(args.source)
            elif args.format == 'json':
                data = pd.read_json(args.source)
            else:
                raise ValueError(f"Unsupported format: {args.format}")
        
        logger.info(f"Loaded data with {len(data)} rows")
        
        # Load credentials and create loader
        credentials = load_credentials_from_env(args.db_type)
        loader = DatabaseLoaderFactory.create_loader(args.db_type, **credentials)
        
        # Load data to database
        loader.load_data(data, args.table, mode=args.mode)
        loader.disconnect()
        
        logger.info("Data load completed successfully")
        
    except Exception as e:
        logger.error(f"Data load failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
