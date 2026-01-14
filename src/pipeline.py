#!/usr/bin/env python3
"""
Unified ETL Pipeline Runner

Provides a single entry point to orchestrate the complete ETL pipeline: 
1. Load CSV data into Spark DataFrame
2. Transform and clean data
3. Load transformed data into DuckDB or PostgreSQL

Features:
- Configuration-driven execution (no hardcoded paths or credentials)
- CLI argument parsing via argparse
- Comprehensive logging with row counts and timing
- Support for DuckDB and PostgreSQL backends
- Optional data scaling simulation for testing
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame

from load_data import load_data, get_project_root
from transform_data import DataTransformationPipeline, create_spark_session
from load_to_db import DatabaseLoader


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineExecutor:
    """
    Orchestrates the complete ETL pipeline with configuration-driven execution.
    
    Supports: 
    - Loading CSV files into Spark DataFrames
    - Optional data scaling simulation
    - Transformation via PySpark
    - Loading to DuckDB or PostgreSQL
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the pipeline executor. 
        
        Args:
            config: Configuration dictionary with keys:
                - input_path: Path to input CSV file (absolute or relative to project root)
                - db_type: Database type ('duckdb' or 'postgres')
                - duckdb_path: Path to DuckDB database file (if db_type='duckdb')
                - postgres_config: Dict with PostgreSQL connection params (if db_type='postgres')
                - simulate_scale: Boolean to simulate scaling (duplicates data)
                - scale_factor: Integer multiplier for scaling simulation (default: 2)
        """
        self.config = config
        self.spark = None
        self.metrics = {
            'load_csv': {'rows': 0, 'duration_seconds': 0.0},
            'transform': {'rows_input': 0, 'rows_output': 0, 'duration_seconds': 0.0},
            'load_db': {'rows': 0, 'duration_seconds': 0.0, 'status': 'PENDING'}
        }
        logger.info(f"Pipeline executor initialized with config: {config}")
    
    def _resolve_input_path(self) -> Path:
        """
        Resolve input path (absolute or relative to project root).
        
        Returns:
            Path: The absolute path to the input file. 
            
        Raises:
            FileNotFoundError: If the input file does not exist.
        """
        input_path = self.config.get('input_path')
        if not input_path:
            raise ValueError("input_path is required in config")
        
        path_obj = Path(input_path)
        
        # If not absolute, resolve from project root
        if not path_obj.is_absolute():
            project_root = get_project_root()
            path_obj = project_root / input_path
        
        if not path_obj.exists():
            raise FileNotFoundError(f"Input file not found: {path_obj}")
        
        logger.info(f"Input path resolved to: {path_obj}")
        return path_obj
    
    def _load_csv_to_spark(self) -> DataFrame:
        """
        Load CSV file into Spark DataFrame.
        
        Returns:
            DataFrame: Spark DataFrame containing the CSV data. 
        """
        logger.info("=" * 80)
        logger.info("STEP 1: LOAD CSV TO SPARK")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        try:
            # Create Spark session if not exists
            if self.spark is None:
                self.spark = create_spark_session(app_name="UnifiedETLPipeline")
            
            # Resolve input path
            input_path = self._resolve_input_path()
            
            # Load CSV with inferred schema
            logger.info(f"Loading CSV from: {input_path}")
            df = self.spark.read.option("header", "true") \
                                .option("inferSchema", "true") \
                                .csv(str(input_path))
            
            row_count = df.count()
            duration = time.time() - start_time
            
            self.metrics['load_csv']['rows'] = row_count
            self.metrics['load_csv']['duration_seconds'] = duration
            
            logger.info(f"CSV loaded successfully")
            logger.info(f"  Rows: {row_count}")
            logger.info(f"  Columns: {len(df.columns)}")
            logger.info(f"  Duration: {duration:.2f}s")
            logger.info(f"  Schema: {df.schema}")
            
            return df
        
        except Exception as e: 
            duration = time.time() - start_time
            self.metrics['load_csv']['duration_seconds'] = duration
            logger.error(f"Failed to load CSV: {str(e)}", exc_info=True)
            raise
    
    def _simulate_scale(self, df: DataFrame) -> DataFrame:
        """
        Simulate scaling by duplicating data.
        
        Args:
            df: Input DataFrame
            
        Returns: 
            DataFrame: Scaled DataFrame
        """
        if not self.config.get('simulate_scale', False):
            return df
        
        logger.info("=" * 80)
        logger.info("STEP 1.5: SIMULATE DATA SCALING")
        logger.info("=" * 80)
        
        start_time = time.time()
        scale_factor = self.config.get('scale_factor', 2)
        
        logger.info(f"Scaling data by factor of {scale_factor}")
        
        # Duplicate the dataframe scale_factor times
        scaled_df = df
        for i in range(scale_factor - 1):
            scaled_df = scaled_df.union(df)
        
        original_count = df.count()
        scaled_count = scaled_df.count()
        duration = time.time() - start_time
        
        logger.info(f"Data scaled successfully")
        logger.info(f"  Original rows: {original_count}")
        logger.info(f"  Scaled rows: {scaled_count}")
        logger.info(f"  Duration: {duration:.2f}s")
        
        return scaled_df
    
    def _transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to the data.
        
        Args:
            df: Input DataFrame
            
        Returns: 
            DataFrame: Transformed DataFrame
        """
        logger.info("=" * 80)
        logger.info("STEP 2: TRANSFORM DATA")
        logger.info("=" * 80)
        
        start_time = time.time()
        input_count = df.count()
        
        try:
            # Initialize transformation pipeline
            transformer = DataTransformationPipeline(self.spark, log_level="INFO")
            
            # Apply transformations in sequence
            transformed_df = df
            
            # 1. Remove duplicates
            logger.info("Removing duplicates...")
            transformed_df = transformer.remove_duplicates(transformed_df)
            
            # 2. Handle missing values (drop rows with nulls)
            logger.info("Handling missing values...")
            transformed_df = transformer.handle_missing_values(
                transformed_df,
                strategy="drop"
            )
            
            # 3. Clean string columns (trim, lowercase)
            logger.info("Cleaning string columns...")
            transformed_df = transformer.clean_string_columns(
                transformed_df,
                lowercase=True,
                trim_whitespace=True
            )
            
            output_count = transformed_df.count()
            duration = time.time() - start_time
            
            self.metrics['transform']['rows_input'] = input_count
            self.metrics['transform']['rows_output'] = output_count
            self.metrics['transform']['duration_seconds'] = duration
            
            logger.info(f"Data transformation completed")
            logger.info(f"  Input rows: {input_count}")
            logger.info(f"  Output rows: {output_count}")
            logger.info(f"  Rows removed: {input_count - output_count}")
            logger.info(f"  Duration: {duration:.2f}s")
            
            return transformed_df
        
        except Exception as e: 
            duration = time.time() - start_time
            self.metrics['transform']['duration_seconds'] = duration
            logger.error(f"Failed to transform data: {str(e)}", exc_info=True)
            raise
    
    def _load_to_db(self, df: DataFrame) -> Dict[str, Any]:
        """
        Load transformed DataFrame into database.
        
        Args:
            df: DataFrame to load
            
        Returns: 
            Dict: Load statistics
        """
        logger.info("=" * 80)
        logger.info("STEP 3: LOAD TO DATABASE")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        try:
            db_type = self.config.get('db_type', 'duckdb').lower()
            
            # Determine connection parameters based on db_type
            if db_type == 'duckdb': 
                db_path = self.config.get('duckdb_path', 'data/output.db')
                # Resolve relative paths from project root
                if not Path(db_path).is_absolute():
                    db_path = str(get_project_root() / db_path)
                
                connection_params = {'database': db_path}
                logger.info(f"Using DuckDB at: {db_path}")
            
            elif db_type == 'postgres' or db_type == 'postgresql':
                connection_params = self.config.get('postgres_config', {})
                if not connection_params: 
                    raise ValueError("postgres_config required when db_type='postgres'")
                logger.info(f"Using PostgreSQL: {connection_params.get('database')}@{connection_params.get('host')}")
            
            else:
                raise ValueError(f"Unsupported db_type: {db_type}")
            
            # Initialize database loader
            loader = DatabaseLoader(db_type, connection_params)
            
            # Load the DataFrame
            table_name = self.config.get('table_name', 'transformed_data')
            logger.info(f"Loading data into table: {table_name}")
            
            load_stats = loader.load_dataframe(
                df,
                table_name=table_name,
                mode='overwrite',
                verify=True
            )
            
            # Close connection
            loader.close()
            
            duration = time.time() - start_time
            self.metrics['load_db']['rows'] = load_stats.get('output_row_count', 0)
            self.metrics['load_db']['duration_seconds'] = duration
            self.metrics['load_db']['status'] = load_stats.get('status', 'UNKNOWN')
            
            logger.info(f"Data loaded to database")
            logger.info(f"  Rows loaded: {load_stats.get('output_row_count', 0)}")
            logger.info(f"  Status: {load_stats.get('status')}")
            logger.info(f"  Duration: {duration:.2f}s")
            
            return load_stats
        
        except Exception as e: 
            duration = time.time() - start_time
            self.metrics['load_db']['duration_seconds'] = duration
            self.metrics['load_db']['status'] = 'FAILED'
            logger.error(f"Failed to load data to database: {str(e)}", exc_info=True)
            raise
    
    def run(self) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            bool: True if pipeline succeeded, False otherwise. 
        """
        pipeline_start = time.time()
        logger.info("\n" + "=" * 80)
        logger.info("STARTING UNIFIED ETL PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Configuration: {json.dumps(self.config, indent=2)}")
        
        try:
            # Step 1: Load CSV to Spark
            df = self._load_csv_to_spark()
            
            # Step 1.5: Optional scaling simulation
            df = self._simulate_scale(df)
            
            # Step 2: Transform data
            df = self._transform_data(df)
            
            # Step 3: Load to database
            load_stats = self._load_to_db(df)
            
            # Close Spark session
            if self.spark:
                self.spark.stop()
            
            total_duration = time.time() - pipeline_start
            
            # Log final summary
            logger.info("\n" + "=" * 80)
            logger.info("PIPELINE EXECUTION SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Status: SUCCESS")
            logger.info(f"Total Duration: {total_duration:.2f}s")
            logger.info(f"\nStep Metrics:")
            logger.info(f"  1. Load CSV: {self.metrics['load_csv']['rows']} rows in {self.metrics['load_csv']['duration_seconds']:.2f}s")
            logger.info(f"  2. Transform: {self.metrics['transform']['rows_input']} → {self.metrics['transform']['rows_output']} rows in {self.metrics['transform']['duration_seconds']:.2f}s")
            logger.info(f"  3. Load DB: {self.metrics['load_db']['rows']} rows in {self.metrics['load_db']['duration_seconds']:.2f}s ({self.metrics['load_db']['status']})")
            logger.info("=" * 80 + "\n")
            
            return True
        
        except Exception as e: 
            total_duration = time.time() - pipeline_start
            logger.error("\n" + "=" * 80)
            logger.error("PIPELINE EXECUTION FAILED")
            logger.error("=" * 80)
            logger.error(f"Error: {str(e)}")
            logger.error(f"Total Duration: {total_duration:.2f}s")
            logger.error("=" * 80 + "\n")
            
            # Close Spark session on error
            if self.spark:
                try:
                    self.spark.stop()
                except:
                    pass
            
            return False


def run_pipeline(config: Dict[str, Any]) -> bool:
    """
    Execute the ETL pipeline with the provided configuration.
    
    This is the main entry point for programmatic pipeline execution.
    
    Args:
        config: Configuration dictionary with keys:
            - input_path: Path to input CSV file
            - db_type: Database type ('duckdb' or 'postgres')
            - duckdb_path: Path to DuckDB file (if db_type='duckdb')
            - postgres_config: PostgreSQL connection params (if db_type='postgres')
            - simulate_scale: Boolean to simulate scaling
            - scale_factor: Integer scale multiplier (default: 2)
            - table_name: Target table name (default: 'transformed_data')
    
    Returns:
        bool: True if pipeline succeeded, False otherwise.
        
    Example:
        config = {
            'input_path': 'data/raw/input.csv',
            'db_type': 'duckdb',
            'duckdb_path': 'data/output.db',
            'simulate_scale': False,
            'table_name': 'events'
        }
        success = run_pipeline(config)
    """
    executor = PipelineExecutor(config)
    return executor.run()


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments. 
    
    Returns:
        Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Unified ETL Pipeline: Load CSV → Transform → Load to Database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load CSV to DuckDB
  python pipeline.py --input_path data/raw/input.csv --db_type duckdb --duckdb_path data/output.db
  
  # Load CSV to PostgreSQL
  python pipeline.py --input_path data/raw/input.csv --db_type postgres \\
    --postgres_host localhost --postgres_user admin --postgres_password secret \\
    --postgres_database mydb --postgres_port 5432
  
  # Simulate data scaling (2x duplication)
  python pipeline.py --input_path data/raw/input.csv --db_type duckdb \\
    --duckdb_path data/output.db --simulate_scale --scale_factor 2
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--input_path',
        type=str,
        required=True,
        help='Path to input CSV file (absolute or relative to project root)'
    )
    
    parser.add_argument(
        '--db_type',
        type=str,
        choices=['duckdb', 'postgres'],
        default='duckdb',
        help='Database type (default: duckdb)'
    )
    
    # DuckDB arguments
    parser.add_argument(
        '--duckdb_path',
        type=str,
        default='data/output.db',
        help='Path to DuckDB database file (default: data/output.db)'
    )
    
    # PostgreSQL arguments
    parser.add_argument(
        '--postgres_host',
        type=str,
        default='localhost',
        help='PostgreSQL host (default: localhost)'
    )
    
    parser.add_argument(
        '--postgres_port',
        type=int,
        default=5432,
        help='PostgreSQL port (default: 5432)'
    )
    
    parser.add_argument(
        '--postgres_user',
        type=str,
        help='PostgreSQL username (required if db_type=postgres)'
    )
    
    parser.add_argument(
        '--postgres_password',
        type=str,
        help='PostgreSQL password (required if db_type=postgres)'
    )
    
    parser.add_argument(
        '--postgres_database',
        type=str,
        help='PostgreSQL database name (required if db_type=postgres)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--table_name',
        type=str,
        default='transformed_data',
        help='Target table name (default: transformed_data)'
    )
    
    parser.add_argument(
        '--simulate_scale',
        action='store_true',
        help='Enable data scaling simulation (duplicates data)'
    )
    
    parser.add_argument(
        '--scale_factor',
        type=int,
        default=2,
        help='Data duplication factor for scaling simulation (default: 2)'
    )
    
    return parser.parse_args()


def main():
    """
    Main entry point for CLI execution.
    """
    args = parse_arguments()
    
    # Build configuration from arguments
    config = {
        'input_path': args.input_path,
        'db_type': args.db_type,
        'table_name': args.table_name,
        'simulate_scale': args.simulate_scale,
        'scale_factor': args.scale_factor,
    }
    
    # Add database-specific configuration
    if args.db_type == 'duckdb':
        config['duckdb_path'] = args.duckdb_path
    
    elif args.db_type == 'postgres':
        # Validate required PostgreSQL arguments
        if not args.postgres_user or not args.postgres_password or not args.postgres_database:
            print(
                "Error: PostgreSQL requires --postgres_user, --postgres_password, "
                "and --postgres_database",
                file=sys.stderr
            )
            sys.exit(1)
        
        config['postgres_config'] = {
            'host': args.postgres_host,
            'port': args.postgres_port,
            'user': args.postgres_user,
            'password': args.postgres_password,
            'database': args.postgres_database
        }
    
    # Run pipeline
    try:
        success = run_pipeline(config)
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Pipeline failed with exception: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__': 
    main()