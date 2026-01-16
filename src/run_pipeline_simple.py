#!/usr/bin/env python3
"""
Simple ETL Pipeline Orchestrator (Pandas Version)
Executes a complete ETL pipeline using pandas-based transformations.

This is a simplified version that works without PySpark, suitable for:
- Testing and development
- Google Colab environments
- Small to medium datasets

Author: mrohitth  
Date: 2026-01-16
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import traceback

import pandas as pd
import numpy as np

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent))

from load_data import load_data, validate_data
from transform_data_pandas import PandasDataTransformer
from load_to_db_simple import SimpleDatabaseLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SimplePipelineOrchestrator:
    """Orchestrates the complete ETL pipeline execution using pandas."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            config: Configuration dictionary with pipeline parameters
        """
        self.config = config or {}
        self.project_root = Path(__file__).parent.parent
        self.logs_dir = self.project_root / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        
        # Pipeline state
        self.pipeline_id = f"pipeline_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        self.pipeline_start_time = datetime.utcnow()
        self.metrics = {
            'pipeline_id': self.pipeline_id,
            'stages': []
        }
        
        logger.info(f"SimplePipelineOrchestrator initialized: {self.pipeline_id}")
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            Dictionary with pipeline execution metrics
        """
        logger.info("=" * 80)
        logger.info("STARTING SIMPLE ETL PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Pipeline ID: {self.pipeline_id}")
        logger.info(f"Start Time: {self.pipeline_start_time.isoformat()}")
        
        all_successful = True
        
        try:
            # Stage 1: Load Data
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 1: DATA LOADING")
            logger.info("=" * 80)
            
            load_start = time.time()
            df = self._load_data_stage()
            load_duration = time.time() - load_start
            
            if df is None or df.empty:
                raise ValueError("Data loading failed or returned empty DataFrame")
            
            self.metrics['stages'].append({
                'stage': 'data_loading',
                'duration_seconds': load_duration,
                'input_rows': 0,
                'output_rows': len(df),
                'status': 'success'
            })
            
            logger.info(f"✅ Data loading completed in {load_duration:.2f}s")
            logger.info(f"   Loaded {len(df)} rows, {len(df.columns)} columns")
            
            # Stage 2: Transform Data
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 2: DATA TRANSFORMATION")
            logger.info("=" * 80)
            
            transform_start = time.time()
            df_transformed = self._transform_data_stage(df)
            transform_duration = time.time() - transform_start
            
            if df_transformed is None or df_transformed.empty:
                raise ValueError("Data transformation failed or returned empty DataFrame")
            
            self.metrics['stages'].append({
                'stage': 'data_transformation',
                'duration_seconds': transform_duration,
                'input_rows': len(df),
                'output_rows': len(df_transformed),
                'rows_removed': len(df) - len(df_transformed),
                'status': 'success'
            })
            
            logger.info(f"✅ Data transformation completed in {transform_duration:.2f}s")
            logger.info(f"   Input: {len(df)} rows, Output: {len(df_transformed)} rows")
            logger.info(f"   Rows removed: {len(df) - len(df_transformed)}")
            
            # Stage 3: Load to Database
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 3: DATABASE LOADING")
            logger.info("=" * 80)
            
            db_start = time.time()
            db_stats = self._load_to_database_stage(df_transformed)
            db_duration = time.time() - db_start
            
            self.metrics['stages'].append({
                'stage': 'database_loading',
                'duration_seconds': db_duration,
                'input_rows': len(df_transformed),
                'output_rows': db_stats.get('output_row_count', 0),
                'status': db_stats.get('status', 'unknown').lower()
            })
            
            logger.info(f"✅ Database loading completed in {db_duration:.2f}s")
            logger.info(f"   Loaded {db_stats.get('output_row_count', 0)} rows to database")
            
        except Exception as e:
            logger.error(f"Pipeline execution error: {e}\n{traceback.format_exc()}")
            all_successful = False
            self.metrics['error'] = str(e)
        
        finally:
            # Generate final report
            self._generate_final_report(all_successful)
        
        return self.metrics
    
    def _load_data_stage(self) -> pd.DataFrame:
        """Execute the data loading stage."""
        try:
            # Get data source from config
            data_source = self.config.get('data_source', 'data/raw/sample_data.csv')
            source_type = self.config.get('source_type', 'local')
            
            logger.info(f"Loading data from: {data_source} (type: {source_type})")
            
            # Load data
            df = load_data(data_source, source_type=source_type)
            
            # Validate data
            if not validate_data(df):
                logger.warning("Data validation returned warnings")
            
            return df
        
        except Exception as e:
            logger.error(f"Data loading stage failed: {str(e)}")
            raise
    
    def _transform_data_stage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the data transformation stage."""
        try:
            logger.info("Initializing data transformer...")
            transformer = PandasDataTransformer()
            
            # Apply transformations
            logger.info("Removing duplicates...")
            df_clean = transformer.remove_duplicates(df)
            
            logger.info("Handling missing values...")
            df_clean = transformer.handle_missing_values(df_clean, strategy='median')
            
            # Validate data ranges (if applicable)
            if 'passenger_count' in df_clean.columns:
                logger.info("Validating data ranges...")
                df_clean = df_clean[
                    (df_clean['passenger_count'] > 0) &
                    (df_clean['passenger_count'] <= 8)
                ]
            
            # Add date features if timestamp column exists
            timestamp_cols = df_clean.select_dtypes(include=['datetime64']).columns
            if len(timestamp_cols) > 0:
                logger.info(f"Creating date features from: {timestamp_cols[0]}")
                df_clean = transformer.create_date_features(df_clean, timestamp_cols[0])
            
            # Generate quality report
            logger.info("Generating data quality report...")
            quality_report = transformer.get_data_quality_report(df_clean)
            logger.info(f"Quality report - Rows: {quality_report['total_rows']}, "
                       f"Duplicates: {quality_report['duplicate_rows']}")
            
            return df_clean
        
        except Exception as e:
            logger.error(f"Data transformation stage failed: {str(e)}")
            raise
    
    def _load_to_database_stage(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Execute the database loading stage."""
        try:
            # Get database config
            db_type = self.config.get('db_type', 'duckdb')
            db_params = self.config.get('db_params', {'database': 'pipeline_data.db'})
            table_name = self.config.get('table_name', 'processed_data')
            
            logger.info(f"Initializing {db_type} database loader...")
            db_loader = SimpleDatabaseLoader(db_type, db_params)
            
            logger.info(f"Loading data to table: {table_name}")
            stats = db_loader.load_dataframe(df, table_name, mode='overwrite', verify=True)
            
            logger.info(f"Database load status: {stats['status']}")
            
            db_loader.close()
            
            return stats
        
        except Exception as e:
            logger.error(f"Database loading stage failed: {str(e)}")
            # Return error stats
            return {
                'status': 'FAILED',
                'message': str(e),
                'output_row_count': 0
            }
    
    def _generate_final_report(self, success: bool):
        """Generate comprehensive pipeline execution report."""
        pipeline_end = datetime.utcnow()
        total_duration = (pipeline_end - self.pipeline_start_time).total_seconds()
        
        # Update metrics
        self.metrics['start_time'] = self.pipeline_start_time.isoformat()
        self.metrics['end_time'] = pipeline_end.isoformat()
        self.metrics['total_duration_seconds'] = total_duration
        self.metrics['overall_status'] = 'success' if success else 'failed'
        
        # Calculate totals
        total_rows_loaded = sum(s.get('output_rows', 0) for s in self.metrics['stages'] 
                               if s.get('stage') == 'data_loading')
        total_rows_transformed = sum(s.get('output_rows', 0) for s in self.metrics['stages'] 
                                    if s.get('stage') == 'data_transformation')
        total_rows_db = sum(s.get('output_rows', 0) for s in self.metrics['stages'] 
                           if s.get('stage') == 'database_loading')
        
        self.metrics['total_rows_loaded'] = total_rows_loaded
        self.metrics['total_rows_transformed'] = total_rows_transformed
        self.metrics['total_rows_loaded_to_db'] = total_rows_db
        
        # Write JSON report
        report_file = self.logs_dir / f"{self.pipeline_id}_report.json"
        with open(report_file, 'w') as f:
            json.dump(self.metrics, f, indent=2)
        
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Pipeline ID: {self.pipeline_id}")
        logger.info(f"Status: {self.metrics['overall_status'].upper()}")
        logger.info(f"Total Duration: {total_duration:.2f} seconds")
        logger.info(f"Total Rows Loaded: {total_rows_loaded:,}")
        logger.info(f"Total Rows Transformed: {total_rows_transformed:,}")
        logger.info(f"Total Rows Loaded to DB: {total_rows_db:,}")
        logger.info(f"\nReport saved to: {report_file}")
        logger.info("=" * 80)
        
        # Print JSON to stdout for CI/CD integration
        print("\nPIPELINE_METRICS_JSON:")
        print(json.dumps(self.metrics, indent=2))


def main():
    """Main entry point for the simple pipeline orchestrator."""
    try:
        # Default configuration
        config = {
            'data_source': 'data/raw/sample_data.csv',
            'source_type': 'local',
            'db_type': 'duckdb',
            'db_params': {'database': 'pipeline_data.db'},
            'table_name': 'processed_data'
        }
        
        orchestrator = SimplePipelineOrchestrator(config)
        metrics = orchestrator.run()
        
        # Exit with appropriate code
        sys.exit(0 if metrics.get('overall_status') == 'success' else 1)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
