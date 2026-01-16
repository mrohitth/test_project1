"""
test_project1 ETL Pipeline Package
==================================

This package provides a comprehensive ETL pipeline with:
- Data loading from various sources (CSV, JSON, S3)
- PySpark-based data transformations
- Database loading (DuckDB, PostgreSQL)
- Pipeline orchestration and monitoring

Modules:
- load_data: Data extraction utilities
- transform_data: PySpark transformation pipeline
- load_to_db: Database loading utilities
- run_pipeline: Pipeline orchestration
- pipeline: Core pipeline class
"""

from .load_data import load_data, validate_data, get_project_root, get_data_path
from .transform_data import DataTransformationPipeline, create_spark_session

__version__ = "1.0.0"
__author__ = "mrohitth"

__all__ = [
    "load_data",
    "validate_data", 
    "get_project_root",
    "get_data_path",
    "DataTransformationPipeline",
    "create_spark_session",
]