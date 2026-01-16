"""
ETL Pipeline Source Package
===========================

This package contains the core ETL pipeline modules:
- load_data: Data extraction from various sources including AWS S3
- transform_data: PySpark-based data transformations
- load_to_db: Database loading (DuckDB, PostgreSQL)
- run_pipeline: Pipeline orchestration
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