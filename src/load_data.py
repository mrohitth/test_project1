"""
Data loading module for test_project1.

This module provides functionality to load and process data from various sources.
It supports both local execution and Colab environments through relative path handling.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """
    Get the project root directory.
    
    Works correctly in both local and Colab environments by finding the
    directory containing the src module.
    
    Returns:
        Path: The absolute path to the project root directory.
    """
    # Get the directory containing this file
    current_file = Path(__file__).resolve()
    # Go up one level from src/ to get project root
    project_root = current_file.parent.parent
    logger.debug(f"Project root detected at: {project_root}")
    return project_root


def get_data_path(relative_path: str) -> Path:
    """
    Get the absolute path to a data file using relative path from project root.
    
    Args:
        relative_path: Path relative to the project root (e.g., 'data/raw/file.csv')
    
    Returns:
        Path: The absolute path to the data file.
    """
    project_root = get_project_root()
    full_path = project_root / relative_path
    logger.debug(f"Data path resolved: {full_path}")
    return full_path


def load_data(file_path: str, source_type: str = 'local', **kwargs) -> Optional[Any]:
    """
    Load data from a file (local or S3).
    
    Supports multiple file formats (CSV, JSON, etc.) and handles errors gracefully.
    
    Args:
        file_path: Path to the data file (can be relative or absolute).
                  For S3: 's3://bucket/key' or provide bucket and key separately
                  For local: relative or absolute path
        source_type: 'local' or 's3'
        **kwargs: Additional arguments:
            - For S3: bucket, key, aws_access_key_id, aws_secret_access_key, region_name
            - For CSV: Any pd.read_csv arguments
    
    Returns:
        Optional[Any]: The loaded data, or None if loading failed.
    
    Raises:
        FileNotFoundError: If the specified file cannot be found.
    """
    try:
        # Handle S3 paths
        if source_type == 's3' or file_path.startswith('s3://'):
            from s3_loader import S3DataLoader
            
            # Parse S3 path
            if file_path.startswith('s3://'):
                # Extract bucket and key from s3:// URL
                s3_parts = file_path.replace('s3://', '').split('/', 1)
                bucket = s3_parts[0]
                key = s3_parts[1] if len(s3_parts) > 1 else ''
            else:
                bucket = kwargs.get('bucket')
                key = kwargs.get('key', file_path)
            
            if not bucket or not key:
                raise ValueError("For S3 sources, provide either 's3://bucket/key' or bucket and key parameters")
            
            logger.info(f"Loading data from S3: s3://{bucket}/{key}")
            
            # Initialize S3 loader
            s3_loader = S3DataLoader(
                aws_access_key_id=kwargs.get('aws_access_key_id'),
                aws_secret_access_key=kwargs.get('aws_secret_access_key'),
                region_name=kwargs.get('region_name', 'us-east-1')
            )
            
            # Determine file type from key
            file_extension = Path(key).suffix.lower()
            
            if file_extension == '.csv':
                data = s3_loader.load_csv_from_s3(bucket, key)
                logger.info(f"Successfully loaded CSV from S3 with shape: {data.shape}")
            elif file_extension == '.json':
                data = s3_loader.load_json_from_s3(bucket, key)
                logger.info(f"Successfully loaded JSON from S3")
            else:
                # Download file and load locally
                import tempfile
                with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as tmp:
                    tmp_path = tmp.name
                s3_loader.download_file(bucket, key, tmp_path)
                data = load_data(tmp_path, source_type='local')
                os.remove(tmp_path)
            
            return data
        
        # Handle local paths
        if not os.path.isabs(file_path):
            data_path = get_data_path(file_path)
        else:
            data_path = Path(file_path)
        
        if not data_path.exists():
            logger.error(f"File not found: {data_path}")
            raise FileNotFoundError(f"Data file not found at: {data_path}")
        
        logger.info(f"Loading data from: {data_path}")
        
        # Determine file type and load accordingly
        file_extension = data_path.suffix.lower()
        
        if file_extension == '.csv':
            import pandas as pd
            data = pd.read_csv(data_path)
            logger.info(f"Successfully loaded CSV file with shape: {data.shape}")
        elif file_extension == '.json':
            import json
            with open(data_path, 'r') as f:
                data = json.load(f)
            logger.info(f"Successfully loaded JSON file")
        elif file_extension in ['.pkl', '.pickle']:
            import pickle
            with open(data_path, 'rb') as f:
                data = pickle.load(f)
            logger.info(f"Successfully loaded pickle file")
        else:
            logger.warning(f"Unsupported file format: {file_extension}")
            data = None
        
        return data
    
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {str(e)}")
        raise


def validate_data(data: Any) -> bool:
    """
    Validate loaded data.
    
    Performs basic validation checks on the loaded data.
    
    Args:
        data: The data to validate.
    
    Returns:
        bool: True if data passes validation, False otherwise.
    """
    if data is None:
        logger.warning("Data is None")
        return False
    
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            if data.empty:
                logger.warning("DataFrame is empty")
                return False
            logger.info(f"DataFrame validation passed. Shape: {data.shape}")
            return True
    except ImportError:
        pass
    
    logger.info("Data validation passed")
    return True


def main(config: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    """
    Main function to load and validate data.
    
    Args:
        config: Optional configuration dictionary with keys:
               - 'data_path': Path to the data file (relative to project root)
               - 'validate': Whether to validate data (default: True)
    
    Returns:
        Optional[Any]: The loaded and validated data, or None if loading failed.
    """
    config = config or {}
    data_path = config.get('data_path', 'data/raw/data.csv')
    should_validate = config.get('validate', True)
    
    logger.info(f"Starting data loading process with config: {config}")
    
    try:
        # Load the data
        data = load_data(data_path)
        
        # Validate if requested
        if should_validate and not validate_data(data):
            logger.warning("Data validation failed")
            return None
        
        logger.info("Data loading process completed successfully")
        return data
    
    except Exception as e:
        logger.error(f"Data loading process failed: {str(e)}")
        return None


if __name__ == '__main__':
    """
    Example usage of the load_data module.
    """
    # Example 1: Load data with default configuration
    logger.info("=== Running load_data.py as main module ===")
    
    # Example configuration
    example_config = {
        'data_path': 'data/raw/example.csv',
        'validate': True
    }
    
    # Load data
    result = main(example_config)
    
    if result is not None:
        logger.info("✓ Data loaded successfully")
    else:
        logger.warning("✗ Failed to load data")
