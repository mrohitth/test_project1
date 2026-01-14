"""
Data loading module for pipeline processing.

This module provides functions to load and preprocess data from various sources.
It can be used both as a library and as a command-line tool.
"""

import logging
import argparse
from pathlib import Path
from typing import Optional, Union
import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_logger(log_level: str = 'INFO') -> None:
    """
    Configure the logging level for the module.
    
    Args:
        log_level: Logging level as a string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.info(f"Logging level set to {log_level}")


def validate_file_path(file_path: Union[str, Path]) -> Path:
    """
    Validate that a file path exists and is readable.
    
    Args:
        file_path: Path to the file to validate
        
    Returns:
        Path object if valid
        
    Raises:
        FileNotFoundError: If file doesn't exist
        IsADirectoryError: If path is a directory
    """
    path = Path(file_path)
    
    if not path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if path.is_dir():
        logger.error(f"Path is a directory, not a file: {file_path}")
        raise IsADirectoryError(f"Path is a directory, not a file: {file_path}")
    
    logger.debug(f"File path validated: {file_path}")
    return path


def load_csv_data(
    file_path: Union[str, Path],
    delimiter: str = ',',
    encoding: str = 'utf-8',
    **kwargs
) -> pd.DataFrame:
    """
    Load data from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        delimiter: Delimiter used in the CSV file
        encoding: File encoding
        **kwargs: Additional arguments to pass to pd.read_csv()
        
    Returns:
        DataFrame containing the loaded data
    """
    try:
        path = validate_file_path(file_path)
        logger.info(f"Loading CSV data from: {path}")
        
        df = pd.read_csv(path, delimiter=delimiter, encoding=encoding, **kwargs)
        logger.info(f"Successfully loaded data with shape: {df.shape}")
        
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV data from {file_path}: {str(e)}")
        raise


def load_json_data(
    file_path: Union[str, Path],
    **kwargs
) -> Union[dict, list, pd.DataFrame]:
    """
    Load data from a JSON file.
    
    Args:
        file_path: Path to the JSON file
        **kwargs: Additional arguments to pass to pd.read_json() or json.load()
        
    Returns:
        Loaded JSON data (dict, list, or DataFrame depending on content)
    """
    try:
        path = validate_file_path(file_path)
        logger.info(f"Loading JSON data from: {path}")
        
        # Try loading as DataFrame first, fall back to json if it fails
        try:
            data = pd.read_json(path, **kwargs)
            logger.info(f"Loaded JSON as DataFrame with shape: {data.shape}")
        except Exception:
            import json
            with open(path, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded JSON data as native Python object")
        
        return data
    except Exception as e:
        logger.error(f"Failed to load JSON data from {file_path}: {str(e)}")
        raise


def load_data(
    file_path: Union[str, Path],
    file_type: Optional[str] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Load data from a file, automatically detecting type if not specified.
    
    Args:
        file_path: Path to the data file
        file_type: Type of file ('csv', 'json', etc.). Auto-detected if None
        **kwargs: Additional arguments passed to the specific loader function
        
    Returns:
        DataFrame containing the loaded data
    """
    path = Path(file_path)
    
    # Auto-detect file type if not specified
    if file_type is None:
        file_type = path.suffix.lstrip('.').lower()
        logger.debug(f"Auto-detected file type: {file_type}")
    
    file_type = file_type.lower()
    
    if file_type == 'csv':
        return load_csv_data(file_path, **kwargs)
    elif file_type == 'json':
        data = load_json_data(file_path, **kwargs)
        if isinstance(data, pd.DataFrame):
            return data
        else:
            logger.warning("JSON data is not a DataFrame, converting...")
            return pd.DataFrame(data)
    else:
        logger.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")


def preprocess_data(
    df: pd.DataFrame,
    remove_duplicates: bool = True,
    drop_missing: bool = False,
    missing_threshold: Optional[float] = None
) -> pd.DataFrame:
    """
    Preprocess the loaded data.
    
    Args:
        df: Input DataFrame
        remove_duplicates: Whether to remove duplicate rows
        drop_missing: Whether to drop rows with missing values
        missing_threshold: Drop columns where missing percentage exceeds this threshold (0-1)
        
    Returns:
        Preprocessed DataFrame
    """
    logger.info(f"Starting preprocessing on data with shape: {df.shape}")
    
    # Remove duplicates
    if remove_duplicates:
        initial_rows = len(df)
        df = df.drop_duplicates()
        removed = initial_rows - len(df)
        if removed > 0:
            logger.info(f"Removed {removed} duplicate rows")
    
    # Drop columns with too many missing values
    if missing_threshold is not None:
        initial_cols = len(df.columns)
        missing_pct = df.isnull().sum() / len(df)
        cols_to_drop = missing_pct[missing_pct > missing_threshold].index
        df = df.drop(columns=cols_to_drop)
        dropped = initial_cols - len(df.columns)
        if dropped > 0:
            logger.info(f"Dropped {dropped} columns with >{missing_threshold*100:.1f}% missing values")
    
    # Drop rows with missing values
    if drop_missing:
        initial_rows = len(df)
        df = df.dropna()
        removed = initial_rows - len(df)
        if removed > 0:
            logger.info(f"Removed {removed} rows with missing values")
    
    logger.info(f"Preprocessing complete. Final data shape: {df.shape}")
    return df


def save_data(
    df: pd.DataFrame,
    output_path: Union[str, Path],
    file_type: Optional[str] = None,
    **kwargs
) -> None:
    """
    Save DataFrame to a file.
    
    Args:
        df: DataFrame to save
        output_path: Path where to save the file
        file_type: Type of file ('csv', 'json', 'parquet'). Auto-detected if None
        **kwargs: Additional arguments passed to the save function
    """
    output_path = Path(output_path)
    
    # Auto-detect file type if not specified
    if file_type is None:
        file_type = output_path.suffix.lstrip('.').lower()
    
    file_type = file_type.lower()
    
    try:
        # Create parent directories if they don't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Saving data to: {output_path}")
        
        if file_type == 'csv':
            df.to_csv(output_path, index=False, **kwargs)
        elif file_type == 'json':
            df.to_json(output_path, **kwargs)
        elif file_type == 'parquet':
            df.to_parquet(output_path, **kwargs)
        else:
            logger.error(f"Unsupported output file type: {file_type}")
            raise ValueError(f"Unsupported output file type: {file_type}")
        
        logger.info(f"Successfully saved data with shape {df.shape} to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save data to {output_path}: {str(e)}")
        raise


def main(args: Optional[list] = None) -> None:
    """
    CLI entry point for the data loading module.
    
    Args:
        args: Command-line arguments. If None, uses sys.argv
    """
    parser = argparse.ArgumentParser(
        description='Load and preprocess data from various file formats'
    )
    parser.add_argument(
        'input_file',
        type=str,
        help='Path to the input data file'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Path to save the processed data (optional)'
    )
    parser.add_argument(
        '-t', '--type',
        type=str,
        choices=['csv', 'json', 'parquet'],
        help='File type (auto-detected if not specified)'
    )
    parser.add_argument(
        '--remove-duplicates',
        action='store_true',
        default=True,
        help='Remove duplicate rows (default: True)'
    )
    parser.add_argument(
        '--no-remove-duplicates',
        dest='remove_duplicates',
        action='store_false',
        help='Keep duplicate rows'
    )
    parser.add_argument(
        '--drop-missing',
        action='store_true',
        help='Drop rows with missing values'
    )
    parser.add_argument(
        '--missing-threshold',
        type=float,
        default=None,
        help='Drop columns where missing percentage exceeds this threshold (0-1)'
    )
    parser.add_argument(
        '-l', '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parsed_args = parser.parse_args(args)
    
    # Setup logging
    setup_logger(parsed_args.log_level)
    
    try:
        # Load data
        logger.info(f"Loading data from: {parsed_args.input_file}")
        df = load_data(parsed_args.input_file, file_type=parsed_args.type)
        
        # Preprocess data
        logger.info("Preprocessing data...")
        df = preprocess_data(
            df,
            remove_duplicates=parsed_args.remove_duplicates,
            drop_missing=parsed_args.drop_missing,
            missing_threshold=parsed_args.missing_threshold
        )
        
        # Save processed data if output path is specified
        if parsed_args.output:
            save_data(df, parsed_args.output, file_type=parsed_args.type)
            logger.info(f"Data processing complete. Output saved to: {parsed_args.output}")
        else:
            logger.info(f"Data processing complete. Shape: {df.shape}")
            logger.info("No output path specified. Data not saved.")
        
    except Exception as e:
        logger.error(f"Error during data processing: {str(e)}")
        raise


if __name__ == '__main__':
    main()
