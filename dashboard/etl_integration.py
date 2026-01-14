"""
ETL Pipeline Integration Module for Streamlit Dashboard
========================================================

This module provides ETL (Extract, Transform, Load) pipeline integration
for the Streamlit dashboard, enabling seamless data processing and
visualization workflows.

Created: 2026-01-14 04:26:30 UTC
Author: mrohitth
"""

import logging
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from datetime import datetime
import json
from pathlib import Path


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ETLException(Exception):
    """Custom exception for ETL pipeline errors."""
    pass


class DataSource(ABC):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to data source."""
        pass
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from source."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to data source."""
        pass


class CSVDataSource(DataSource):
    """Data source for CSV files."""
    
    def __init__(self, file_path: str):
        """Initialize CSV data source.
        
        Args:
            file_path: Path to CSV file
        """
        self.file_path = Path(file_path)
        self.connected = False
    
    def connect(self) -> None:
        """Verify CSV file exists."""
        if not self.file_path.exists():
            raise ETLException(f"CSV file not found: {self.file_path}")
        self.connected = True
        logger.info(f"Connected to CSV source: {self.file_path}")
    
    def extract(self) -> pd.DataFrame:
        """Extract data from CSV file."""
        if not self.connected:
            raise ETLException("Data source not connected. Call connect() first.")
        try:
            data = pd.read_csv(self.file_path)
            logger.info(f"Extracted {len(data)} rows from {self.file_path}")
            return data
        except Exception as e:
            raise ETLException(f"Failed to extract CSV data: {str(e)}")
    
    def disconnect(self) -> None:
        """Disconnect from CSV source."""
        self.connected = False
        logger.info("Disconnected from CSV source")


class DataTransformer:
    """Handles data transformation operations."""
    
    @staticmethod
    def remove_duplicates(data: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """Remove duplicate rows from dataframe.
        
        Args:
            data: Input dataframe
            subset: Column names to consider for duplicates
            
        Returns:
            Dataframe with duplicates removed
        """
        original_size = len(data)
        data = data.drop_duplicates(subset=subset)
        logger.info(f"Removed {original_size - len(data)} duplicate rows")
        return data
    
    @staticmethod
    def fill_missing_values(data: pd.DataFrame, strategy: str = 'mean', 
                           columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Fill missing values in dataframe.
        
        Args:
            data: Input dataframe
            strategy: Strategy for filling ('mean', 'median', 'ffill', 'bfill')
            columns: Specific columns to fill (None for all)
            
        Returns:
            Dataframe with filled values
        """
        if columns is None:
            columns = data.columns
        
        for col in columns:
            if col in data.columns:
                if strategy == 'mean':
                    data[col].fillna(data[col].mean(), inplace=True)
                elif strategy == 'median':
                    data[col].fillna(data[col].median(), inplace=True)
                elif strategy == 'ffill':
                    data[col].fillna(method='ffill', inplace=True)
                elif strategy == 'bfill':
                    data[col].fillna(method='bfill', inplace=True)
        
        logger.info(f"Filled missing values using {strategy} strategy")
        return data
    
    @staticmethod
    def normalize_columns(data: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Normalize column names (lowercase, replace spaces with underscores).
        
        Args:
            data: Input dataframe
            columns: Specific columns to normalize (None for all)
            
        Returns:
            Dataframe with normalized column names
        """
        if columns is None:
            columns = data.columns
        
        rename_map = {col: col.lower().replace(' ', '_').replace('-', '_') 
                     for col in columns if col in data.columns}
        data.rename(columns=rename_map, inplace=True)
        logger.info(f"Normalized {len(rename_map)} column names")
        return data
    
    @staticmethod
    def filter_data(data: pd.DataFrame, conditions: Dict[str, Any]) -> pd.DataFrame:
        """Filter dataframe based on conditions.
        
        Args:
            data: Input dataframe
            conditions: Dictionary of column names and filter values
            
        Returns:
            Filtered dataframe
        """
        filtered = data.copy()
        for column, value in conditions.items():
            if column in filtered.columns:
                if isinstance(value, (list, tuple)):
                    filtered = filtered[filtered[column].isin(value)]
                else:
                    filtered = filtered[filtered[column] == value]
        
        logger.info(f"Filtered data from {len(data)} to {len(filtered)} rows")
        return filtered


class ETLPipeline:
    """Main ETL Pipeline orchestrator."""
    
    def __init__(self, name: str):
        """Initialize ETL pipeline.
        
        Args:
            name: Pipeline name for logging
        """
        self.name = name
        self.source: Optional[DataSource] = None
        self.transformer = DataTransformer()
        self.data: Optional[pd.DataFrame] = None
        self.execution_log: List[Dict[str, Any]] = []
    
    def set_source(self, source: DataSource) -> None:
        """Set data source for pipeline.
        
        Args:
            source: DataSource instance
        """
        self.source = source
        logger.info(f"Data source set for pipeline: {self.name}")
    
    def extract(self) -> pd.DataFrame:
        """Execute extraction phase.
        
        Returns:
            Extracted dataframe
        """
        if self.source is None:
            raise ETLException("Data source not set. Call set_source() first.")
        
        self.source.connect()
        self.data = self.source.extract()
        self.source.disconnect()
        
        self._log_phase("extract", {"rows": len(self.data), "columns": len(self.data.columns)})
        return self.data
    
    def transform(self, operations: List[Tuple[str, Dict[str, Any]]]) -> pd.DataFrame:
        """Execute transformation phase.
        
        Args:
            operations: List of (operation_name, kwargs) tuples
            
        Returns:
            Transformed dataframe
        """
        if self.data is None:
            raise ETLException("No data to transform. Call extract() first.")
        
        for operation, kwargs in operations:
            if operation == 'remove_duplicates':
                self.data = self.transformer.remove_duplicates(self.data, **kwargs)
            elif operation == 'fill_missing_values':
                self.data = self.transformer.fill_missing_values(self.data, **kwargs)
            elif operation == 'normalize_columns':
                self.data = self.transformer.normalize_columns(self.data, **kwargs)
            elif operation == 'filter_data':
                self.data = self.transformer.filter_data(self.data, **kwargs)
            else:
                logger.warning(f"Unknown transformation operation: {operation}")
        
        self._log_phase("transform", {"rows": len(self.data), "operations": len(operations)})
        return self.data
    
    def load(self, output_path: str, format: str = 'csv') -> None:
        """Execute load phase.
        
        Args:
            output_path: Path to save output file
            format: Output format ('csv', 'json', 'parquet')
        """
        if self.data is None:
            raise ETLException("No data to load. Call extract() and transform() first.")
        
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if format == 'csv':
                self.data.to_csv(output_path, index=False)
            elif format == 'json':
                self.data.to_json(output_path, orient='records', indent=2)
            elif format == 'parquet':
                self.data.to_parquet(output_path, index=False)
            else:
                raise ETLException(f"Unsupported format: {format}")
            
            logger.info(f"Data loaded to {output_path}")
            self._log_phase("load", {"format": format, "path": str(output_path), "rows": len(self.data)})
        except Exception as e:
            raise ETLException(f"Failed to load data: {str(e)}")
    
    def execute(self, output_path: str, transformations: Optional[List[Tuple[str, Dict[str, Any]]]] = None,
                output_format: str = 'csv') -> pd.DataFrame:
        """Execute complete ETL pipeline.
        
        Args:
            output_path: Path to save output file
            transformations: List of transformation operations
            output_format: Output format
            
        Returns:
            Processed dataframe
        """
        try:
            logger.info(f"Starting ETL pipeline: {self.name}")
            self.extract()
            
            if transformations:
                self.transform(transformations)
            
            self.load(output_path, output_format)
            logger.info(f"ETL pipeline {self.name} completed successfully")
            
            return self.data
        except Exception as e:
            logger.error(f"ETL pipeline {self.name} failed: {str(e)}")
            raise
    
    def _log_phase(self, phase: str, details: Dict[str, Any]) -> None:
        """Log pipeline phase execution.
        
        Args:
            phase: Phase name
            details: Phase details
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "pipeline": self.name,
            "phase": phase,
            "details": details
        }
        self.execution_log.append(log_entry)
    
    def get_execution_log(self) -> List[Dict[str, Any]]:
        """Get execution log for the pipeline.
        
        Returns:
            List of execution log entries
        """
        return self.execution_log


def create_pipeline(pipeline_name: str, source_path: str) -> ETLPipeline:
    """Factory function to create a configured ETL pipeline.
    
    Args:
        pipeline_name: Name for the pipeline
        source_path: Path to data source
        
    Returns:
        Configured ETLPipeline instance
    """
    pipeline = ETLPipeline(pipeline_name)
    pipeline.set_source(CSVDataSource(source_path))
    return pipeline
