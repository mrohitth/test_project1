"""
Pandas Data Transformation Module (Non-Spark Version)

This module provides comprehensive data transformation utilities using pandas
for environments where PySpark is not available (like Google Colab).

Author: mrohitth
Date: 2026-01-16
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PandasDataTransformer:
    """
    A comprehensive data transformation pipeline for Pandas DataFrames.
    Provides methods for cleaning, feature engineering, and aggregations.
    """

    def __init__(self, log_level: str = "INFO"):
        """
        Initialize the transformation pipeline.

        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level)
        self.logger.info("PandasDataTransformer initialized")

    # ==================== Data Cleaning Methods ====================

    def remove_duplicates(
        self,
        df: pd.DataFrame,
        subset: Optional[List[str]] = None,
        keep: str = "first"
    ) -> pd.DataFrame:
        """
        Remove duplicate rows from DataFrame.

        Args:
            df: Input DataFrame
            subset: Column names to consider for duplicates. If None, uses all columns
            keep: Which duplicates to keep ('first' or 'last')

        Returns:
            DataFrame with duplicates removed
        """
        self.logger.info(f"Removing duplicates from DataFrame")
        try:
            original_count = len(df)
            if subset:
                result = df.drop_duplicates(subset=subset, keep=keep)
            else:
                result = df.drop_duplicates(keep=keep)
            removed = original_count - len(result)
            self.logger.info(f"Successfully removed {removed} duplicates. Original: {original_count}, Result: {len(result)}")
            return result
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {str(e)}")
            raise

    def handle_missing_values(
        self,
        df: pd.DataFrame,
        strategy: str = "drop",
        fill_value: Optional[Dict] = None,
        threshold: float = 0.5
    ) -> pd.DataFrame:
        """
        Handle missing values in DataFrame.

        Args:
            df: Input DataFrame
            strategy: 'drop', 'fill', 'median', or 'mean'
            fill_value: Dictionary mapping column names to fill values
            threshold: Fraction of non-null values required (0 to 1)

        Returns:
            DataFrame with handled missing values
        """
        self.logger.info(f"Handling missing values with strategy: {strategy}")
        try:
            result = df.copy()
            
            if strategy == "drop":
                result = result.dropna()
            elif strategy == "fill" and fill_value:
                result = result.fillna(fill_value)
            elif strategy == "median":
                numeric_cols = result.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    result[col].fillna(result[col].median(), inplace=True)
            elif strategy == "mean":
                numeric_cols = result.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    result[col].fillna(result[col].mean(), inplace=True)
            else:
                self.logger.warning(f"Unknown strategy: {strategy}, returning original DataFrame")

            missing_after = result.isnull().sum().sum()
            self.logger.info(f"Missing values handled. Remaining nulls: {missing_after}")
            return result
        except Exception as e:
            self.logger.error(f"Error handling missing values: {str(e)}")
            raise

    def clean_string_columns(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        lowercase: bool = True,
        trim_whitespace: bool = True,
        remove_special_chars: bool = False
    ) -> pd.DataFrame:
        """
        Clean string columns by trimming, converting case, and removing special characters.

        Args:
            df: Input DataFrame
            columns: Specific columns to clean. If None, cleans all string columns
            lowercase: Convert to lowercase
            trim_whitespace: Remove leading/trailing whitespace
            remove_special_chars: Remove special characters

        Returns:
            DataFrame with cleaned string columns
        """
        self.logger.info(f"Cleaning string columns: {columns}")
        try:
            result = df.copy()
            
            if columns is None:
                columns = result.select_dtypes(include=['object']).columns.tolist()

            for col_name in columns:
                if col_name in result.columns:
                    if trim_whitespace:
                        result[col_name] = result[col_name].str.strip()

                    if lowercase:
                        result[col_name] = result[col_name].str.lower()

                    if remove_special_chars:
                        result[col_name] = result[col_name].str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)

            self.logger.info(f"String columns cleaned successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error cleaning string columns: {str(e)}")
            raise

    def remove_outliers(
        self,
        df: pd.DataFrame,
        columns: List[str],
        method: str = "iqr",
        threshold: float = 1.5
    ) -> pd.DataFrame:
        """
        Remove outliers from numerical columns.

        Args:
            df: Input DataFrame
            columns: Numerical columns to check for outliers
            method: 'iqr' (Interquartile Range) or 'zscore'
            threshold: IQR multiplier or Z-score threshold

        Returns:
            DataFrame with outliers removed
        """
        self.logger.info(f"Removing outliers from {columns} using {method} method")
        try:
            result = df.copy()
            original_count = len(result)

            if method == "iqr":
                for col_name in columns:
                    Q1 = result[col_name].quantile(0.25)
                    Q3 = result[col_name].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - threshold * IQR
                    upper_bound = Q3 + threshold * IQR

                    result = result[(result[col_name] >= lower_bound) & (result[col_name] <= upper_bound)]
                    self.logger.info(f"Column {col_name}: removed outliers (bounds: {lower_bound:.2f}, {upper_bound:.2f})")

            elif method == "zscore":
                for col_name in columns:
                    z_scores = np.abs((result[col_name] - result[col_name].mean()) / result[col_name].std())
                    result = result[z_scores < threshold]
                    self.logger.info(f"Column {col_name}: removed outliers (z-score threshold: {threshold})")

            final_count = len(result)
            self.logger.info(f"Outliers removed. Original: {original_count}, Result: {final_count}, Removed: {original_count - final_count}")
            return result
        except Exception as e:
            self.logger.error(f"Error removing outliers: {str(e)}")
            raise

    # ==================== Feature Engineering Methods ====================

    def create_date_features(
        self,
        df: pd.DataFrame,
        date_column: str,
        prefix: str = ""
    ) -> pd.DataFrame:
        """
        Create date-based features from a timestamp column.

        Args:
            df: Input DataFrame
            date_column: Name of the date/timestamp column
            prefix: Prefix for new feature columns

        Returns:
            DataFrame with new date features
        """
        self.logger.info(f"Creating date features from column: {date_column}")
        try:
            result = df.copy()
            prefix = f"{prefix}_" if prefix else ""

            # Ensure column is datetime
            result[date_column] = pd.to_datetime(result[date_column])

            result[f"{prefix}year"] = result[date_column].dt.year
            result[f"{prefix}month"] = result[date_column].dt.month
            result[f"{prefix}day"] = result[date_column].dt.day
            result[f"{prefix}hour"] = result[date_column].dt.hour
            result[f"{prefix}minute"] = result[date_column].dt.minute
            result[f"{prefix}quarter"] = result[date_column].dt.quarter
            result[f"{prefix}dayofweek"] = result[date_column].dt.dayofweek
            result[f"{prefix}is_weekend"] = result[date_column].dt.dayofweek.isin([5, 6])

            self.logger.info(f"Date features created successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error creating date features: {str(e)}")
            raise

    def create_numerical_features(
        self,
        df: pd.DataFrame,
        columns: List[str],
        operations: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Create derived numerical features (sqrt, log, squared, etc.).

        Args:
            df: Input DataFrame
            columns: Numerical columns to transform
            operations: List of operations ('sqrt', 'log', 'square', 'reciprocal', 'abs')

        Returns:
            DataFrame with new numerical features
        """
        self.logger.info(f"Creating numerical features for columns: {columns}")
        try:
            if operations is None:
                operations = ['sqrt', 'log', 'square']

            result = df.copy()

            for col_name in columns:
                if 'sqrt' in operations:
                    result[f"{col_name}_sqrt"] = np.sqrt(result[col_name].abs())

                if 'log' in operations:
                    result[f"{col_name}_log"] = np.log1p(result[col_name].abs())

                if 'square' in operations:
                    result[f"{col_name}_square"] = result[col_name] ** 2

                if 'reciprocal' in operations:
                    result[f"{col_name}_reciprocal"] = 1.0 / (result[col_name] + 1)

                if 'abs' in operations:
                    result[f"{col_name}_abs"] = result[col_name].abs()

            self.logger.info(f"Numerical features created successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error creating numerical features: {str(e)}")
            raise

    # ==================== Aggregation Methods ====================

    def simple_aggregation(
        self,
        df: pd.DataFrame,
        group_cols: List[str],
        agg_specs: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Perform simple aggregations on grouped data.

        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            agg_specs: Dictionary mapping column names to aggregation functions
                      ('sum', 'mean', 'count', 'min', 'max', 'std', 'var')

        Returns:
            Aggregated DataFrame
        """
        self.logger.info(f"Performing aggregations grouped by {group_cols}")
        try:
            # Build aggregation dictionary
            agg_dict = {}
            for col_name, func in agg_specs.items():
                agg_dict[col_name] = func

            result = df.groupby(group_cols).agg(agg_dict).reset_index()
            
            # Rename columns
            new_cols = group_cols.copy()
            for col_name, func in agg_specs.items():
                new_cols.append(f"{col_name}_{func}")
            result.columns = new_cols

            self.logger.info(f"Aggregations completed. Result count: {len(result)}")
            return result
        except Exception as e:
            self.logger.error(f"Error performing aggregations: {str(e)}")
            raise

    # ==================== Data Validation Methods ====================

    def get_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate a comprehensive data quality report.

        Args:
            df: Input DataFrame

        Returns:
            Dictionary containing quality metrics
        """
        self.logger.info("Generating data quality report")
        try:
            report = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "columns": list(df.columns),
                "dtypes": df.dtypes.astype(str).to_dict(),
                "null_counts": {},
                "duplicate_rows": df.duplicated().sum(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
            }

            # Calculate null counts
            for col_name in df.columns:
                null_count = df[col_name].isnull().sum()
                report["null_counts"][col_name] = {
                    "count": int(null_count),
                    "percentage": float((null_count / len(df)) * 100) if len(df) > 0 else 0.0
                }

            self.logger.info(f"Data quality report generated")
            return report
        except Exception as e:
            self.logger.error(f"Error generating quality report: {str(e)}")
            raise

    def get_statistics(self, df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get statistical summary of numerical columns.

        Args:
            df: Input DataFrame
            columns: Specific columns to analyze. If None, analyzes all numerical columns

        Returns:
            DataFrame with statistics
        """
        self.logger.info("Computing statistics")
        try:
            if columns is None:
                columns = df.select_dtypes(include=[np.number]).columns.tolist()

            result = df[columns].describe()
            self.logger.info("Statistics computed successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error computing statistics: {str(e)}")
            raise


def main():
    """Example usage of the PandasDataTransformer."""
    logger.info("=" * 80)
    logger.info("Pandas Data Transformer Example")
    logger.info("=" * 80)
    
    # Create sample data
    np.random.seed(42)
    sample_data = pd.DataFrame({
        'id': range(1, 101),
        'value': np.random.randn(100) * 10 + 50,
        'category': np.random.choice(['A', 'B', 'C'], 100),
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='H')
    })
    
    logger.info(f"Created sample data with shape: {sample_data.shape}")
    
    # Initialize transformer
    transformer = PandasDataTransformer()
    
    # Demonstrate transformations
    logger.info("\n1. Creating date features...")
    data_with_dates = transformer.create_date_features(sample_data, 'timestamp')
    logger.info(f"   Added columns: {[c for c in data_with_dates.columns if c not in sample_data.columns]}")
    
    logger.info("\n2. Creating numerical features...")
    data_with_features = transformer.create_numerical_features(
        sample_data, ['value'], operations=['sqrt', 'log', 'square']
    )
    logger.info(f"   Added columns: {[c for c in data_with_features.columns if c not in sample_data.columns]}")
    
    logger.info("\n3. Generating data quality report...")
    quality_report = transformer.get_data_quality_report(sample_data)
    logger.info(f"   Total rows: {quality_report['total_rows']}")
    logger.info(f"   Total columns: {quality_report['total_columns']}")
    logger.info(f"   Memory usage: {quality_report['memory_usage_mb']:.2f} MB")
    
    logger.info("\n" + "=" * 80)
    logger.info("Pandas Data Transformer Example Complete!")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
