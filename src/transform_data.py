"""
PySpark Data Transformation Module

This module provides comprehensive data transformation utilities including:
- Data cleaning and validation
- Feature engineering
- Data aggregations
- Logging and monitoring

Author: mrohitth
Date: 2026-01-13
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, lower, upper, coalesce,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    stddev, variance, percentile_approx, datediff, to_date,
    year, month, dayofmonth, hour, minute, second,
    concat, substring, length, instr, explode, array_contains,
    lag, lead, row_number, rank, dense_rank,
    first, last, collect_list, collect_set,
    isnan, isnull, isfinite, greatest, least,
    round as spark_round, abs as spark_abs, sqrt, log, exp
)
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, BooleanType, DateType, ArrayType
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataTransformationPipeline:
    """
    A comprehensive data transformation pipeline for PySpark DataFrames.
    Provides methods for cleaning, feature engineering, and aggregations.
    """

    def __init__(self, spark: SparkSession, log_level: str = "INFO"):
        """
        Initialize the transformation pipeline.

        Args:
            spark: SparkSession instance
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level)
        self.logger.info("DataTransformationPipeline initialized")

    # ==================== Data Cleaning Methods ====================

    def remove_duplicates(
        self,
        df: DataFrame,
        subset: Optional[List[str]] = None,
        keep: str = "first"
    ) -> DataFrame:
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
            if subset:
                result = df.drop_duplicates(subset=subset)
            else:
                result = df.drop_duplicates()
            self.logger.info(f"Successfully removed duplicates. Original: {df.count()}, Result: {result.count()}")
            return result
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {str(e)}")
            raise

    def handle_missing_values(
        self,
        df: DataFrame,
        strategy: str = "drop",
        fill_value: Optional[Dict] = None,
        threshold: float = 0.5
    ) -> DataFrame:
        """
        Handle missing values in DataFrame.

        Args:
            df: Input DataFrame
            strategy: 'drop', 'fill', or 'forward_fill'
            fill_value: Dictionary mapping column names to fill values
            threshold: Fraction of non-null values required (0 to 1)

        Returns:
            DataFrame with handled missing values
        """
        self.logger.info(f"Handling missing values with strategy: {strategy}")
        try:
            if strategy == "drop":
                result = df.dropna()
            elif strategy == "fill" and fill_value:
                result = df.fillna(fill_value)
            elif strategy == "forward_fill":
                window = Window.orderBy("id")
                result = df.select([
                    coalesce(col(c), last(col(c), ignorenulls=True).over(window))
                    if c != "id" else col(c)
                    for c in df.columns
                ])
            else:
                result = df

            self.logger.info(f"Missing values handled. Remaining nulls: {result.select([count(when(isnull(col(c)), 1)) for c in result.columns]).collect()}")
            return result
        except Exception as e:
            self.logger.error(f"Error handling missing values: {str(e)}")
            raise

    def clean_string_columns(
        self,
        df: DataFrame,
        columns: Optional[List[str]] = None,
        lowercase: bool = True,
        trim_whitespace: bool = True,
        remove_special_chars: bool = False
    ) -> DataFrame:
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
            if columns is None:
                columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

            result = df
            for col_name in columns:
                if col_name in df.columns:
                    col_expr = col(col_name)

                    if trim_whitespace:
                        col_expr = trim(col_expr)

                    if lowercase:
                        col_expr = lower(col_expr)

                    if remove_special_chars:
                        col_expr = regexp_replace(col_expr, "[^a-zA-Z0-9 ]", "")

                    result = result.withColumn(col_name, col_expr)

            self.logger.info(f"String columns cleaned successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error cleaning string columns: {str(e)}")
            raise

    def remove_outliers(
        self,
        df: DataFrame,
        columns: List[str],
        method: str = "iqr",
        threshold: float = 1.5
    ) -> DataFrame:
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
            result = df
            original_count = df.count()

            if method == "iqr":
                for col_name in columns:
                    q1 = df.approxQuantile(col_name, [0.25], 0.01)[0]
                    q3 = df.approxQuantile(col_name, [0.75], 0.01)[0]
                    iqr = q3 - q1
                    lower_bound = q1 - threshold * iqr
                    upper_bound = q3 + threshold * iqr

                    result = result.filter((col(col_name) >= lower_bound) & (col(col_name) <= upper_bound))
                    self.logger.info(f"Column {col_name}: removed outliers (bounds: {lower_bound}, {upper_bound})")

            elif method == "zscore":
                for col_name in columns:
                    mean = df.select(avg(col(col_name))).collect()[0][0]
                    stddev_val = df.select(stddev(col(col_name))).collect()[0][0]

                    result = result.filter(
                        (spark_abs((col(col_name) - mean) / stddev_val) < threshold)
                    )
                    self.logger.info(f"Column {col_name}: removed outliers (z-score threshold: {threshold})")

            final_count = result.count()
            self.logger.info(f"Outliers removed. Original: {original_count}, Result: {final_count}")
            return result
        except Exception as e:
            self.logger.error(f"Error removing outliers: {str(e)}")
            raise

    # ==================== Feature Engineering Methods ====================

    def create_date_features(
        self,
        df: DataFrame,
        date_column: str,
        prefix: str = ""
    ) -> DataFrame:
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
            prefix = f"{prefix}_" if prefix else ""

            result = df \
                .withColumn(f"{prefix}year", year(col(date_column))) \
                .withColumn(f"{prefix}month", month(col(date_column))) \
                .withColumn(f"{prefix}day", dayofmonth(col(date_column))) \
                .withColumn(f"{prefix}hour", hour(col(date_column))) \
                .withColumn(f"{prefix}minute", minute(col(date_column))) \
                .withColumn(f"{prefix}quarter", ((month(col(date_column)) - 1) / 3 + 1).cast(IntegerType()))

            self.logger.info(f"Date features created successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error creating date features: {str(e)}")
            raise

    def create_numerical_features(
        self,
        df: DataFrame,
        columns: List[str],
        operations: Optional[List[str]] = None
    ) -> DataFrame:
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

            result = df

            for col_name in columns:
                if 'sqrt' in operations:
                    result = result.withColumn(f"{col_name}_sqrt", sqrt(col(col_name)))

                if 'log' in operations:
                    result = result.withColumn(f"{col_name}_log", log(col(col_name) + 1))

                if 'square' in operations:
                    result = result.withColumn(f"{col_name}_square", col(col_name) * col(col_name))

                if 'reciprocal' in operations:
                    result = result.withColumn(f"{col_name}_reciprocal", 1.0 / (col(col_name) + 1))

                if 'abs' in operations:
                    result = result.withColumn(f"{col_name}_abs", spark_abs(col(col_name)))

            self.logger.info(f"Numerical features created successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error creating numerical features: {str(e)}")
            raise

    def create_categorical_features(
        self,
        df: DataFrame,
        column: str,
        method: str = "onehot"
    ) -> DataFrame:
        """
        Create categorical features through encoding.

        Args:
            df: Input DataFrame
            column: Categorical column to encode
            method: 'onehot' or 'label' encoding

        Returns:
            DataFrame with encoded categorical features
        """
        self.logger.info(f"Creating categorical features for column: {column} using {method} method")
        try:
            if method == "onehot":
                from pyspark.ml.feature import OneHotEncoder, StringIndexer

                # First, index the string column
                indexer = StringIndexer(inputCol=column, outputCol=f"{column}_indexed")
                indexed_df = indexer.fit(df).transform(df)

                # Then, one-hot encode
                encoder = OneHotEncoder(inputCol=f"{column}_indexed", outputCol=f"{column}_encoded")
                result = encoder.fit(indexed_df).transform(indexed_df)
                result = result.drop(f"{column}_indexed")

            else:  # label encoding
                from pyspark.ml.feature import StringIndexer
                indexer = StringIndexer(inputCol=column, outputCol=f"{column}_encoded")
                result = indexer.fit(df).transform(df)

            self.logger.info(f"Categorical features created successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error creating categorical features: {str(e)}")
            raise

    def create_window_features(
        self,
        df: DataFrame,
        partition_cols: List[str],
        order_cols: List[str],
        agg_functions: Dict[str, str]
    ) -> DataFrame:
        """
        Create features using window functions (lag, lead, rolling aggregations).

        Args:
            df: Input DataFrame
            partition_cols: Columns to partition by
            order_cols: Columns to order by
            agg_functions: Dict mapping column names to aggregation functions

        Returns:
            DataFrame with window features
        """
        self.logger.info(f"Creating window features partitioned by {partition_cols}")
        try:
            window_spec = Window.partitionBy(*partition_cols).orderBy(*[col(c) for c in order_cols])

            result = df
            for col_name, func in agg_functions.items():
                if func == "lag":
                    result = result.withColumn(f"{col_name}_lag1", lag(col(col_name)).over(window_spec))
                elif func == "lead":
                    result = result.withColumn(f"{col_name}_lead1", lead(col(col_name)).over(window_spec))
                elif func == "row_number":
                    result = result.withColumn(f"{col_name}_row_num", row_number().over(window_spec))
                elif func == "rank":
                    result = result.withColumn(f"{col_name}_rank", rank().over(window_spec))

            self.logger.info(f"Window features created successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error creating window features: {str(e)}")
            raise

    # ==================== Aggregation Methods ====================

    def simple_aggregation(
        self,
        df: DataFrame,
        group_cols: List[str],
        agg_specs: Dict[str, str]
    ) -> DataFrame:
        """
        Perform simple aggregations on grouped data.

        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            agg_specs: Dictionary mapping column names to aggregation functions
                      ('sum', 'avg', 'count', 'min', 'max', 'stddev', 'variance')

        Returns:
            Aggregated DataFrame
        """
        self.logger.info(f"Performing aggregations grouped by {group_cols}")
        try:
            agg_dict = {}
            for col_name, func in agg_specs.items():
                if func == "sum":
                    agg_dict[f"{col_name}_sum"] = spark_sum(col(col_name))
                elif func == "avg":
                    agg_dict[f"{col_name}_avg"] = avg(col(col_name))
                elif func == "count":
                    agg_dict[f"{col_name}_count"] = count(col(col_name))
                elif func == "min":
                    agg_dict[f"{col_name}_min"] = spark_min(col(col_name))
                elif func == "max":
                    agg_dict[f"{col_name}_max"] = spark_max(col(col_name))
                elif func == "stddev":
                    agg_dict[f"{col_name}_stddev"] = stddev(col(col_name))
                elif func == "variance":
                    agg_dict[f"{col_name}_variance"] = variance(col(col_name))

            result = df.groupBy(*group_cols).agg(agg_dict)
            self.logger.info(f"Aggregations completed. Result count: {result.count()}")
            return result
        except Exception as e:
            self.logger.error(f"Error performing aggregations: {str(e)}")
            raise

    def percentile_aggregation(
        self,
        df: DataFrame,
        group_cols: List[str],
        value_col: str,
        percentiles: List[float] = [0.25, 0.5, 0.75]
    ) -> DataFrame:
        """
        Calculate percentiles for grouped data.

        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            value_col: Column to calculate percentiles for
            percentiles: List of percentiles (0 to 1)

        Returns:
            DataFrame with percentile columns
        """
        self.logger.info(f"Calculating percentiles {percentiles} for {value_col}")
        try:
            agg_specs = {
                f"p{int(p*100)}": percentile_approx(col(value_col), p)
                for p in percentiles
            }

            result = df.groupBy(*group_cols).agg(agg_specs)
            self.logger.info(f"Percentile aggregation completed")
            return result
        except Exception as e:
            self.logger.error(f"Error calculating percentiles: {str(e)}")
            raise

    def pivot_aggregation(
        self,
        df: DataFrame,
        group_cols: List[str],
        pivot_col: str,
        value_col: str,
        agg_func: str = "sum"
    ) -> DataFrame:
        """
        Pivot data and aggregate values.

        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            pivot_col: Column to pivot on
            value_col: Column containing values to aggregate
            agg_func: Aggregation function ('sum', 'avg', 'count', 'min', 'max')

        Returns:
            Pivoted DataFrame
        """
        self.logger.info(f"Pivoting data on column: {pivot_col}")
        try:
            if agg_func == "sum":
                agg_expr = spark_sum(col(value_col))
            elif agg_func == "avg":
                agg_expr = avg(col(value_col))
            elif agg_func == "count":
                agg_expr = count(col(value_col))
            elif agg_func == "min":
                agg_expr = spark_min(col(value_col))
            elif agg_func == "max":
                agg_expr = spark_max(col(value_col))
            else:
                agg_expr = spark_sum(col(value_col))

            result = df.groupBy(*group_cols).pivot(pivot_col).agg(agg_expr)
            self.logger.info(f"Pivot aggregation completed")
            return result
        except Exception as e:
            self.logger.error(f"Error performing pivot aggregation: {str(e)}")
            raise

    # ==================== Data Validation Methods ====================

    def get_data_quality_report(self, df: DataFrame) -> Dict:
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
                "total_rows": df.count(),
                "total_columns": len(df.columns),
                "columns": df.columns,
                "schema": str(df.schema),
                "null_counts": {},
                "duplicate_rows": df.count() - df.drop_duplicates().count(),
                "memory_usage": None
            }

            # Calculate null counts
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                report["null_counts"][col_name] = {
                    "count": null_count,
                    "percentage": (null_count / report["total_rows"]) * 100
                }

            self.logger.info(f"Data quality report generated")
            return report
        except Exception as e:
            self.logger.error(f"Error generating quality report: {str(e)}")
            raise

    def validate_schema(self, df: DataFrame, expected_schema: StructType) -> bool:
        """
        Validate DataFrame schema against expected schema.

        Args:
            df: Input DataFrame
            expected_schema: Expected StructType schema

        Returns:
            Boolean indicating if schema matches
        """
        self.logger.info("Validating DataFrame schema")
        try:
            if df.schema == expected_schema:
                self.logger.info("Schema validation passed")
                return True
            else:
                self.logger.warning("Schema validation failed")
                return False
        except Exception as e:
            self.logger.error(f"Error validating schema: {str(e)}")
            raise

    # ==================== Utility Methods ====================

    def display_sample(self, df: DataFrame, n: int = 5, truncate: bool = False) -> None:
        """
        Display sample rows from DataFrame.

        Args:
            df: Input DataFrame
            n: Number of rows to display
            truncate: Whether to truncate columns
        """
        self.logger.info(f"Displaying {n} sample rows")
        df.show(n, truncate=truncate)

    def get_statistics(self, df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
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
                columns = [field.name for field in df.schema.fields 
                          if isinstance(field.dataType, (IntegerType, DoubleType))]

            result = df.describe(*columns)
            self.logger.info("Statistics computed successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error computing statistics: {str(e)}")
            raise


# ==================== Standalone Functions ====================

def log_transformation_step(
    step_name: str,
    input_count: int,
    output_count: int,
    duration_seconds: float
) -> None:
    """
    Log details of a transformation step.

    Args:
        step_name: Name of the transformation step
        input_count: Number of input rows
        output_count: Number of output rows
        duration_seconds: Execution duration in seconds
    """
    logger.info(
        f"Step: {step_name} | Input: {input_count} rows | "
        f"Output: {output_count} rows | Duration: {duration_seconds:.2f}s"
    )


def create_spark_session(app_name: str = "DataTransformation") -> SparkSession:
    """
    Create and return a SparkSession.

    Args:
        app_name: Name of the Spark application

    Returns:
        SparkSession instance
    """
    logger.info(f"Creating SparkSession: {app_name}")
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


if __name__ == "__main__":
    logger.info("Data Transformation Module loaded successfully")
