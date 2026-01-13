"""
NYC Taxi Rides Data Loader
Loads NYC Taxi Rides CSV data from local or S3 using PySpark
with schema inference, error handling, and data inspection capabilities.
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def create_spark_session(app_name="NYC_Taxi_Data_Loader"):
    """
    Create and return a Spark session.
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        
        print(f"✓ Spark session '{app_name}' created successfully")
        return spark
    
    except Exception as e:
        print(f"✗ Error creating Spark session: {str(e)}")
        sys.exit(1)


def load_taxi_data(spark, data_path):
    """
    Load NYC Taxi Rides CSV data from local or S3 path.
    
    Supports both local file paths and S3 paths (s3://bucket/path).
    Automatically infers schema and handles various error conditions.
    
    Args:
        spark (SparkSession): Active Spark session
        data_path (str): Path to CSV file (local or S3)
        
    Returns:
        DataFrame: Loaded taxi rides data, or None if loading fails
        
    Raises:
        FileNotFoundError: If local file does not exist
        Exception: For other errors during data loading
    """
    try:
        print(f"\n📂 Loading data from: {data_path}")
        
        # Check if it's a local path and file exists
        if not data_path.startswith("s3://"):
            import os
            if not os.path.exists(data_path):
                raise FileNotFoundError(f"Local file not found: {data_path}")
        
        # Load CSV with inferred schema
        df = spark.read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(data_path)
        
        print(f"✓ Data loaded successfully")
        print(f"  - Total records: {df.count():,}")
        print(f"  - Total columns: {len(df.columns)}")
        
        return df
    
    except FileNotFoundError as e:
        print(f"✗ FileNotFoundError: {str(e)}")
        print("  Please verify the file path and ensure the file exists.")
        return None
    
    except Exception as e:
        print(f"✗ Error loading data: {type(e).__name__}: {str(e)}")
        if "S3" in str(type(e)) or "s3" in data_path.lower():
            print("  Please verify S3 credentials and bucket/key permissions.")
        return None


def display_data_info(df):
    """
    Display schema and sample rows from the DataFrame.
    
    Args:
        df (DataFrame): Spark DataFrame to inspect
    """
    if df is None:
        print("✗ No data to display")
        return
    
    try:
        print("\n" + "="*70)
        print("DATA SCHEMA")
        print("="*70)
        df.printSchema()
        
        print("\n" + "="*70)
        print("SAMPLE DATA (First 10 rows)")
        print("="*70)
        df.show(10, truncate=False)
        
        print("\n" + "="*70)
        print("DATA STATISTICS")
        print("="*70)
        print(f"Column Names: {', '.join(df.columns)}")
        print(f"Data Types: {dict(df.dtypes)}")
        
    except Exception as e:
        print(f"✗ Error displaying data info: {str(e)}")


def main(path=None):
    """
    Main entry point for the NYC Taxi Data Loader.
    
    Accepts an optional command-line argument for the data path.
    If no path is provided, uses a default sample path.
    
    Args:
        path (str, optional): Path to CSV file. If None, parses from command line.
    """
    # Parse command-line arguments if path not provided
    if path is None:
        parser = argparse.ArgumentParser(
            description="Load and inspect NYC Taxi Rides CSV data"
        )
        parser.add_argument(
            "path",
            nargs="?",
            default="data/nyc_taxi_rides.csv",
            help="Path to CSV file (local path or s3://bucket/key). "
                 "Default: data/nyc_taxi_rides.csv"
        )
        
        args = parser.parse_args()
        data_path = args.path
    else:
        data_path = path
    
    print("="*70)
    print("NYC TAXI RIDES DATA LOADER")
    print("="*70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load data
    df = load_taxi_data(spark, data_path)
    
    # Display information
    if df is not None:
        display_data_info(df)
        print("\n" + "="*70)
        print("✓ Data loading and inspection completed successfully")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("✗ Data loading failed. Please check the error messages above.")
        print("="*70)
        sys.exit(1)


if __name__ == "__main__":
    main()
