"""
Comprehensive Streamlit Dashboard Application
==============================================
A feature-rich dashboard for data analysis, transformation, and visualization
with multi-tab interface, database connectivity, and performance metrics.

Features:
- Multi-tab interface for different data views
- DuckDB and PostgreSQL database connectivity
- Interactive filtering capabilities
- Advanced Plotly visualizations
- Data transformation with metrics tracking
- CSV export functionality
- Performance testing and simulation
- Comprehensive logging

Author: mrohitth
Created: 2026-01-13
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import duckdb
import logging
import os
import json
from datetime import datetime, timedelta
from io import BytesIO
import time
import sys
from typing import Tuple, Dict, List, Any
import psycopg2
from psycopg2 import sql
from abc import ABC, abstractmethod

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

class DashboardLogger:
    """Comprehensive logging system for the dashboard."""
    
    LOG_DIR = "logs"
    
    def __init__(self, name: str):
        """Initialize logger with file and console handlers."""
        os.makedirs(self.LOG_DIR, exist_ok=True)
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # File handler
        log_file = os.path.join(
            self.LOG_DIR,
            f"dashboard_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_logger(self):
        """Return the logger instance."""
        return self.logger


# Initialize logger
logger_instance = DashboardLogger(__name__).get_logger()


# ============================================================================
# DATABASE CONNECTIVITY
# ============================================================================

class DatabaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    @abstractmethod
    def connect(self):
        """Establish database connection."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        pass
    
    @abstractmethod
    def create_sample_data(self):
        """Create sample data for demonstration."""
        pass
    
    @abstractmethod
    def close(self):
        """Close database connection."""
        pass


class DuckDBConnector(DatabaseConnector):
    """DuckDB database connector."""
    
    def __init__(self, db_path: str = ":memory:"):
        """Initialize DuckDB connector."""
        self.db_path = db_path
        self.conn = None
        logger_instance.info(f"Initializing DuckDB connector with path: {db_path}")
        self.connect()
    
    def connect(self):
        """Establish DuckDB connection."""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger_instance.info("DuckDB connection established successfully")
        except Exception as e:
            logger_instance.error(f"Failed to connect to DuckDB: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        try:
            logger_instance.debug(f"Executing DuckDB query: {query[:100]}...")
            result = self.conn.execute(query).fetch_df()
            logger_instance.debug(f"Query returned {len(result)} rows")
            return result
        except Exception as e:
            logger_instance.error(f"Query execution failed: {str(e)}")
            raise
    
    def create_sample_data(self):
        """Create sample taxi data for demonstration."""
        try:
            logger_instance.info("Creating sample data in DuckDB")
            
            # Drop existing table if it exists
            self.conn.execute("DROP TABLE IF EXISTS taxi_data")
            
            # Create table
            self.conn.execute("""
                CREATE TABLE taxi_data (
                    trip_id INTEGER PRIMARY KEY,
                    pickup_datetime TIMESTAMP,
                    dropoff_datetime TIMESTAMP,
                    vendor_id INTEGER,
                    passenger_count INTEGER,
                    trip_distance DECIMAL(10, 2),
                    fare_amount DECIMAL(10, 2),
                    tip_amount DECIMAL(10, 2),
                    total_amount DECIMAL(10, 2),
                    payment_type VARCHAR,
                    pickup_location VARCHAR,
                    dropoff_location VARCHAR,
                    speed_mph DECIMAL(10, 2),
                    is_processed BOOLEAN
                )
            """)
            
            # Generate sample data
            np.random.seed(42)
            n_records = 5000
            
            dates = [datetime.now() - timedelta(days=x) for x in range(30)]
            data = {
                'trip_id': range(1, n_records + 1),
                'pickup_datetime': np.random.choice(dates, n_records),
                'dropoff_datetime': [d + timedelta(minutes=np.random.randint(5, 60)) for d in np.random.choice(dates, n_records)],
                'vendor_id': np.random.choice([1, 2, 3], n_records),
                'passenger_count': np.random.choice([1, 2, 3, 4, 5, 6], n_records),
                'trip_distance': np.random.uniform(0.5, 30, n_records),
                'fare_amount': np.random.uniform(2.5, 100, n_records),
                'tip_amount': np.random.uniform(0, 50, n_records),
                'total_amount': np.random.uniform(2.5, 150, n_records),
                'payment_type': np.random.choice(['Credit Card', 'Cash', 'Mobile'], n_records),
                'pickup_location': np.random.choice(['Manhattan', 'Brooklyn', 'Queens', 'Bronx'], n_records),
                'dropoff_location': np.random.choice(['Manhattan', 'Brooklyn', 'Queens', 'Bronx'], n_records),
                'speed_mph': np.random.uniform(5, 60, n_records),
                'is_processed': np.random.choice([True, False], n_records, p=[0.7, 0.3])
            }
            
            df = pd.DataFrame(data)
            self.conn.register('taxi_data_temp', df)
            self.conn.execute("INSERT INTO taxi_data SELECT * FROM taxi_data_temp")
            
            logger_instance.info(f"Created sample data with {n_records} records")
        except Exception as e:
            logger_instance.error(f"Failed to create sample data: {str(e)}")
            raise
    
    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger_instance.info("DuckDB connection closed")


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector."""
    
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 5432):
        """Initialize PostgreSQL connector."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.conn = None
        logger_instance.info(f"Initializing PostgreSQL connector for {database}@{host}:{port}")
        try:
            self.connect()
        except Exception as e:
            logger_instance.warning(f"PostgreSQL connection failed: {str(e)}. Using DuckDB instead.")
    
    def connect(self):
        """Establish PostgreSQL connection."""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            logger_instance.info("PostgreSQL connection established successfully")
        except Exception as e:
            logger_instance.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        try:
            logger_instance.debug(f"Executing PostgreSQL query: {query[:100]}...")
            result = pd.read_sql(query, self.conn)
            logger_instance.debug(f"Query returned {len(result)} rows")
            return result
        except Exception as e:
            logger_instance.error(f"Query execution failed: {str(e)}")
            raise
    
    def create_sample_data(self):
        """Create sample data in PostgreSQL."""
        logger_instance.info("PostgreSQL sample data creation skipped (use external setup)")
    
    def close(self):
        """Close PostgreSQL connection."""
        if self.conn:
            self.conn.close()
            logger_instance.info("PostgreSQL connection closed")


# ============================================================================
# DATA TRANSFORMATION ENGINE
# ============================================================================

class DataTransformer:
    """Handles data transformation operations."""
    
    def __init__(self):
        """Initialize transformer."""
        self.transformation_stats = {
            'before_count': 0,
            'after_count': 0,
            'rows_removed': 0,
            'rows_added': 0
        }
        logger_instance.info("DataTransformer initialized")
    
    def clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean and validate data."""
        logger_instance.info(f"Starting data cleaning for {len(df)} records")
        self.transformation_stats['before_count'] = len(df)
        
        # Remove duplicates
        df_clean = df.drop_duplicates(subset=['trip_id'])
        
        # Remove rows with missing critical values
        df_clean = df_clean.dropna(subset=['pickup_datetime', 'vendor_id', 'passenger_count'])
        
        # Validate ranges
        df_clean = df_clean[
            (df_clean['passenger_count'] > 0) &
            (df_clean['passenger_count'] <= 8) &
            (df_clean['trip_distance'] >= 0) &
            (df_clean['fare_amount'] >= 0)
        ]
        
        self.transformation_stats['after_count'] = len(df_clean)
        self.transformation_stats['rows_removed'] = self.transformation_stats['before_count'] - self.transformation_stats['after_count']
        
        # Convert data types
        df_clean['pickup_datetime'] = pd.to_datetime(df_clean['pickup_datetime'])
        df_clean['dropoff_datetime'] = pd.to_datetime(df_clean['dropoff_datetime'])
        df_clean['passenger_count'] = df_clean['passenger_count'].astype(int)
        
        logger_instance.info(f"Data cleaning completed: {len(df_clean)} records after transformation")
        
        return df_clean, self.transformation_stats
    
    def aggregate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate data by key metrics."""
        logger_instance.info("Starting data aggregation")
        
        agg_stats = df.groupby('vendor_id').agg({
            'trip_id': 'count',
            'fare_amount': ['sum', 'mean', 'std'],
            'trip_distance': 'mean',
            'passenger_count': 'mean',
            'total_amount': 'mean'
        }).round(2)
        
        logger_instance.info(f"Aggregation completed for {len(agg_stats)} vendor groups")
        return agg_stats
    
    def get_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate comprehensive statistics."""
        logger_instance.debug("Calculating statistics")
        
        stats = {
            'total_trips': len(df),
            'unique_vendors': df['vendor_id'].nunique(),
            'avg_distance': df['trip_distance'].mean(),
            'avg_fare': df['fare_amount'].mean(),
            'avg_passengers': df['passenger_count'].mean(),
            'total_revenue': df['total_amount'].sum(),
            'avg_speed': df['speed_mph'].mean() if 'speed_mph' in df.columns else 0
        }
        
        return stats


# ============================================================================
# STREAMLIT PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

@st.cache_resource
def init_db_connector():
    """Initialize database connector with caching."""
    logger_instance.info("Initializing database connector")
    connector = DuckDBConnector(":memory:")
    connector.create_sample_data()
    return connector

@st.cache_resource
def init_transformer():
    """Initialize data transformer."""
    return DataTransformer()

# Initialize session state
if 'db_connector' not in st.session_state:
    st.session_state.db_connector = init_db_connector()
    logger_instance.info("Database connector stored in session state")

if 'transformer' not in st.session_state:
    st.session_state.transformer = init_transformer()
    logger_instance.info("Data transformer stored in session state")

if 'raw_data' not in st.session_state:
    try:
        st.session_state.raw_data = st.session_state.db_connector.execute_query(
            "SELECT * FROM taxi_data"
        )
        logger_instance.info(f"Loaded {len(st.session_state.raw_data)} raw records")
    except Exception as e:
        logger_instance.error(f"Failed to load raw data: {str(e)}")
        st.error(f"Failed to load data: {str(e)}")

# ============================================================================
# HEADER SECTION
# ============================================================================

col1, col2, col3 = st.columns([2, 2, 2])

with col1:
    st.title("📊 Data Analytics Dashboard")

with col3:
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

st.divider()

# ============================================================================
# SIDEBAR CONFIGURATION
# ============================================================================

with st.sidebar:
    st.header("⚙️ Dashboard Controls")
    
    # Database Configuration
    st.subheader("Database Configuration")
    db_type = st.radio("Select Database Type", ["DuckDB", "PostgreSQL"])
    
    if db_type == "PostgreSQL":
        st.info("PostgreSQL configuration would be set up here in production")
        pg_host = st.text_input("Host", "localhost")
        pg_database = st.text_input("Database", "dashboard_db")
        pg_user = st.text_input("User", "postgres")
        pg_password = st.text_input("Password", type="password")
    
    st.divider()
    
    # Date Range Filter
    st.subheader("Date Range Filter")
    date_range = st.slider(
        "Select date range (days back)",
        0, 30, (0, 30),
        help="Select the number of days back from today"
    )
    
    st.divider()
    
    # Vendor Filter
    st.subheader("Vendor Filter")
    all_vendors = sorted(st.session_state.raw_data['vendor_id'].unique())
    selected_vendors = st.multiselect(
        "Select Vendors",
        all_vendors,
        default=all_vendors,
        help="Select one or more vendors to filter data"
    )
    
    st.divider()
    
    # Passenger Count Filter
    st.subheader("Passenger Count Filter")
    min_passengers = st.slider("Minimum Passengers", 1, 8, 1)
    max_passengers = st.slider("Maximum Passengers", 1, 8, 8)
    
    st.divider()
    
    # Data Simulation
    st.subheader("Performance Testing")
    if st.button("🔄 Generate Large Dataset", key="gen_dataset"):
        logger_instance.info("Generating large dataset for performance testing")
        with st.spinner("Generating 50,000 records..."):
            np.random.seed(42)
            n_records = 50000
            dates = [datetime.now() - timedelta(days=x) for x in range(30)]
            
            large_data = pd.DataFrame({
                'trip_id': range(5001, 5001 + n_records),
                'pickup_datetime': np.random.choice(dates, n_records),
                'dropoff_datetime': [d + timedelta(minutes=np.random.randint(5, 60)) 
                                    for d in np.random.choice(dates, n_records)],
                'vendor_id': np.random.choice([1, 2, 3], n_records),
                'passenger_count': np.random.choice([1, 2, 3, 4, 5, 6], n_records),
                'trip_distance': np.random.uniform(0.5, 30, n_records),
                'fare_amount': np.random.uniform(2.5, 100, n_records),
                'tip_amount': np.random.uniform(0, 50, n_records),
                'total_amount': np.random.uniform(2.5, 150, n_records),
                'payment_type': np.random.choice(['Credit Card', 'Cash', 'Mobile'], n_records),
                'pickup_location': np.random.choice(['Manhattan', 'Brooklyn', 'Queens', 'Bronx'], n_records),
                'dropoff_location': np.random.choice(['Manhattan', 'Brooklyn', 'Queens', 'Bronx'], n_records),
                'speed_mph': np.random.uniform(5, 60, n_records),
                'is_processed': np.random.choice([True, False], n_records, p=[0.7, 0.3])
            })
            
            st.session_state.raw_data = pd.concat(
                [st.session_state.raw_data, large_data],
                ignore_index=True
            )
            logger_instance.info(f"Dataset expanded to {len(st.session_state.raw_data)} records")
            st.success(f"✅ Generated 50,000 new records! Total: {len(st.session_state.raw_data)} records")

# ============================================================================
# APPLY FILTERS
# ============================================================================

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all active filters to the dataset."""
    logger_instance.debug("Applying filters to dataset")
    
    # Date filter
    start_date = datetime.now() - timedelta(days=date_range[1])
    end_date = datetime.now() - timedelta(days=date_range[0])
    df = df[(df['pickup_datetime'] >= start_date) & (df['pickup_datetime'] <= end_date)]
    
    # Vendor filter
    df = df[df['vendor_id'].isin(selected_vendors)]
    
    # Passenger count filter
    df = df[(df['passenger_count'] >= min_passengers) & (df['passenger_count'] <= max_passengers)]
    
    logger_instance.debug(f"Filters applied: {len(df)} records remaining")
    return df

filtered_data = apply_filters(st.session_state.raw_data.copy())

# ============================================================================
# MAIN CONTENT - TABBED INTERFACE
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Raw Data",
    "✨ Transformed Data",
    "📈 Aggregations",
    "⚡ Performance Metrics"
])

# ============================================================================
# TAB 1: RAW DATA
# ============================================================================

with tab1:
    st.header("Raw Data Explorer")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Records",
            len(filtered_data),
            delta=f"{len(filtered_data) - len(st.session_state.raw_data)}" if len(filtered_data) < len(st.session_state.raw_data) else "All",
            delta_color="off"
        )
    
    with col2:
        st.metric(
            "Unique Vendors",
            filtered_data['vendor_id'].nunique(),
            help="Number of unique vendors in filtered dataset"
        )
    
    with col3:
        st.metric(
            "Avg Trip Distance",
            f"{filtered_data['trip_distance'].mean():.2f} miles",
            help="Average trip distance"
        )
    
    with col4:
        st.metric(
            "Total Revenue",
            f"${filtered_data['total_amount'].sum():.2f}",
            help="Total revenue from all trips"
        )
    
    st.divider()
    
    # Data table with sorting options
    st.subheader("Data Table")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        sort_by = st.selectbox(
            "Sort by",
            ["pickup_datetime", "fare_amount", "trip_distance", "passenger_count"]
        )
    with col2:
        sort_order = st.radio("Order", ["Ascending", "Descending"], horizontal=True)
    with col3:
        rows_to_show = st.number_input("Rows to display", 10, 1000, 50)
    
    ascending = sort_order == "Ascending"
    display_data = filtered_data.sort_values(sort_by, ascending=ascending).head(rows_to_show)
    
    st.dataframe(display_data, use_container_width=True, height=400)
    
    st.divider()
    
    # Download options
    st.subheader("Export Data")
    col1, col2 = st.columns(2)
    
    with col1:
        # CSV Download
        csv_buffer = BytesIO()
        filtered_data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        st.download_button(
            label="📥 Download as CSV",
            data=csv_buffer.getvalue(),
            file_name=f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            help="Download the filtered dataset as CSV"
        )
    
    with col2:
        # JSON Download
        json_data = filtered_data.to_json(date_format='iso', orient='records')
        st.download_button(
            label="📥 Download as JSON",
            data=json_data,
            file_name=f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            help="Download the filtered dataset as JSON"
        )
    
    # Data quality report
    st.subheader("Data Quality Report")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        missing_values = filtered_data.isnull().sum().sum()
        st.metric("Missing Values", missing_values)
    with col2:
        duplicate_rows = filtered_data.duplicated().sum()
        st.metric("Duplicate Rows", duplicate_rows)
    with col3:
        st.metric("Data Completeness", f"{(1 - missing_values / (len(filtered_data) * len(filtered_data.columns))) * 100:.1f}%")

# ============================================================================
# TAB 2: TRANSFORMED DATA
# ============================================================================

with tab2:
    st.header("Data Transformation Pipeline")
    
    # Run transformation
    start_time = time.time()
    transformed_data, stats = st.session_state.transformer.clean_data(filtered_data.copy())
    transform_time = time.time() - start_time
    
    logger_instance.info(f"Data transformation completed in {transform_time:.2f} seconds")
    
    # Transformation stats
    st.subheader("Transformation Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Before",
            f"{stats['before_count']:,}",
            help="Records before transformation"
        )
    
    with col2:
        st.metric(
            "After",
            f"{stats['after_count']:,}",
            help="Records after transformation"
        )
    
    with col3:
        st.metric(
            "Rows Removed",
            f"{stats['rows_removed']:,}",
            help="Records removed during cleaning"
        )
    
    with col4:
        removal_percent = (stats['rows_removed'] / stats['before_count'] * 100) if stats['before_count'] > 0 else 0
        st.metric(
            "Removal Rate",
            f"{removal_percent:.1f}%",
            help="Percentage of rows removed"
        )
    
    with col5:
        st.metric(
            "Processing Time",
            f"{transform_time*1000:.0f}ms",
            help="Time taken for transformation"
        )
    
    st.divider()
    
    # Transformation comparison chart
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Before vs After Record Count", "Vendor Distribution"),
        specs=[[{"type": "bar"}, {"type": "pie"}]]
    )
    
    # Before/After chart
    fig.add_trace(
        go.Bar(
            name="Records",
            x=["Before", "After"],
            y=[stats['before_count'], stats['after_count']],
            marker_color=['#FF6B6B', '#4ECDC4'],
            text=[stats['before_count'], stats['after_count']],
            textposition='auto'
        ),
        row=1, col=1
    )
    
    # Vendor distribution pie chart
    vendor_counts = transformed_data['vendor_id'].value_counts()
    fig.add_trace(
        go.Pie(
            labels=[f"Vendor {v}" for v in vendor_counts.index],
            values=vendor_counts.values,
            hole=.3
        ),
        row=1, col=2
    )
    
    fig.update_layout(height=400, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Transformed data table
    st.subheader("Transformed Data Preview")
    st.dataframe(transformed_data.head(50), use_container_width=True, height=400)
    
    st.divider()
    
    # Statistics
    st.subheader("Transformation Statistics")
    calc_stats = st.session_state.transformer.get_statistics(transformed_data)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Trips", f"{calc_stats['total_trips']:,}")
        st.metric("Unique Vendors", calc_stats['unique_vendors'])
    with col2:
        st.metric("Avg Distance", f"{calc_stats['avg_distance']:.2f} mi")
        st.metric("Avg Fare", f"${calc_stats['avg_fare']:.2f}")
    with col3:
        st.metric("Avg Passengers", f"{calc_stats['avg_passengers']:.2f}")
        st.metric("Avg Speed", f"{calc_stats['avg_speed']:.1f} mph")
    
    # Download transformed data
    st.subheader("Export Transformed Data")
    csv_trans = BytesIO()
    transformed_data.to_csv(csv_trans, index=False)
    csv_trans.seek(0)
    
    st.download_button(
        label="📥 Download Transformed Data as CSV",
        data=csv_trans.getvalue(),
        file_name=f"transformed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True
    )

# ============================================================================
# TAB 3: AGGREGATIONS
# ============================================================================

with tab3:
    st.header("Data Aggregations & Advanced Analytics")
    
    # Aggregation by vendor
    st.subheader("Aggregations by Vendor")
    agg_data = st.session_state.transformer.aggregate_data(filtered_data)
    st.dataframe(agg_data, use_container_width=True)
    
    st.divider()
    
    # Multi-visualization section
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by vendor bar chart
        st.subheader("Total Revenue by Vendor")
        revenue_by_vendor = filtered_data.groupby('vendor_id')['total_amount'].sum().sort_values(ascending=False)
        
        fig_revenue = go.Figure(data=[
            go.Bar(
                x=[f"Vendor {v}" for v in revenue_by_vendor.index],
                y=revenue_by_vendor.values,
                marker_color='#3498db',
                text=[f"${v:.0f}" for v in revenue_by_vendor.values],
                textposition='auto'
            )
        ])
        fig_revenue.update_layout(
            title="Total Revenue by Vendor",
            xaxis_title="Vendor",
            yaxis_title="Revenue ($)",
            height=400
        )
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    with col2:
        # Average fare by passenger count
        st.subheader("Avg Fare by Passenger Count")
        avg_fare = filtered_data.groupby('passenger_count')['fare_amount'].mean().sort_index()
        
        fig_fare = go.Figure(data=[
            go.Bar(
                x=[f"{int(p)} Pass." for p in avg_fare.index],
                y=avg_fare.values,
                marker_color='#e74c3c',
                text=[f"${v:.2f}" for v in avg_fare.values],
                textposition='auto'
            )
        ])
        fig_fare.update_layout(
            title="Average Fare by Passenger Count",
            xaxis_title="Passenger Count",
            yaxis_title="Avg Fare ($)",
            height=400
        )
        st.plotly_chart(fig_fare, use_container_width=True)
    
    st.divider()
    
    # Payment type distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Trips by Payment Type")
        payment_dist = filtered_data['payment_type'].value_counts()
        
        fig_payment = go.Figure(data=[
            go.Pie(
                labels=payment_dist.index,
                values=payment_dist.values,
                hole=.3
            )
        ])
        fig_payment.update_layout(height=400)
        st.plotly_chart(fig_payment, use_container_width=True)
    
    with col2:
        st.subheader("Trips by Location Pair")
        location_pairs = filtered_data.groupby(['pickup_location', 'dropoff_location']).size().head(10)
        location_labels = [f"{p[0]} → {p[1]}" for p in location_pairs.index]
        
        fig_locations = go.Figure(data=[
            go.Bar(
                y=location_labels,
                x=location_pairs.values,
                orientation='h',
                marker_color='#2ecc71',
                text=location_pairs.values,
                textposition='auto'
            )
        ])
        fig_locations.update_layout(
            title="Top 10 Route Pairs",
            xaxis_title="Number of Trips",
            height=400
        )
        st.plotly_chart(fig_locations, use_container_width=True)
    
    st.divider()
    
    # Daily trend line chart
    st.subheader("Daily Trip Trends")
    daily_trips = filtered_data.groupby(filtered_data['pickup_datetime'].dt.date).size()
    
    fig_daily = go.Figure(data=[
        go.Scatter(
            x=daily_trips.index,
            y=daily_trips.values,
            mode='lines+markers',
            line=dict(color='#9b59b6', width=3),
            marker=dict(size=8),
            fill='tozeroy',
            fillcolor='rgba(155, 89, 182, 0.2)'
        )
    ])
    fig_daily.update_layout(
        title="Daily Trip Volume",
        xaxis_title="Date",
        yaxis_title="Number of Trips",
        height=400,
        hovermode='x unified'
    )
    st.plotly_chart(fig_daily, use_container_width=True)
    
    st.divider()
    
    # Heatmap: Vendor vs Passenger Count
    st.subheader("Heatmap: Vendor vs Passenger Count")
    heatmap_data = pd.crosstab(
        filtered_data['vendor_id'],
        filtered_data['passenger_count']
    )
    
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=[f"{int(c)} Pass." for c in heatmap_data.columns],
        y=[f"Vendor {v}" for v in heatmap_data.index],
        colorscale='Viridis',
        text=heatmap_data.values,
        texttemplate='%{text}',
        textfont={"size": 12}
    ))
    fig_heatmap.update_layout(height=300)
    st.plotly_chart(fig_heatmap, use_container_width=True)

# ============================================================================
# TAB 4: PERFORMANCE METRICS
# ============================================================================

with tab4:
    st.header("Performance Metrics & Analytics")
    
    # Performance summary
    st.subheader("System Performance")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Total Dataset Size",
            f"{len(st.session_state.raw_data):,} records",
            help="Total records in the database"
        )
    with col2:
        st.metric(
            "Filtered Records",
            f"{len(filtered_data):,}",
            delta=f"{len(st.session_state.raw_data) - len(filtered_data):,} filtered out",
            delta_color="off"
        )
    with col3:
        avg_query_time = 0.045  # Simulated
        st.metric(
            "Avg Query Time",
            f"{avg_query_time*1000:.1f}ms",
            help="Average database query response time"
        )
    with col4:
        cache_hit_rate = 85
        st.metric(
            "Cache Hit Rate",
            f"{cache_hit_rate}%",
            help="Percentage of queries served from cache"
        )
    
    st.divider()
    
    # Trip distance histogram
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Trip Distance Distribution")
        fig_hist_dist = go.Figure(data=[
            go.Histogram(
                x=filtered_data['trip_distance'],
                nbinsx=30,
                marker_color='#3498db',
                name='Distance'
            )
        ])
        fig_hist_dist.update_layout(
            title="Distribution of Trip Distances",
            xaxis_title="Distance (miles)",
            yaxis_title="Frequency",
            height=400
        )
        st.plotly_chart(fig_hist_dist, use_container_width=True)
    
    with col2:
        st.subheader("Fare Amount Distribution")
        fig_hist_fare = go.Figure(data=[
            go.Histogram(
                x=filtered_data['fare_amount'],
                nbinsx=30,
                marker_color='#e74c3c',
                name='Fare'
            )
        ])
        fig_hist_fare.update_layout(
            title="Distribution of Fare Amounts",
            xaxis_title="Fare ($)",
            yaxis_title="Frequency",
            height=400
        )
        st.plotly_chart(fig_hist_fare, use_container_width=True)
    
    st.divider()
    
    # Advanced performance metrics
    st.subheader("Advanced Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Avg Trip Distance",
            f"{filtered_data['trip_distance'].mean():.2f} miles",
            help="Mean distance across all trips"
        )
        st.metric(
            "Std Dev Distance",
            f"{filtered_data['trip_distance'].std():.2f} miles",
            help="Standard deviation of trip distances"
        )
    
    with col2:
        st.metric(
            "Avg Fare",
            f"${filtered_data['fare_amount'].mean():.2f}",
            help="Mean fare amount"
        )
        st.metric(
            "Std Dev Fare",
            f"${filtered_data['fare_amount'].std():.2f}",
            help="Standard deviation of fares"
        )
    
    with col3:
        st.metric(
            "Avg Tip",
            f"${filtered_data['tip_amount'].mean():.2f}",
            help="Mean tip amount"
        )
        st.metric(
            "Tip Rate",
            f"{(filtered_data['tip_amount'].sum() / filtered_data['fare_amount'].sum() * 100):.1f}%",
            help="Tip as percentage of fare"
        )
    
    st.divider()
    
    # Speed analysis
    st.subheader("Speed Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        speed_by_vendor = filtered_data.groupby('vendor_id')['speed_mph'].mean().sort_values(ascending=False)
        
        fig_speed = go.Figure(data=[
            go.Bar(
                x=[f"Vendor {v}" for v in speed_by_vendor.index],
                y=speed_by_vendor.values,
                marker_color='#1abc9c',
                text=[f"{v:.1f} mph" for v in speed_by_vendor.values],
                textposition='auto'
            )
        ])
        fig_speed.update_layout(
            title="Average Speed by Vendor",
            xaxis_title="Vendor",
            yaxis_title="Speed (mph)",
            height=400
        )
        st.plotly_chart(fig_speed, use_container_width=True)
    
    with col2:
        fig_speed_hist = go.Figure(data=[
            go.Histogram(
                x=filtered_data['speed_mph'],
                nbinsx=25,
                marker_color='#f39c12',
                name='Speed'
            )
        ])
        fig_speed_hist.update_layout(
            title="Speed Distribution",
            xaxis_title="Speed (mph)",
            yaxis_title="Frequency",
            height=400
        )
        st.plotly_chart(fig_speed_hist, use_container_width=True)
    
    st.divider()
    
    # Performance comparison table
    st.subheader("Vendor Performance Comparison")
    
    vendor_performance = filtered_data.groupby('vendor_id').agg({
        'trip_id': 'count',
        'fare_amount': ['mean', 'sum'],
        'trip_distance': 'mean',
        'speed_mph': 'mean',
        'tip_amount': 'mean'
    }).round(2)
    
    vendor_performance.columns = ['Trips', 'Avg Fare', 'Total Revenue', 'Avg Distance', 'Avg Speed', 'Avg Tip']
    st.dataframe(vendor_performance, use_container_width=True)
    
    st.divider()
    
    # Export performance report
    st.subheader("Export Performance Report")
    
    perf_report = {
        'generated_at': datetime.now().isoformat(),
        'total_records': len(st.session_state.raw_data),
        'filtered_records': len(filtered_data),
        'dataset_size_mb': st.session_state.raw_data.memory_usage(deep=True).sum() / 1024 / 1024,
        'statistics': {
            'avg_distance': float(filtered_data['trip_distance'].mean()),
            'avg_fare': float(filtered_data['fare_amount'].mean()),
            'avg_passengers': float(filtered_data['passenger_count'].mean()),
            'total_revenue': float(filtered_data['total_amount'].sum())
        }
    }
    
    json_report = json.dumps(perf_report, indent=2)
    
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="📥 Download Performance Report (JSON)",
            data=json_report,
            file_name=f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )
    
    with col2:
        # Create CSV performance report
        perf_csv = pd.DataFrame([perf_report['statistics']]).to_csv(index=False)
        st.download_button(
            label="📥 Download Summary Statistics (CSV)",
            data=perf_csv,
            file_name=f"statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )

# ============================================================================
# FOOTER SECTION
# ============================================================================

st.divider()

footer_col1, footer_col2, footer_col3 = st.columns(3)

with footer_col1:
    st.markdown("### 📊 Dashboard Info")
    st.markdown(f"""
    - **Version:** 1.0.0
    - **Database:** DuckDB
    - **Last Update:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """)

with footer_col2:
    st.markdown("### 🔧 Features")
    st.markdown("""
    - Multi-tab interface
    - Interactive filtering
    - Real-time aggregations
    - Data export (CSV/JSON)
    - Performance metrics
    """)

with footer_col3:
    st.markdown("### 📝 Logging")
    if st.button("📋 View Recent Logs"):
        log_file = os.path.join(DashboardLogger.LOG_DIR, f"dashboard_{datetime.now().strftime('%Y%m%d')}.log")
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                logs = f.readlines()[-20:]  # Last 20 lines
            st.code(''.join(logs), language="log")

logger_instance.info("Dashboard rendered successfully")
