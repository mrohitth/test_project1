import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import pyarrow.parquet as pq
from functools import lru_cache
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Optimized Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding: 0rem 0rem;
    }
    </style>
    """, unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_parquet_data(file_path: str) -> pd.DataFrame:
    """
    Load data from Parquet file with caching.
    
    Args:
        file_path: Path to the Parquet file
        
    Returns:
        DataFrame loaded from Parquet
    """
    try:
        logger.info(f"Loading data from {file_path}")
        df = pd.read_parquet(file_path)
        logger.info(f"Successfully loaded {len(df)} rows")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        st.error(f"Data file not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def get_data_summary(df: pd.DataFrame) -> dict:
    """
    Calculate summary statistics with caching.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary containing summary statistics
    """
    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "memory_usage": df.memory_usage(deep=True).sum() / 1024**2,  # MB
        "missing_values": df.isnull().sum().to_dict()
    }


def create_dashboard_header():
    """Create the dashboard header and title."""
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📊 Optimized Data Dashboard")
        st.markdown("_Real-time analytics with Streamlit and Parquet optimization_")
    with col2:
        st.metric("Current Time", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))


def display_data_summary(df: pd.DataFrame):
    """Display data summary metrics."""
    if df.empty:
        st.warning("No data available")
        return
    
    summary = get_data_summary(df)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Rows", f"{summary['total_rows']:,}")
    with col2:
        st.metric("Total Columns", summary['total_columns'])
    with col3:
        st.metric("Memory Usage", f"{summary['memory_usage']:.2f} MB")
    with col4:
        missing_total = sum(summary['missing_values'].values())
        st.metric("Missing Values", missing_total)


def display_data_explorer(df: pd.DataFrame):
    """Display data explorer section."""
    st.subheader("Data Explorer")
    
    col1, col2 = st.columns(2)
    
    with col1:
        rows_to_show = st.slider(
            "Rows to display",
            min_value=5,
            max_value=min(100, len(df)),
            value=10,
            step=5
        )
    
    with col2:
        if st.checkbox("Show column information"):
            st.write(df.dtypes)
    
    st.dataframe(
        df.head(rows_to_show),
        use_container_width=True,
        height=400
    )


def display_statistics(df: pd.DataFrame):
    """Display statistical analysis."""
    st.subheader("Statistical Analysis")
    
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    
    if numeric_columns:
        selected_column = st.selectbox(
            "Select column for analysis",
            numeric_columns
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Mean", f"{df[selected_column].mean():.2f}")
        with col2:
            st.metric("Median", f"{df[selected_column].median():.2f}")
        with col3:
            st.metric("Std Dev", f"{df[selected_column].std():.2f}")
        
        # Distribution histogram
        fig = px.histogram(
            df,
            x=selected_column,
            nbins=30,
            title=f"Distribution of {selected_column}",
            labels={selected_column: selected_column}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No numeric columns found in the dataset")


def display_correlations(df: pd.DataFrame):
    """Display correlation heatmap."""
    st.subheader("Correlation Analysis")
    
    numeric_df = df.select_dtypes(include=['number'])
    
    if len(numeric_df.columns) > 1:
        corr_matrix = numeric_df.corr()
        
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.columns,
            colorscale='RdBu',
            zmid=0,
            text=corr_matrix.values,
            texttemplate='%{text:.2f}',
            textfont={"size": 10}
        ))
        
        fig.update_layout(
            title="Correlation Matrix",
            height=600,
            width=600
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Need at least 2 numeric columns for correlation analysis")


def main():
    """Main dashboard application."""
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Data source selection
        data_source = st.radio(
            "Select data source",
            ["Upload Parquet File", "Sample Data"]
        )
        
        if data_source == "Upload Parquet File":
            uploaded_file = st.file_uploader(
                "Choose a Parquet file",
                type=["parquet", "pq"]
            )
            
            if uploaded_file is not None:
                # Save uploaded file temporarily
                temp_path = Path(f"/tmp/{uploaded_file.name}")
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                df = load_parquet_data(str(temp_path))
            else:
                df = pd.DataFrame()
        else:
            # Generate sample data
            df = pd.DataFrame({
                'ID': range(1, 1001),
                'Value_A': pd.np.random.randn(1000),
                'Value_B': pd.np.random.randn(1000),
                'Category': pd.np.random.choice(['A', 'B', 'C'], 1000),
                'Date': pd.date_range('2024-01-01', periods=1000)
            })
        
        st.markdown("---")
        st.info("💡 This dashboard uses Streamlit's caching for optimal performance with Parquet files")
    
    # Main content
    create_dashboard_header()
    
    if not df.empty:
        # Display summary metrics
        display_data_summary(df)
        
        st.markdown("---")
        
        # Create tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(
            ["📈 Overview", "🔍 Explorer", "📊 Statistics", "🔗 Correlations"]
        )
        
        with tab1:
            st.subheader("Quick Overview")
            st.write(df.describe().T)
        
        with tab2:
            display_data_explorer(df)
        
        with tab3:
            display_statistics(df)
        
        with tab4:
            display_correlations(df)
    else:
        st.warning("👈 Please select a data source from the sidebar")


if __name__ == "__main__":
    main()
