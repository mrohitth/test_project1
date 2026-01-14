"""
Simplified Dashboard Application
Loads data once and displays simple metrics without complex visualizations.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st


# Configuration
st.set_page_config(
    page_title="Simplified Dashboard",
    page_icon="📊",
    layout="wide"
)

# Load data once at startup using Streamlit's caching
@st.cache_data
def load_data():
    """Load data from CSV file."""
    data_path = Path(__file__).parent.parent / "data" / "data.csv"
    
    if not data_path.exists():
        st.warning(f"Data file not found at {data_path}")
        return None
    
    try:
        df = pd.read_csv(data_path)
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None


def main():
    """Main application function."""
    st.title("📊 Simplified Dashboard")
    st.markdown("---")
    
    # Load data
    df = load_data()
    
    if df is None or df.empty:
        st.error("No data available to display.")
        return
    
    # Display basic information
    st.subheader("Data Overview")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Records", len(df))
    
    with col2:
        st.metric("Total Columns", len(df.columns))
    
    with col3:
        st.metric("Data Shape", f"{len(df)} × {len(df.columns)}")
    
    st.markdown("---")
    
    # Display simple metrics
    st.subheader("Simple Metrics")
    
    # Numeric columns analysis
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    if numeric_cols:
        metrics_data = {}
        for col in numeric_cols:
            metrics_data[col] = {
                "Mean": f"{df[col].mean():.2f}",
                "Min": f"{df[col].min():.2f}",
                "Max": f"{df[col].max():.2f}",
                "Count": f"{df[col].count()}"
            }
        
        for col, metrics in metrics_data.items():
            st.write(f"**{col}**")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Mean", metrics["Mean"])
            with col2:
                st.metric("Min", metrics["Min"])
            with col3:
                st.metric("Max", metrics["Max"])
            with col4:
                st.metric("Count", metrics["Count"])
    
    st.markdown("---")
    
    # Display data table
    st.subheader("Data Table")
    st.dataframe(df, use_container_width=True)
    
    st.markdown("---")
    st.info("Dashboard loaded successfully. All data is cached for optimal performance.")


if __name__ == "__main__":
    main()
