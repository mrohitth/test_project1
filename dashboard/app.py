import streamlit as st
import pandas as pd
import os
from pathlib import Path

# Set page configuration
st.set_page_config(page_title="Data Dashboard", layout="wide")

# Title and description
st.title("📊 Data Dashboard")
st.markdown("Display and analyze processed data")

# Automatically load processed_data.parquet from the working directory
@st.cache_data
def load_data():
    """Load processed_data.parquet from the working directory."""
    parquet_path = Path("processed_data.parquet")
    
    if not parquet_path.exists():
        st.error(f"Error: processed_data.parquet not found in {os.getcwd()}")
        st.info("Please ensure processed_data.parquet is in the working directory.")
        return None
    
    try:
        df = pd.read_parquet(parquet_path)
        return df
    except Exception as e:
        st.error(f"Error loading parquet file: {str(e)}")
        return None

# Load the data
df = load_data()

if df is not None:
    # Display data summary
    st.subheader("📈 Data Summary")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Rows", len(df))
    with col2:
        st.metric("Total Columns", len(df.columns))
    with col3:
        st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Display dataset info
    st.subheader("📋 Dataset Information")
    st.write(df.info())
    
    # Display first rows
    st.subheader("👀 Data Preview")
    st.dataframe(df.head(10))
    
    # Display statistics
    st.subheader("📊 Statistical Summary")
    st.dataframe(df.describe())
    
    # Column information
    st.subheader("🔍 Column Details")
    selected_column = st.selectbox("Select a column to view details:", df.columns)
    if selected_column:
        st.write(f"**Column:** {selected_column}")
        st.write(f"**Data Type:** {df[selected_column].dtype}")
        st.write(f"**Non-null Count:** {df[selected_column].notna().sum()}")
        st.write(f"**Null Count:** {df[selected_column].isna().sum()}")
        
        if df[selected_column].dtype in ['int64', 'float64']:
            st.write(f"**Min:** {df[selected_column].min()}")
            st.write(f"**Max:** {df[selected_column].max()}")
            st.write(f"**Mean:** {df[selected_column].mean():.4f}")
            st.write(f"**Std Dev:** {df[selected_column].std():.4f}")
else:
    st.warning("Could not load data. Please check that processed_data.parquet exists in the working directory.")
