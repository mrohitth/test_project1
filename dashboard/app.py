import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Page title and header
st.title("📊 Data Dashboard")
st.markdown("---")

# Load data
@st.cache_data
def load_data():
    """Load processed data from parquet file"""
    try:
        data_path = Path("/content/processed_data.parquet")
        if data_path.exists():
            df = pd.read_parquet(data_path)
            return df
        else:
            st.error(f"Data file not found at {data_path}")
            return None
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

# Load the data
df = load_data()

if df is not None:
    # Display basic information
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", len(df))
    
    with col2:
        st.metric("Total Columns", len(df.columns))
    
    with col3:
        st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    with col4:
        st.metric("Data Shape", f"{df.shape[0]} x {df.shape[1]}")
    
    st.markdown("---")
    
    # Sidebar filters
    st.sidebar.header("🔧 Filters & Options")
    
    # Display data info
    if st.sidebar.checkbox("Show Data Info", value=True):
        st.subheader("📋 Dataset Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Data Types:**")
            st.write(df.dtypes)
        
        with col2:
            st.write("**Missing Values:**")
            st.write(df.isnull().sum())
    
    # Display raw data
    if st.sidebar.checkbox("Show Raw Data"):
        st.subheader("📄 Raw Data")
        
        # Add pagination
        page_size = st.sidebar.slider("Rows per page", 5, 100, 20)
        
        num_pages = (len(df) // page_size) + (1 if len(df) % page_size > 0 else 0)
        page = st.sidebar.number_input("Page", 1, num_pages, 1)
        
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True)
        st.caption(f"Showing rows {start_idx + 1} to {min(end_idx, len(df))} of {len(df)}")
    
    # Statistical summary
    if st.sidebar.checkbox("Show Statistics"):
        st.subheader("📈 Statistical Summary")
        st.write(df.describe())
    
    # Data visualizations
    st.subheader("📊 Data Visualizations")
    
    # Get numeric columns for visualization
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    if numeric_cols:
        # Histogram
        col1, col2 = st.columns(2)
        
        with col1:
            if st.checkbox("Show Histogram", value=True):
                selected_col = st.selectbox(
                    "Select numeric column for histogram",
                    numeric_cols,
                    key="hist"
                )
                fig = px.histogram(
                    df,
                    x=selected_col,
                    title=f"Distribution of {selected_col}",
                    nbins=30,
                    template="plotly_white"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if st.checkbox("Show Box Plot", value=True):
                selected_col = st.selectbox(
                    "Select numeric column for box plot",
                    numeric_cols,
                    key="boxplot"
                )
                fig = px.box(
                    df,
                    y=selected_col,
                    title=f"Box Plot of {selected_col}",
                    template="plotly_white"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Scatter plot for multiple numeric columns
    if len(numeric_cols) >= 2:
        if st.checkbox("Show Scatter Plot"):
            col1, col2 = st.columns(2)
            
            with col1:
                x_col = st.selectbox(
                    "Select X-axis column",
                    numeric_cols,
                    key="scatter_x"
                )
            
            with col2:
                y_col = st.selectbox(
                    "Select Y-axis column",
                    numeric_cols,
                    index=1 if len(numeric_cols) > 1 else 0,
                    key="scatter_y"
                )
            
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                title=f"{x_col} vs {y_col}",
                template="plotly_white"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Categorical visualizations
    if categorical_cols:
        if st.checkbox("Show Category Distribution"):
            selected_cat = st.selectbox(
                "Select categorical column",
                categorical_cols,
                key="category"
            )
            
            fig = px.bar(
                df[selected_cat].value_counts().reset_index(),
                x=selected_cat,
                y='count',
                title=f"Distribution of {selected_cat}",
                template="plotly_white",
                labels={'count': 'Count', selected_cat: selected_cat}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Correlation heatmap
    if len(numeric_cols) > 1:
        if st.checkbox("Show Correlation Heatmap"):
            corr_matrix = df[numeric_cols].corr()
            fig = px.imshow(
                corr_matrix,
                title="Correlation Matrix",
                template="plotly_white",
                color_continuous_scale="RdBu",
                zmin=-1,
                zmax=1
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.caption("Dashboard created on: 2026-01-14 05:47:35 UTC")

else:
    st.error("Unable to load data. Please check the data file path and try again.")
    st.info("Expected path: /content/processed_data.parquet")
