import streamlit as st
import pandas as pd
from pathlib import Path

# Set page configuration
st.set_page_config(
    page_title="Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Page title
st.title("📊 Data Dashboard")
st.markdown("---")

# Load and convert data types
@st.cache_data
def load_data():
    """Load processed data and fix type incompatibilities"""
    try:
        data_path = Path("/content/processed_data.parquet")
        if data_path.exists():
            df = pd.read_parquet(data_path)
            
            # Convert int32 → int64 (Arrow compatible)
            int32_cols = df.select_dtypes(include=['int32']).columns
            for col in int32_cols: 
                df[col] = df[col]. astype('int64')
            
            # Convert datetime → string (Arrow compatible)
            datetime_cols = df.select_dtypes(include=['datetime64']).columns
            for col in datetime_cols:
                df[col] = df[col].astype(str)
            
            # Convert float32 → float64 (Arrow compatible)
            float32_cols = df.select_dtypes(include=['float32']).columns
            for col in float32_cols:
                df[col] = df[col].astype('float64')
            
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
    # Display basic metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", len(df))
    
    with col2:
        st.metric("Total Columns", len(df.columns))
    
    with col3:
        st.metric("Memory Usage", f"{df. memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    with col4:
        st.metric("Data Shape", f"{df.shape[0]} x {df.shape[1]}")
    
    st.markdown("---")
    
    # Sidebar options
    st.sidebar. header("🔧 Display Options")
    
    # Show column info
    if st.sidebar. checkbox("Show Column Info", value=False):
        st.subheader("📋 Column Information")
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Data Types:**")
            st.write(df. dtypes)
        with col2:
            st.write("**Missing Values:**")
            missing = df.isnull().sum()
            st.write(missing[missing > 0] if missing. sum() > 0 else "No missing values!")
    
    # Show data table
    if st.sidebar. checkbox("Show Data Table", value=True):
        st.subheader("📄 Data Table")
        
        # Pagination
        page_size = st.sidebar.slider("Rows per page", 5, 50, 10)
        total_pages = (len(df) // page_size) + (1 if len(df) % page_size > 0 else 0)
        page = st.sidebar.number_input("Page", 1, total_pages, 1)
        
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        st.dataframe(df. iloc[start_idx:end_idx])
        st.caption(f"Showing rows {start_idx + 1} to {min(end_idx, len(df))} of {len(df)}")
    
    # Show statistics
    if st.sidebar.checkbox("Show Statistics", value=False):
        st.subheader("📊 Statistical Summary")
        numeric_df = df.select_dtypes(include=['int64', 'float64'])
        if not numeric_df.empty:
            st.write(numeric_df.describe())
        else:
            st.info("No numeric columns available")
    
    st.markdown("---")
    st.success("✅ Dashboard loaded successfully!")
    
else:
    st. error("Unable to load data. Please check the file path.")
    st.info("Expected path: /content/processed_data.parquet")
