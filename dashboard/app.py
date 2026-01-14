import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# Set page configuration
st.set_page_config(
    page_title="Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("📊 Data Analysis Dashboard")

# Sidebar configuration
st.sidebar.header("Dashboard Controls")

# Load and process data
@st.cache_data
def load_data():
    """Load sample data for dashboard"""
    data = {
        'date': pd.date_range('2025-01-01', periods=30, freq='D'),
        'sales': [100 + i*5 for i in range(30)],
        'customers': [20 + i*2 for i in range(30)],
        'revenue': [1000 + i*50 for i in range(30)]
    }
    df = pd.DataFrame(data)
    
    # Fix PyArrow serialization: Convert datetime to string for Streamlit caching
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Ensure numeric columns are proper types
    df['sales'] = df['sales'].astype('int64')
    df['customers'] = df['customers'].astype('int64')
    df['revenue'] = df['revenue'].astype('int64')
    
    return df

# Load data
df = load_data()

# Convert date back to datetime for plotting
df['date'] = pd.to_datetime(df['date'])

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["Overview", "Detailed Analysis", "Data Table"])

with tab1:
    st.subheader("Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Sales", f"${df['revenue'].sum():,.0f}")
    
    with col2:
        st.metric("Avg Daily Sales", f"${df['revenue'].mean():,.0f}")
    
    with col3:
        st.metric("Total Customers", df['customers'].sum())
    
    with col4:
        st.metric("Avg Customers/Day", f"{df['customers'].mean():.0f}")
    
    st.markdown("---")
    
    # Line chart
    st.subheader("Revenue Trend")
    fig_line = px.line(
        df,
        x='date',
        y='revenue',
        title='Daily Revenue',
        markers=True,
        template='plotly_white'
    )
    fig_line.update_xaxes(title_text="Date")
    fig_line.update_yaxes(title_text="Revenue ($)")
    st.plotly_chart(fig_line, use_container_width=True)

with tab2:
    st.subheader("Sales vs Customers")
    
    # Prepare data for scatter plot - ensure compatible types
    plot_df = df.copy()
    plot_df['sales'] = pd.to_numeric(plot_df['sales'], errors='coerce')
    plot_df['customers'] = pd.to_numeric(plot_df['customers'], errors='coerce')
    
    fig_scatter = px.scatter(
        plot_df,
        x='customers',
        y='sales',
        color='revenue',
        size='revenue',
        hover_data=['date'],
        title='Customer Count vs Sales',
        template='plotly_white'
    )
    st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Bar chart
    st.subheader("Daily Breakdown")
    fig_bar = px.bar(
        df,
        x='date',
        y=['sales', 'customers'],
        title='Sales and Customers by Day',
        barmode='group',
        template='plotly_white'
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with tab3:
    st.subheader("Data Table")
    
    # Prepare table data with compatible types
    table_df = df.copy()
    table_df['date'] = table_df['date'].dt.strftime('%Y-%m-%d')
    
    # Format columns for display
    display_df = table_df.copy()
    display_df['sales'] = display_df['sales'].astype('int64')
    display_df['customers'] = display_df['customers'].astype('int64')
    display_df['revenue'] = display_df['revenue'].astype('int64')
    
    st.dataframe(display_df, use_container_width=True)
    
    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="Download as CSV",
        data=csv,
        file_name="dashboard_data.csv",
        mime="text/csv"
    )

# Footer
st.markdown("---")
st.markdown(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
