import pandas as pd
import streamlit as st
from datetime import datetime

# Set page configuration FIRST
st.set_page_config(
    page_title="Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Title
st.title("📊 Data Analysis Dashboard")

# Show loading message while data loads
with st.spinner("Loading data..."):
    @st.cache_data(ttl=3600)
    def load_data():
        """Load sample data for dashboard - cached for 1 hour"""
        try:
            data = {
                'date': pd.date_range('2025-01-01', periods=30, freq='D'),
                'sales': [100 + i*5 for i in range(30)],
                'customers': [20 + i*2 for i in range(30)],
                'revenue': [1000 + i*50 for i in range(30)]
            }
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return None
    
    # Load data
    df = load_data()

# Check if data loaded successfully
if df is None:
    st.error("Failed to load data. Please refresh the page.")
    st.stop()

# Success indicator
st.success("✅ Data loaded successfully")

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["📊 Overview", "📈 Analysis", "📋 Data"])

with tab1:
    st.subheader("Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Revenue", f"${df['revenue'].sum():,.0f}")
    
    with col2:
        st.metric("Avg Daily Revenue", f"${df['revenue'].mean():,.0f}")
    
    with col3:
        st.metric("Total Customers", df['customers'].sum())
    
    with col4:
        st.metric("Avg Customers/Day", f"{df['customers'].mean():.0f}")
    
    st.markdown("---")
    
    # Simple line chart (native Streamlit - very fast)
    st.subheader("Revenue Trend")
    chart_data = df.set_index('date')[['revenue']].copy()
    st.line_chart(chart_data, use_container_width=True)

with tab2:
    st.subheader("Sales Analysis")
    
    # Simple bar chart
    st.subheader("Sales & Customers by Day")
    bar_data = df.set_index('date')[['sales', 'customers']].copy()
    st.bar_chart(bar_data, use_container_width=True)
    
    st.markdown("---")
    
    st.subheader("Summary Statistics")
    st.write(df[['sales', 'customers', 'revenue']].describe())

with tab3:
    st.subheader("Data Table")
    
    # Format display
    display_df = df.copy()
    display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
    
    # Use st.dataframe without deprecated parameter
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name="dashboard_data.csv",
        mime="text/csv"
    )

# Footer
st.markdown("---")
st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC*")
st.markdown("*Data is cached for 1 hour. Refresh browser to clear cache.*")