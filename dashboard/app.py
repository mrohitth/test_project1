import pandas as pd
import streamlit as st
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Title
st.title("📊 Data Analysis Dashboard")

# Aggressive caching to prevent re-runs
@st.cache_data(ttl=3600)
def load_data():
    """Load sample data for dashboard"""
    data = {
        'date': pd.date_range('2025-01-01', periods=30, freq='D'),
        'sales': [100 + i*5 for i in range(30)],
        'customers': [20 + i*2 for i in range(30)],
        'revenue': [1000 + i*50 for i in range(30)]
    }
    df = pd.DataFrame(data)
    return df

# Load data once and cache it
df = load_data()

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["Overview", "Analysis", "Data"])

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
    
    # Simple line chart (faster than Plotly)
    st.subheader("Revenue Trend")
    st.line_chart(df.set_index('date')['revenue'])

with tab2:
    st.subheader("Sales Analysis")
    
    # Simple bar chart
    st.subheader("Sales by Day")
    st.bar_chart(df.set_index('date')[['sales', 'customers']])

with tab3:
    st.subheader("Data Table")
    
    # Format display
    display_df = df.copy()
    display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
    
    st.dataframe(display_df, width='stretch')
    
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
