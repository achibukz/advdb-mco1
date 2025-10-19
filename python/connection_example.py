"""
Example: Using st.connection() for MySQL Database

This shows the modern Streamlit way to connect to databases.
"""

import streamlit as st
import pandas as pd

st.title("Database Connection Example")

# =============================================================================
# METHOD 1: Using st.connection() directly (RECOMMENDED for Streamlit)
# =============================================================================
st.header("Method 1: st.connection() (Recommended)")

try:
    # Create connection using the 'mysql' connection from secrets.toml
    conn = st.connection('mysql', type='sql')
    
    # Query with automatic caching (ttl=600 means cache for 10 minutes)
    df = conn.query("SELECT * FROM DimDistrict LIMIT 10;", ttl=600)
    
    st.success("✅ Connection successful using st.connection()!")
    st.dataframe(df)
    
except Exception as e:
    st.error(f"❌ Connection failed: {str(e)}")

# =============================================================================
# METHOD 2: Using db_config.py (WORKS EVERYWHERE)
# =============================================================================
st.header("Method 2: Using db_config.py")

try:
    from db_config import fetch_data
    
    # This automatically uses st.connection() when in Streamlit
    # or falls back to direct connection when not in Streamlit
    df = fetch_data("SELECT * FROM DimDistrict LIMIT 10;", ttl=600)
    
    st.success("✅ Connection successful using db_config!")
    st.dataframe(df)
    
except Exception as e:
    st.error(f"❌ Connection failed: {str(e)}")

# =============================================================================
# READING FROM GOOGLE CLOUD STORAGE (Your bucket example)
# =============================================================================
st.header("Bonus: Reading from Google Cloud Storage")

try:
    from st_files_connection import FilesConnection
    
    # Create GCS connection
    gcs_conn = st.connection('gcs', type=FilesConnection)
    
    # Read a file from your bucket
    # Replace with your actual bucket and file path
    st.info("To read from GCS bucket 'warehouse-db-stadvdb':")
    st.code('''
# Read SQL file from GCS
sql_content = gcs_conn.read(
    "warehouse-db-stadvdb/warehouse_db.sql", 
    input_format="text",
    ttl=600
)
st.text_area("SQL File Content", sql_content, height=300)
    ''')
    
except Exception as e:
    st.warning(f"GCS connection not configured or file not found: {str(e)}")

# =============================================================================
# CONNECTION INFO
# =============================================================================
with st.expander("ℹ️ Connection Configuration"):
    st.markdown("""
    ### How it works:
    
    1. **secrets.toml** stores your database credentials
    2. **st.connection()** automatically:
       - Reads credentials from secrets
       - Manages connection pooling
       - Caches query results (controlled by `ttl` parameter)
       - Handles retries on failure
    
    ### Benefits of st.connection():
    - ✅ Automatic caching with TTL
    - ✅ Connection pooling (reuses connections)
    - ✅ Better error handling
    - ✅ No manual connection management
    - ✅ Works seamlessly with Streamlit Cloud
    
    ### Cache TTL Examples:
    - `ttl=0` - No caching (always fetch fresh)
    - `ttl=600` - Cache for 10 minutes
    - `ttl=3600` - Cache for 1 hour
    - `ttl=None` - Cache forever (until app restarts)
    """)
