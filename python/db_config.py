"""
Database Configuration and Connection Module

This module handles all database connections for the Financial Reports Dashboard.
It supports both local MySQL connections and Google Cloud SQL connections.
"""

import mysql.connector
import pandas as pd
import hashlib
import pickle
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Cloud SQL connector will be initialized only when needed
_connector = None

# Query Cache Configuration
CACHE_ENABLED = os.getenv('CACHE_ENABLED', 'True').lower() == 'true'
CACHE_TTL_SECONDS = int(os.getenv('CACHE_TTL_SECONDS'))
_query_cache = {}  # In-memory cache storage

# Database Configuration
# Choose connection method by setting USE_CLOUD_SQL environment variable
USE_CLOUD_SQL = os.getenv('USE_CLOUD_SQL', 'False').lower() == 'true'

# Google Cloud SQL Configuration (loaded from environment variables)
CLOUD_SQL_CONFIG = {
    "host": os.getenv('CLOUD_DB_HOST'),
    "port": int(os.getenv('CLOUD_DB_PORT')),
    "user": os.getenv('CLOUD_DB_USER'),
    "password": os.getenv('CLOUD_DB_PASSWORD'),
    "database": os.getenv('CLOUD_DB_NAME')
}

# Direct IP Connection Configuration (loaded from environment variables)
LOCAL_CONFIG = {
    "host": os.getenv('LOCAL_DB_HOST'),
    "port": int(os.getenv('LOCAL_DB_PORT')),
    "user": os.getenv('LOCAL_DB_USER'),
    "password": os.getenv('LOCAL_DB_PASSWORD'),
    "database": os.getenv('LOCAL_DB_NAME')
}

def _generate_cache_key(query):
    """
    Generate a unique cache key for a query.
    
    Args:
        query (str): SQL query string
        
    Returns:
        str: MD5 hash of the query
    """
    # Normalize query: strip whitespace and convert to lowercase
    normalized_query = ' '.join(query.strip().lower().split())
    return hashlib.md5(normalized_query.encode()).hexdigest()


def _is_cache_valid(cache_entry):
    """
    Check if a cache entry is still valid based on TTL.
    
    Args:
        cache_entry (dict): Cache entry with 'timestamp' and 'data' keys
        
    Returns:
        bool: True if cache is valid, False if expired
    """
    if not CACHE_ENABLED:
        return False
    
    timestamp = cache_entry.get('timestamp')
    if not timestamp:
        return False
    
    age = (datetime.now() - timestamp).total_seconds()
    return age < CACHE_TTL_SECONDS


def clear_cache():
    """
    Clear all cached queries.
    Useful for forcing fresh data from the database.
    """
    global _query_cache
    _query_cache = {}
    print("Query cache cleared successfully.")


def get_cache_stats():
    """
    Get statistics about the current cache.
    
    Returns:
        dict: Cache statistics including size, entries, and memory usage
    """
    total_entries = len(_query_cache)
    valid_entries = sum(1 for entry in _query_cache.values() if _is_cache_valid(entry))
    
    # Calculate approximate memory usage
    try:
        cache_size_bytes = len(pickle.dumps(_query_cache))
        cache_size_mb = cache_size_bytes / (1024 * 1024)
    except:
        cache_size_mb = 0
    
    return {
        'total_entries': total_entries,
        'valid_entries': valid_entries,
        'expired_entries': total_entries - valid_entries,
        'cache_size_mb': round(cache_size_mb, 2),
        'cache_enabled': CACHE_ENABLED,
        'ttl_seconds': CACHE_TTL_SECONDS
    }


def get_db_connection():
    """
    Establish and return a database connection.
    
    Returns:
        mysql.connector.connection: Database connection object
        
    Raises:
        Exception: If connection fails
    """
    
    # Select appropriate configuration based on USE_CLOUD_SQL setting
    config = CLOUD_SQL_CONFIG if USE_CLOUD_SQL else LOCAL_CONFIG
    config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"
    
    try:
        conn = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to {config_type} database at {config['host']}:{config['port']}\n"
                      f"Error: {str(e)}")


def fetch_data(query):
    """
    Execute a SQL query and return results as a pandas DataFrame.
    Results are cached to avoid repeated database queries.
    
    Args:
        query (str): SQL query to execute
        
    Returns:
        pandas.DataFrame: Query results (from cache or fresh from database)
        
    Raises:
        Exception: If query execution fails
    """
    # Generate cache key
    cache_key = _generate_cache_key(query)
    
    # Check if valid cached result
    if CACHE_ENABLED and cache_key in _query_cache:
        cache_entry = _query_cache[cache_key]
        if _is_cache_valid(cache_entry):
            # Return cached data (create a copy to prevent modifications)
            return cache_entry['data'].copy()
        else:
            # Remove expired cache entry
            del _query_cache[cache_key]
    
    # Cache miss or expired - fetch from database
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        result_df = pd.DataFrame(data)
        
        # Store in cache
        if CACHE_ENABLED:
            _query_cache[cache_key] = {
                'timestamp': datetime.now(),
                'data': result_df.copy(),
                'query': query[:100]  # Store first 100 chars for debugging
            }
        
        return result_df
    
    except Exception as e:
        raise Exception(f"Failed to fetch data: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def execute_multi_statement_query(query):
    """
    Execute a multi-statement SQL query and return the final SELECT results.
    Useful for queries that create temporary tables before selecting data.
    Results are cached to avoid repeated database queries.
    
    Args:
        query (str): Multi-statement SQL query (statements separated by semicolons)
        
    Returns:
        pandas.DataFrame: Results from the final SELECT statement (from cache or fresh)
        
    Raises:
        Exception: If query execution fails
    """
    # Generate cache key
    cache_key = _generate_cache_key(query)
    
    # Check if we have a valid cached result
    if CACHE_ENABLED and cache_key in _query_cache:
        cache_entry = _query_cache[cache_key]
        if _is_cache_valid(cache_entry):
            # Return cached data (create a copy to prevent modifications)
            return cache_entry['data'].copy()
        else:
            # Remove expired cache entry
            del _query_cache[cache_key]
    
    # Cache miss or expired - fetch from database
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Split the query into individual statements
        statements = [s.strip() for s in query.split(';') if s.strip()]
        
        # Execute all statements except the last one
        for statement in statements[:-1]:
            cursor.execute(statement)
            try:
                cursor.fetchall()  # Consume results to avoid "Unread result found" error
            except:
                pass  # Ignore if there are no results to fetch
        
        # Execute the final SELECT statement and fetch results
        cursor.execute(statements[-1])
        result_df = pd.DataFrame(cursor.fetchall())
        
        # Store in cache
        if CACHE_ENABLED:
            _query_cache[cache_key] = {
                'timestamp': datetime.now(),
                'data': result_df.copy(),
                'query': query[:100]  # Store first 100 chars for debugging
            }
        
        return result_df
    
    except Exception as e:
        raise Exception(f"Failed to execute multi-statement query: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def test_connection():
    """
    Test the database connection.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("Database connection successful!")
        return True
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return False


if __name__ == "__main__":
    # Test the connection when running this file directly
    print("Testing database connection...")
    print(f"Connection mode: {'Cloud SQL' if USE_CLOUD_SQL else 'Local/Direct IP'}")
    
    config = CLOUD_SQL_CONFIG if USE_CLOUD_SQL else LOCAL_CONFIG
    if config['host'] and config['user']:
        print(f"Host: {config['host']}:{config['port']}")
        print(f"User: {config['user']}")
        print(f"Database: {config['database']}")
        test_connection()
    else:
        print("Configuration incomplete! Please check your .env file.")
        print("Required environment variables are missing.")
