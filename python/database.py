"""
Database utility module for connecting to MySQL databases
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pymysql
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Warehouse database configuration
WAREHOUSE_DB_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'warehouse_user',
    'password': 'rootpass',
    'database': 'warehouse_db',
    'charset': 'utf8mb4',
    'autocommit': True
}

def get_warehouse_connection():
    """Get connection to warehouse database"""
    try:
        # Try with SSL disabled and specific auth method
        config = WAREHOUSE_DB_CONFIG.copy()
        config['ssl_disabled'] = True
        config['auth_plugin_map'] = {
            'mysql_native_password': 'mysql_native_password',
            'caching_sha2_password': 'mysql_native_password'
        }
        conn = pymysql.connect(**config)
        logger.info("Connected to warehouse database successfully")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to warehouse database: {e}")
        raise


class DatabaseConnection:
    """Helper class for database operations"""
    
    def __init__(self, db_type='source'):
        """
        Initialize database connection
        
        Args:
            db_type (str): 'source' or 'warehouse'
        """
        self.db_type = db_type
        self.engine = self._create_engine()
    
    def _create_engine(self):
        """Create SQLAlchemy engine based on db_type"""
        if self.db_type == 'source':
            host = os.getenv('SOURCE_DB_HOST', 'localhost')
            port = os.getenv('SOURCE_DB_PORT', '3306')
            user = os.getenv('SOURCE_DB_USER', 'app_user')
            password = os.getenv('SOURCE_DB_PASSWORD', 'rootpass')
            database = os.getenv('SOURCE_DB_NAME', 'financedata')
        else:  # warehouse
            host = os.getenv('WAREHOUSE_DB_HOST', 'localhost')
            port = os.getenv('WAREHOUSE_DB_PORT', '3307')
            user = os.getenv('WAREHOUSE_DB_USER', 'warehouse_user')
            password = os.getenv('WAREHOUSE_DB_PASSWORD', 'rootpass')
            database = os.getenv('WAREHOUSE_DB_NAME', 'warehouse_db')
        
        connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
        return create_engine(connection_string)
    
    def query_to_dataframe(self, query):
        """
        Execute SQL query and return results as pandas DataFrame
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pd.DataFrame: Query results
        """
        return pd.read_sql(query, self.engine)
    
    def table_to_dataframe(self, table_name, limit=None):
        """
        Load entire table into pandas DataFrame
        
        Args:
            table_name (str): Name of the table
            limit (int, optional): Maximum number of rows to fetch
            
        Returns:
            pd.DataFrame: Table data
        """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        return self.query_to_dataframe(query)
    
    def write_dataframe(self, df, table_name, if_exists='replace'):
        """
        Write pandas DataFrame to database table
        
        Args:
            df (pd.DataFrame): DataFrame to write
            table_name (str): Name of the target table
            if_exists (str): How to behave if table exists ('fail', 'replace', 'append')
        """
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
    
    def get_table_list(self):
        """
        Get list of all tables in the database
        
        Returns:
            list: List of table names
        """
        query = "SHOW TABLES"
        df = self.query_to_dataframe(query)
        return df.iloc[:, 0].tolist()
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()


def get_source_data(query):
    """
    Convenience function to get data from source database
    
    Args:
        query (str): SQL query
        
    Returns:
        pd.DataFrame: Query results
    """
    db = DatabaseConnection('source')
    df = db.query_to_dataframe(query)
    db.close()
    return df


def get_warehouse_data(query):
    """
    Convenience function to get data from warehouse database
    
    Args:
        query (str): SQL query
        
    Returns:
        pd.DataFrame: Query results
    """
    db = DatabaseConnection('warehouse')
    df = db.query_to_dataframe(query)
    db.close()
    return df
