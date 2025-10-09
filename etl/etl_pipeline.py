import csv
import time
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Configuration
SOURCE_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'rootpass',
    'database': 'financedata',
    'charset': 'utf8mb4',
    'autocommit': True
}

WAREHOUSE_DB_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'root',
    'password': 'rootpass',
    'database': 'warehouse_db',
    'charset': 'utf8mb4',
    'autocommit': True
}

def get_source_connection():
    """Get connection to source database"""
    try:
        # Try with SSL disabled and specific auth method
        config = SOURCE_DB_CONFIG.copy()
        config['ssl_disabled'] = True
        config['auth_plugin_map'] = {
            'mysql_native_password': 'mysql_native_password',
            'caching_sha2_password': 'mysql_native_password'
        }
        conn = pymysql.connect(**config)
        logger.info("Connected to source database successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to source database: {e}")
        # Try alternative connection method
        try:
            config = SOURCE_DB_CONFIG.copy()
            config['ssl'] = False
            conn = pymysql.connect(**config)
            logger.info("Connected to source database successfully (alternative method)")
            return conn
        except Exception as e2:
            logger.error(f"Alternative connection also failed: {e2}")
            raise e

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
        logger.error(f"Failed to connect to warehouse database: {e}")
        # Try alternative connection method
        try:
            config = WAREHOUSE_DB_CONFIG.copy()
            config['ssl'] = False
            conn = pymysql.connect(**config)
            logger.info("Connected to warehouse database successfully (alternative method)")
            return conn
        except Exception as e2:
            logger.error(f"Alternative connection also failed: {e2}")
            raise e

def create_warehouse_schema():
    """Create warehouse tables from setup_dw.sql file"""
    logger.info("Creating warehouse schema...")
    
    warehouse_conn = get_warehouse_connection()
    
    try:
        create_warehouse_schema_with_conn(warehouse_conn)
    finally:
        warehouse_conn.close()

def create_warehouse_schema_with_conn(warehouse_conn):
    """Create warehouse tables from setup_dw.sql file using existing connection"""
    logger.info("Creating warehouse schema...")
    
    try:
        # First, drop existing tables to ensure clean setup
        drop_tables_sql = """
        SET FOREIGN_KEY_CHECKS = 0;
        DROP TABLE IF EXISTS FactOrder;
        DROP TABLE IF EXISTS FactLoan;
        DROP TABLE IF EXISTS FactTrans;
        DROP TABLE IF EXISTS DimCard;
        DROP TABLE IF EXISTS DimClientAccount;
        DROP TABLE IF EXISTS DimDistrict;
        DROP TABLE IF EXISTS DimDate;
        SET FOREIGN_KEY_CHECKS = 1;
        """
        
        with warehouse_conn.cursor() as cursor:
            # Execute drop statements
            logger.info("Dropping existing warehouse tables...")
            drop_statements = [stmt.strip() for stmt in drop_tables_sql.split(';') if stmt.strip()]
            for statement in drop_statements:
                cursor.execute(statement)
            warehouse_conn.commit()
            logger.info("Existing tables dropped successfully")
        
        # Read the SQL file
        sql_file_path = 'sql/warehouse_init/setup_dw.sql'
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        
        # Split SQL statements by semicolon and execute each one
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        with warehouse_conn.cursor() as cursor:
            for statement in sql_statements:
                if statement.upper().startswith('SELECT'):
                    # Execute SELECT statements and log results
                    cursor.execute(statement)
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Schema creation result: {result[0]}")
                elif statement.strip():
                    # Execute other statements (CREATE TABLE, etc.)
                    cursor.execute(statement)
            
            warehouse_conn.commit()
            logger.info("Warehouse schema created successfully!")
            
    except FileNotFoundError:
        logger.error("Could not find setup_dw.sql file. Please ensure it exists in sql/warehouse_init/")
        raise
    except Exception as e:
        logger.error(f"Error creating warehouse schema: {e}")
        warehouse_conn.rollback()
        raise

def load_dim_date():
    """Load DimDate dimension table"""
    logger.info("Starting DimDate loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        load_dim_date_with_conn(source_conn, warehouse_conn)
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_dim_date_with_conn(source_conn, warehouse_conn):
    """Load DimDate dimension table using existing connections"""
    logger.info("Starting DimDate loading...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract unique dates from all source tables with date columns
            date_query = """
            SELECT DISTINCT newdate as date FROM (
                SELECT newdate FROM trans 
                UNION 
                SELECT newdate FROM loan
                UNION
                SELECT newissued as newdate FROM card
                UNION  
                SELECT newdate FROM account
            ) AS all_dates
            WHERE newdate IS NOT NULL
            ORDER BY newdate
            """
            
            source_cursor.execute(date_query)
            dates = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Process each date
            date_records = []
            for i, (date_value,) in enumerate(dates, 1):
                if isinstance(date_value, str):
                    # Handle YYYYMMDD format or other string formats
                    if len(date_value) == 8 and date_value.isdigit():
                        date_obj = datetime.strptime(date_value, '%Y%m%d').date()
                    else:
                        try:
                            date_obj = datetime.strptime(date_value, '%Y-%m-%d').date()
                        except:
                            continue
                else:
                    date_obj = date_value
                
                # Calculate date parts
                date_id = i
                quarter = (date_obj.month - 1) // 3 + 1
                year = date_obj.year
                month = date_obj.month
                day = date_obj.day
                
                date_records.append((date_id, date_obj, quarter, year, month, day))
            
            # Insert data
            insert_query = """
            INSERT INTO DimDate (date_id, date, quarter, year, month, day)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, date_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(date_records)} records into DimDate")
            
    except Exception as e:
        logger.error(f"Error loading DimDate: {e}")
        warehouse_conn.rollback()
        raise

def load_dim_district():
    """Load DimDistrict dimension table"""
    logger.info("Starting DimDistrict loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        load_dim_district_with_conn(source_conn, warehouse_conn)
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_dim_district_with_conn(source_conn, warehouse_conn):
    """Load DimDistrict dimension table using existing connections"""
    logger.info("Starting DimDistrict loading...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract district data
            district_query = """
            SELECT 
                district_id,
                district_name,
                region,
                inhabitants,
                noCities,
                ratio_urbaninhabitants,
                average_salary,
                unemployment,
                noEntrepreneur,
                noCrimes
            FROM district
            ORDER BY district_id
            """
            
            source_cursor.execute(district_query)
            districts = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Process and clean district data
            district_records = []
            for district in districts:
                district_id, district_name, region, inhabitants, noCities, ratio_urban, avg_salary, unemployment, noEntrepreneur, noCrimes = district
                
                # Handle null values and data cleaning
                inhabitants = int(inhabitants) if inhabitants is not None else 0
                noCities = int(noCities) if noCities is not None else 0
                ratio_urban = float(ratio_urban) if ratio_urban is not None else 0.0
                avg_salary = float(avg_salary) if avg_salary is not None else 0.0
                unemployment = float(unemployment) if unemployment is not None else 0.0
                noEntrepreneur = int(noEntrepreneur) if noEntrepreneur is not None else 0
                noCrimes = int(noCrimes) if noCrimes is not None else 0
                
                district_records.append((
                    district_id, district_name, region, inhabitants, noCities,
                    ratio_urban, avg_salary, unemployment, noEntrepreneur, noCrimes
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO DimDistrict (
                district_id, district_name, region, inhabitants, noCities,
                ratio_urbaninhabitants, average_salary, unemployment, 
                noEntrepreneur, noCrimes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, district_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(district_records)} records into DimDistrict")
            
    except Exception as e:
        logger.error(f"Error loading DimDistrict: {e}")
        warehouse_conn.rollback()
        raise

def load_dim_client_account():
    """Load DimClientAccount dimension table by joining client and account data"""
    logger.info("Starting DimClientAccount loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        load_dim_client_account_with_conn(source_conn, warehouse_conn)
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_dim_client_account_with_conn(source_conn, warehouse_conn):
    """Load DimClientAccount dimension table using existing connections"""
    logger.info("Starting DimClientAccount loading...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Join client and account data
            client_account_query = """
            SELECT 
                a.account_id,
                c.client_id,
                a.frequency,
                a.newdate as account_date,
                c.district_id,
                d.type as disp_type
            FROM account a
            JOIN disp d ON a.account_id = d.account_id
            JOIN client c ON d.client_id = c.client_id
            WHERE d.type = 'OWNER'  -- Only get account owners, not just authorized users
            ORDER BY a.account_id
            """
            
            source_cursor.execute(client_account_query)
            client_accounts = source_cursor.fetchall()
            
        # Get date mappings from DimDate
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get date mappings
            date_mapping_query = "SELECT date, date_id FROM DimDate"
            warehouse_cursor.execute(date_mapping_query)
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process client account data
            client_account_records = []
            for i, (account_id, client_id, frequency, account_date, district_id, disp_type) in enumerate(client_accounts, 1):
                
                # Get date_id from mapping
                date_key = str(account_date) if account_date else None
                date_id = date_mappings.get(date_key, 1)  # Default to first date if not found
                
                # Clean and transform data
                frequency = frequency if frequency else 'UNKNOWN'
                
                client_account_records.append((
                    i,  # clientAcc_id (surrogate key)
                    client_id,
                    account_id,
                    disp_type,  # type from disp table
                    district_id,  # distCli_id (client's district)
                    district_id,  # distAcc_id (account's district - same as client's in this case)
                    date_id,
                    frequency
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO DimClientAccount (
                clientAcc_id, client_id, account_id, type, distCli_id, distAcc_id, date_id, frequency
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, client_account_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(client_account_records)} records into DimClientAccount")
            
    except Exception as e:
        logger.error(f"Error loading DimClientAccount: {e}")
        warehouse_conn.rollback()
        raise

def load_dim_card():
    """Load DimCard dimension table"""
    logger.info("Starting DimCard loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        load_dim_card_with_conn(source_conn, warehouse_conn)
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_fact_trans():
    """Load FactTrans fact table"""
    logger.info("Starting FactTrans loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        load_fact_trans_with_conn(source_conn, warehouse_conn)
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_fact_loan():
    """Load FactLoan fact table"""
    logger.info("Starting FactLoan loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        load_fact_loan_with_conn(source_conn, warehouse_conn)
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_fact_order():
    """Load FactOrder fact table"""
    logger.info("Starting FactOrder loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        load_fact_order_with_conn(source_conn, warehouse_conn)
    finally:
        source_conn.close()
        warehouse_conn.close()

def validate_data_quality():
    """Perform data quality checks on the warehouse"""
    logger.info("Starting data quality validation...")
    
    warehouse_conn = get_warehouse_connection()
    
    try:
        validate_data_quality_with_conn(warehouse_conn)
    finally:
        warehouse_conn.close()

# Backup implementations (kept for compatibility)
def load_dim_card_backup():
    """Load DimCard dimension table"""
    logger.info("Starting DimCard loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract card data with account info
            card_query = """
            SELECT 
                c.card_id,
                c.disp_id,
                c.type,
                c.newissued as card_date,
                d.account_id
            FROM card c
            JOIN disp d ON c.disp_id = d.disp_id
            ORDER BY c.card_id
            """
            
            source_cursor.execute(card_query)
            cards = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            date_mapping_query = "SELECT date, date_id FROM DimDate"
            warehouse_cursor.execute(date_mapping_query)
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process card data
            card_records = []
            for card_id, disp_id, card_type, card_date, account_id in cards:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                    
                date_key = str(card_date) if card_date else None
                date_id = date_mappings.get(date_key, 1)
                
                card_records.append((
                    card_id,
                    clientAcc_id,
                    date_id,
                    card_type if card_type else 'UNKNOWN'
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO DimCard (card_id, clientAcc_id, date_id, type)
            VALUES (%s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, card_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(card_records)} records into DimCard")
            
    except Exception as e:
        logger.error(f"Error loading DimCard: {e}")
        warehouse_conn.rollback()
        raise
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_fact_trans():
    """Load FactTrans fact table"""
    logger.info("Starting FactTrans loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract transaction data
            trans_query = """
            SELECT 
                trans_id,
                account_id,
                newdate as date,
                type,
                operation,
                amount,
                balance,
                k_symbol,
                bank,
                account
            FROM trans
            ORDER BY trans_id
            """
            
            source_cursor.execute(trans_query)
            transactions = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            date_mapping_query = "SELECT date, date_id FROM DimDate"
            warehouse_cursor.execute(date_mapping_query)
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process transaction data
            trans_records = []
            for trans_id, account_id, trans_date, trans_type, operation, amount, balance, k_symbol, bank, account in transactions:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                    
                date_key = str(trans_date) if trans_date else None
                date_id = date_mappings.get(date_key, 1)
                
                # Data cleaning and transformation
                amount = float(amount) if amount is not None else 0.0
                balance = float(balance) if balance is not None else 0.0
                trans_type = trans_type if trans_type else 'UNKNOWN'
                operation = operation if operation else 'UNKNOWN'
                k_symbol = k_symbol if k_symbol else ''
                bank = bank if bank else ''
                
                # Convert account from TEXT to INT (handle as needed)
                account_int = None
                if account:
                    try:
                        account_int = int(account)
                    except (ValueError, TypeError):
                        account_int = 0  # Default value for non-numeric accounts
                
                trans_records.append((
                    trans_id,
                    clientAcc_id,
                    date_id,
                    account_int,
                    trans_type,
                    operation,
                    k_symbol,
                    bank,
                    amount,
                    balance
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO FactTrans (
                trans_id, clientAcc_id, date_id, account, type, operation,
                k_symbol, bank, amount, balance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, trans_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(trans_records)} records into FactTrans")
            
    except Exception as e:
        logger.error(f"Error loading FactTrans: {e}")
        warehouse_conn.rollback()
        raise
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_fact_loan():
    """Load FactLoan fact table"""
    logger.info("Starting FactLoan loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract loan data
            loan_query = """
            SELECT 
                l.loan_id,
                l.account_id,
                l.newdate as date,
                l.amount,
                l.duration,
                l.payments,
                l.status,
                ls.description
            FROM loan l
            LEFT JOIN ref_loanstatus ls ON l.status = ls.status
            ORDER BY l.loan_id
            """
            
            source_cursor.execute(loan_query)
            loans = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            date_mapping_query = "SELECT date, date_id FROM DimDate"
            warehouse_cursor.execute(date_mapping_query)
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process loan data
            loan_records = []
            for loan_id, account_id, loan_date, amount, duration, payments, status, description in loans:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                    
                date_key = str(loan_date) if loan_date else None
                date_id = date_mappings.get(date_key, 1)
                
                # Data cleaning and transformation
                amount = int(amount) if amount is not None else 0
                duration = int(duration) if duration is not None else 0
                payments = float(payments) if payments is not None else 0.0
                status = status if status else 'U'  # Default status
                description = description if description else 'Unknown'
                
                loan_records.append((
                    loan_id,
                    clientAcc_id,
                    date_id,
                    status,
                    amount,
                    duration,
                    payments,
                    description
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO FactLoan (
                loan_id, clientAcc_id, date_id, status, amount, duration, payments, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, loan_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(loan_records)} records into FactLoan")
            
    except Exception as e:
        logger.error(f"Error loading FactLoan: {e}")
        warehouse_conn.rollback()
        raise
    finally:
        source_conn.close()
        warehouse_conn.close()

def load_fact_order():
    """Load FactOrder fact table"""
    logger.info("Starting FactOrder loading...")
    
    source_conn = get_source_connection()
    warehouse_conn = get_warehouse_connection()
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract order data
            order_query = """
            SELECT 
                order_id,
                account_id,
                bank_to,
                account_to,
                amount,
                k_symbol
            FROM orders
            ORDER BY order_id
            """
            
            source_cursor.execute(order_query)
            orders = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get client account mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            # Process order data
            order_records = []
            for order_id, account_id, bank_to, account_to, amount, k_symbol in orders:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                
                # Data cleaning and transformation
                amount = float(amount) if amount is not None else 0.0
                bank_to = bank_to if bank_to else ''
                k_symbol = k_symbol if k_symbol else ''
                account_to = int(account_to) if account_to is not None else 0
                
                order_records.append((
                    order_id,
                    clientAcc_id,
                    account_to,
                    amount,
                    bank_to,
                    k_symbol
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO FactOrder (
                order_id, clientAcc_id, account_to, amount, bank_to, k_symbol
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, order_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(order_records)} records into FactOrder")
            
    except Exception as e:
        logger.error(f"Error loading FactOrder: {e}")
        warehouse_conn.rollback()
        raise
    finally:
        source_conn.close()
        warehouse_conn.close()

def validate_data_quality():
    """Perform data quality checks on the warehouse"""
    logger.info("Starting data quality validation...")
    
    warehouse_conn = get_warehouse_connection()
    
    try:
        with warehouse_conn.cursor() as cursor:
            # Check for orphaned records
            logger.info("Checking for data integrity issues...")
            
            # Check DimClientAccount foreign key integrity for client district
            cursor.execute("""
                SELECT COUNT(*) FROM DimClientAccount dca 
                LEFT JOIN DimDistrict dd ON dca.distCli_id = dd.district_id 
                WHERE dd.district_id IS NULL
            """)
            orphaned_client_districts = cursor.fetchone()[0]
            if orphaned_client_districts > 0:
                logger.warning(f"Found {orphaned_client_districts} client accounts with invalid client district references")
            
            # Check DimClientAccount foreign key integrity for account district
            cursor.execute("""
                SELECT COUNT(*) FROM DimClientAccount dca 
                LEFT JOIN DimDistrict dd ON dca.distAcc_id = dd.district_id 
                WHERE dd.district_id IS NULL
            """)
            orphaned_account_districts = cursor.fetchone()[0]
            if orphaned_account_districts > 0:
                logger.warning(f"Found {orphaned_account_districts} client accounts with invalid account district references")
            
            # Check FactTrans foreign key integrity
            cursor.execute("""
                SELECT COUNT(*) FROM FactTrans ft 
                LEFT JOIN DimClientAccount dca ON ft.clientAcc_id = dca.clientAcc_id 
                WHERE dca.clientAcc_id IS NULL
            """)
            orphaned_trans = cursor.fetchone()[0]
            if orphaned_trans > 0:
                logger.warning(f"Found {orphaned_trans} transactions with invalid client account references")
            
            # Check for duplicate records
            cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT date_id) FROM DimDate")
            duplicate_dates = cursor.fetchone()[0]
            if duplicate_dates > 0:
                logger.warning(f"Found {duplicate_dates} duplicate dates in DimDate")
            
            # Check for null values in critical fields
            cursor.execute("SELECT COUNT(*) FROM FactTrans WHERE amount IS NULL")
            null_amounts = cursor.fetchone()[0]
            if null_amounts > 0:
                logger.warning(f"Found {null_amounts} transactions with null amounts")
            
            # Count records in each table
            tables = ['DimDate', 'DimDistrict', 'DimClientAccount', 'DimCard', 'FactTrans', 'FactLoan', 'FactOrder']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"{table}: {count} records")
            
            logger.info("Data quality validation completed")
            
    except Exception as e:
        logger.error(f"Error during data quality validation: {e}")
        raise
    finally:
        warehouse_conn.close()

# Additional _with_conn functions for persistent connections

def load_dim_card_with_conn(source_conn, warehouse_conn):
    """Load DimCard dimension table using existing connections"""
    logger.info("Starting DimCard loading...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract card data with account info
            card_query = """
            SELECT 
                c.card_id,
                c.disp_id,
                c.type,
                c.newissued as card_date,
                d.account_id
            FROM card c
            JOIN disp d ON c.disp_id = d.disp_id
            ORDER BY c.card_id
            """
            
            source_cursor.execute(card_query)
            cards = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            date_mapping_query = "SELECT date, date_id FROM DimDate"
            warehouse_cursor.execute(date_mapping_query)
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process card data
            card_records = []
            for card_id, disp_id, card_type, card_date, account_id in cards:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                    
                date_key = str(card_date) if card_date else None
                date_id = date_mappings.get(date_key, 1)
                
                card_records.append((
                    card_id,
                    clientAcc_id,
                    date_id,
                    card_type if card_type else 'UNKNOWN'
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO DimCard (card_id, clientAcc_id, date_id, type)
            VALUES (%s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, card_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(card_records)} records into DimCard")
            
    except Exception as e:
        logger.error(f"Error loading DimCard: {e}")
        warehouse_conn.rollback()
        raise

def load_fact_trans_with_conn(source_conn, warehouse_conn):
    """Load FactTrans fact table using existing connections"""
    logger.info("Starting FactTrans loading...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract transaction data
            trans_query = """
            SELECT 
                trans_id,
                account_id,
                newdate as date,
                type,
                operation,
                amount,
                balance,
                k_symbol,
                bank,
                account
            FROM trans
            ORDER BY trans_id
            """
            
            source_cursor.execute(trans_query)
            transactions = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            date_mapping_query = "SELECT date, date_id FROM DimDate"
            warehouse_cursor.execute(date_mapping_query)
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process transaction data
            trans_records = []
            for trans_id, account_id, trans_date, trans_type, operation, amount, balance, k_symbol, bank, account in transactions:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                    
                date_key = str(trans_date) if trans_date else None
                date_id = date_mappings.get(date_key, 1)
                
                # Data cleaning and transformation
                amount = float(amount) if amount is not None else 0.0
                balance = float(balance) if balance is not None else 0.0
                trans_type = trans_type if trans_type else 'UNKNOWN'
                operation = operation if operation else 'UNKNOWN'
                k_symbol = k_symbol if k_symbol else ''
                bank = bank if bank else ''
                
                # Convert account from TEXT to INT (handle as needed)
                account_int = None
                if account:
                    try:
                        account_int = int(account)
                    except (ValueError, TypeError):
                        account_int = 0  # Default value for non-numeric accounts
                
                trans_records.append((
                    trans_id,
                    clientAcc_id,
                    date_id,
                    account_int,
                    trans_type,
                    operation,
                    k_symbol,
                    bank,
                    amount,
                    balance
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO FactTrans (
                trans_id, clientAcc_id, date_id, account, type, operation,
                k_symbol, bank, amount, balance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, trans_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(trans_records)} records into FactTrans")
            
    except Exception as e:
        logger.error(f"Error loading FactTrans: {e}")
        warehouse_conn.rollback()
        raise

def load_fact_loan_with_conn(source_conn, warehouse_conn):
    """Load FactLoan fact table using existing connections"""
    logger.info("Starting FactLoan loading...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract loan data
            loan_query = """
            SELECT 
                l.loan_id,
                l.account_id,
                l.newdate as date,
                l.amount,
                l.duration,
                l.payments,
                l.status,
                ls.description
            FROM loan l
            LEFT JOIN ref_loanstatus ls ON l.status = ls.status
            ORDER BY l.loan_id
            """
            
            source_cursor.execute(loan_query)
            loans = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            date_mapping_query = "SELECT date, date_id FROM DimDate"
            warehouse_cursor.execute(date_mapping_query)
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process loan data
            loan_records = []
            for loan_id, account_id, loan_date, amount, duration, payments, status, description in loans:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                    
                date_key = str(loan_date) if loan_date else None
                date_id = date_mappings.get(date_key, 1)
                
                # Data cleaning and transformation
                amount = int(amount) if amount is not None else 0
                duration = int(duration) if duration is not None else 0
                payments = float(payments) if payments is not None else 0.0
                status = status if status else 'U'  # Default status
                description = description if description else 'Unknown'
                
                loan_records.append((
                    loan_id,
                    clientAcc_id,
                    date_id,
                    status,
                    amount,
                    duration,
                    payments,
                    description
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO FactLoan (
                loan_id, clientAcc_id, date_id, status, amount, duration, payments, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, loan_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(loan_records)} records into FactLoan")
            
    except Exception as e:
        logger.error(f"Error loading FactLoan: {e}")
        warehouse_conn.rollback()
        raise

def load_fact_order_with_conn(source_conn, warehouse_conn):
    """Load FactOrder fact table using existing connections"""
    logger.info("Starting FactOrder loading...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract order data
            order_query = """
            SELECT 
                order_id,
                account_id,
                bank_to,
                account_to,
                amount,
                k_symbol
            FROM orders
            ORDER BY order_id
            """
            
            source_cursor.execute(order_query)
            orders = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get client account mappings
            client_account_mapping_query = """
            SELECT account_id, clientAcc_id FROM DimClientAccount
            """
            warehouse_cursor.execute(client_account_mapping_query)
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            # Process order data
            order_records = []
            for order_id, account_id, bank_to, account_to, amount, k_symbol in orders:
                
                # Get foreign keys
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue  # Skip if no matching client account
                
                # Data cleaning and transformation
                amount = float(amount) if amount is not None else 0.0
                bank_to = bank_to if bank_to else ''
                k_symbol = k_symbol if k_symbol else ''
                account_to = int(account_to) if account_to is not None else 0
                
                order_records.append((
                    order_id,
                    clientAcc_id,
                    account_to,
                    amount,
                    bank_to,
                    k_symbol
                ))
            
            # Insert data
            insert_query = """
            INSERT INTO FactOrder (
                order_id, clientAcc_id, account_to, amount, bank_to, k_symbol
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            warehouse_cursor.executemany(insert_query, order_records)
            warehouse_conn.commit()
            
            logger.info(f"Loaded {len(order_records)} records into FactOrder")
            
    except Exception as e:
        logger.error(f"Error loading FactOrder: {e}")
        warehouse_conn.rollback()
        raise

def validate_data_quality_with_conn(warehouse_conn):
    """Perform data quality checks on the warehouse using existing connection"""
    logger.info("Starting data quality validation...")
    
    try:
        with warehouse_conn.cursor() as cursor:
            # Check for orphaned records
            logger.info("Checking for data integrity issues...")
            
            # Check DimClientAccount foreign key integrity for client district
            cursor.execute("""
                SELECT COUNT(*) FROM DimClientAccount dca 
                LEFT JOIN DimDistrict dd ON dca.distCli_id = dd.district_id 
                WHERE dd.district_id IS NULL
            """)
            orphaned_client_districts = cursor.fetchone()[0]
            if orphaned_client_districts > 0:
                logger.warning(f"Found {orphaned_client_districts} client accounts with invalid client district references")
            
            # Check DimClientAccount foreign key integrity for account district
            cursor.execute("""
                SELECT COUNT(*) FROM DimClientAccount dca 
                LEFT JOIN DimDistrict dd ON dca.distAcc_id = dd.district_id 
                WHERE dd.district_id IS NULL
            """)
            orphaned_account_districts = cursor.fetchone()[0]
            if orphaned_account_districts > 0:
                logger.warning(f"Found {orphaned_account_districts} client accounts with invalid account district references")
            
            # Check FactTrans foreign key integrity
            cursor.execute("""
                SELECT COUNT(*) FROM FactTrans ft 
                LEFT JOIN DimClientAccount dca ON ft.clientAcc_id = dca.clientAcc_id 
                WHERE dca.clientAcc_id IS NULL
            """)
            orphaned_trans = cursor.fetchone()[0]
            if orphaned_trans > 0:
                logger.warning(f"Found {orphaned_trans} transactions with invalid client account references")
            
            # Check for duplicate records
            cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT date_id) FROM DimDate")
            duplicate_dates = cursor.fetchone()[0]
            if duplicate_dates > 0:
                logger.warning(f"Found {duplicate_dates} duplicate dates in DimDate")
            
            # Check for null values in critical fields
            cursor.execute("SELECT COUNT(*) FROM FactTrans WHERE amount IS NULL")
            null_amounts = cursor.fetchone()[0]
            if null_amounts > 0:
                logger.warning(f"Found {null_amounts} transactions with null amounts")
            
            # Count records in each table
            tables = ['DimDate', 'DimDistrict', 'DimClientAccount', 'DimCard', 'FactTrans', 'FactLoan', 'FactOrder']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"{table}: {count} records")
            
            logger.info("Data quality validation completed")
            
    except Exception as e:
        logger.error(f"Error during data quality validation: {e}")
        raise

def clean_and_transform_data(df, table_name):
    """Generic data cleaning and transformation function"""
    logger.info(f"Cleaning data for {table_name}...")
    
    original_count = len(df)
    
    # Remove exact duplicates
    df = df.drop_duplicates()
    if len(df) < original_count:
        logger.info(f"Removed {original_count - len(df)} duplicate records from {table_name}")
    
    # Handle missing values based on data type
    for column in df.columns:
        null_count = df[column].isnull().sum()
        if null_count > 0:
            logger.info(f"Found {null_count} null values in {table_name}.{column}")
            
            # Handle numeric columns
            if df[column].dtype in ['int64', 'float64']:
                df[column] = df[column].fillna(0)
            # Handle text columns
            else:
                df[column] = df[column].fillna('UNKNOWN')
    
    return df

def run_etl_pipeline():
    """Main ETL pipeline execution function with persistent connections"""
    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline Execution")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    # Get persistent connections at the start
    source_conn = None
    warehouse_conn = None
    
    try:
        # Establish connections once
        logger.info("Establishing database connections...")
        source_conn = get_source_connection()
        warehouse_conn = get_warehouse_connection()
        
        # Step 0: Create Warehouse Schema
        logger.info("Phase 0: Creating Warehouse Schema")
        create_warehouse_schema_with_conn(warehouse_conn)
        
        # Step 1: Load Dimension Tables (order matters due to foreign keys)
        logger.info("Phase 1: Loading Dimension Tables")
        load_dim_date_with_conn(source_conn, warehouse_conn)
        load_dim_district_with_conn(source_conn, warehouse_conn) 
        load_dim_client_account_with_conn(source_conn, warehouse_conn)
        load_dim_card_with_conn(source_conn, warehouse_conn)
        
        # Step 2: Load Fact Tables
        logger.info("Phase 2: Loading Fact Tables")
        load_fact_trans_with_conn(source_conn, warehouse_conn)
        load_fact_loan_with_conn(source_conn, warehouse_conn)
        load_fact_order_with_conn(source_conn, warehouse_conn)
        
        # Step 3: Data Quality Validation
        logger.info("Phase 3: Data Quality Validation")
        validate_data_quality_with_conn(warehouse_conn)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("ETL Pipeline Completed Successfully!")
        logger.info(f"Total execution time: {execution_time:.2f} seconds")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise
    finally:
        # Close connections only at the end
        if source_conn:
            source_conn.close()
            logger.info("Source database connection closed")
        if warehouse_conn:
            warehouse_conn.close()
            logger.info("Warehouse database connection closed")

if __name__ == "__main__":
    """Execute the ETL pipeline when script is run directly"""
    print("Financial Data Warehouse ETL Pipeline")
    print("=====================================")
    
    try:
        run_etl_pipeline()
        print("\nETL Pipeline completed successfully!")
        print("Check the logs for detailed information.")
        
    except Exception as e:
        print(f"\nETL Pipeline failed: {e}")
        print("Check the logs for error details.")
        exit(1)

