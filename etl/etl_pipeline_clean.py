"""
Financial Data Warehouse ETL Pipeline
====================================
Refactored and optimized version for loading financial data into warehouse.
"""

import time
import pymysql
import logging
from datetime import datetime

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
        config = SOURCE_DB_CONFIG.copy()
        config['ssl_disabled'] = True
        conn = pymysql.connect(**config)
        logger.info("Connected to source database successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to source database: {e}")
        raise

def get_warehouse_connection():
    """Get connection to warehouse database"""
    try:
        config = WAREHOUSE_DB_CONFIG.copy()
        config['ssl_disabled'] = True
        conn = pymysql.connect(**config)
        logger.info("Connected to warehouse database successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to warehouse database: {e}")
        raise

def create_warehouse_schema(warehouse_conn):
    """Create warehouse tables from setup_dw.sql file"""
    logger.info("Creating warehouse schema...")
    
    try:
        # Drop existing tables first
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
            drop_statements = [stmt.strip() for stmt in drop_tables_sql.split(';') if stmt.strip()]
            for statement in drop_statements:
                cursor.execute(statement)
            warehouse_conn.commit()
            logger.info("Existing tables dropped successfully")
        
        # Read and execute SQL file
        sql_file_path = 'sql/warehouse_init/setup_dw.sql'
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        with warehouse_conn.cursor() as cursor:
            for statement in sql_statements:
                if statement.upper().startswith('SELECT'):
                    cursor.execute(statement)
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Schema creation result: {result[0]}")
                elif statement.strip():
                    cursor.execute(statement)
            warehouse_conn.commit()
            logger.info("Warehouse schema created successfully!")
            
    except Exception as e:
        logger.error(f"Error creating warehouse schema: {e}")
        warehouse_conn.rollback()
        raise

def load_dim_date(source_conn, warehouse_conn):
    """Load DimDate dimension table"""
    logger.info("Loading DimDate dimension...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Extract unique dates from all source tables
            date_query = """
            SELECT DISTINCT newdate as date FROM (
                SELECT newdate FROM trans 
                UNION SELECT newdate FROM loan
                UNION SELECT newissued as newdate FROM card
                UNION SELECT newdate FROM account
            ) AS all_dates
            WHERE newdate IS NOT NULL
            ORDER BY newdate
            """
            
            source_cursor.execute(date_query)
            dates = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            date_records = []
            for i, (date_value,) in enumerate(dates, 1):
                # Handle different date formats
                if isinstance(date_value, str):
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
                quarter = (date_obj.month - 1) // 3 + 1
                date_records.append((i, date_obj, quarter, date_obj.year, date_obj.month, date_obj.day))
            
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

def load_dim_district(source_conn, warehouse_conn):
    """Load DimDistrict dimension table"""
    logger.info("Loading DimDistrict dimension...")
    
    try:
        with source_conn.cursor() as source_cursor:
            district_query = """
            SELECT district_id, district_name, region, inhabitants, noCities,
                   ratio_urbaninhabitants, average_salary, unemployment, 
                   noEntrepreneur, noCrimes
            FROM district
            ORDER BY district_id
            """
            source_cursor.execute(district_query)
            districts = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Clean and transform data
            district_records = []
            for district in districts:
                (district_id, district_name, region, inhabitants, noCities, 
                 ratio_urban, avg_salary, unemployment, noEntrepreneur, noCrimes) = district
                
                # Handle null values
                district_records.append((
                    district_id, district_name, region,
                    int(inhabitants) if inhabitants else 0,
                    int(noCities) if noCities else 0,
                    float(ratio_urban) if ratio_urban else 0.0,
                    float(avg_salary) if avg_salary else 0.0,
                    float(unemployment) if unemployment else 0.0,
                    int(noEntrepreneur) if noEntrepreneur else 0,
                    int(noCrimes) if noCrimes else 0
                ))
            
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

def load_dim_client_account(source_conn, warehouse_conn):
    """Load DimClientAccount dimension table"""
    logger.info("Loading DimClientAccount dimension...")
    
    try:
        with source_conn.cursor() as source_cursor:
            # Join client and account data
            client_account_query = """
            SELECT a.account_id, c.client_id, a.frequency, a.newdate,
                   c.district_id
            FROM account a
            JOIN disp d ON a.account_id = d.account_id
            JOIN client c ON d.client_id = c.client_id
            WHERE d.type = 'OWNER'
            ORDER BY a.account_id
            """
            source_cursor.execute(client_account_query)
            client_accounts = source_cursor.fetchall()
            
        # Get date mappings
        with warehouse_conn.cursor() as warehouse_cursor:
            warehouse_cursor.execute("SELECT date, date_id FROM DimDate")
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process client account data
            client_account_records = []
            for i, (account_id, client_id, frequency, account_date, district_id) in enumerate(client_accounts, 1):
                date_key = str(account_date) if account_date else None
                date_id = date_mappings.get(date_key, 1)
                
                client_account_records.append((
                    i, client_id, account_id,
                    district_id, district_id, date_id,
                    frequency if frequency else 'UNKNOWN'
                ))
            
            insert_query = """
            INSERT INTO DimClientAccount (
                clientAcc_id, client_id, account_id, 
                distCli_id, distAcc_id, date_id, frequency
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            warehouse_cursor.executemany(insert_query, client_account_records)
            warehouse_conn.commit()
            logger.info(f"Loaded {len(client_account_records)} records into DimClientAccount")
            
    except Exception as e:
        logger.error(f"Error loading DimClientAccount: {e}")
        warehouse_conn.rollback()
        raise

def load_dim_card(source_conn, warehouse_conn):
    """Load DimCard dimension table"""
    logger.info("Loading DimCard dimension...")
    
    try:
        with source_conn.cursor() as source_cursor:
            card_query = """
            SELECT c.card_id, c.type, c.newissued, d.account_id
            FROM card c
            JOIN disp d ON c.disp_id = d.disp_id
            ORDER BY c.card_id
            """
            source_cursor.execute(card_query)
            cards = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            warehouse_cursor.execute("SELECT account_id, clientAcc_id FROM DimClientAccount")
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            warehouse_cursor.execute("SELECT date, date_id FROM DimDate")
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process card data
            card_records = []
            for card_id, card_type, card_date, account_id in cards:
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue
                    
                date_key = str(card_date) if card_date else None
                date_id = date_mappings.get(date_key, 1)
                
                card_records.append((
                    card_id, clientAcc_id, date_id,
                    card_type if card_type else 'UNKNOWN'
                ))
            
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

def load_fact_trans(source_conn, warehouse_conn):
    """Load FactTrans fact table"""
    logger.info("Loading FactTrans fact table...")
    
    try:
        with source_conn.cursor() as source_cursor:
            trans_query = """
            SELECT trans_id, account_id, newdate, type, operation,
                   amount, balance, k_symbol, bank, account
            FROM trans
            ORDER BY trans_id
            """
            source_cursor.execute(trans_query)
            transactions = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            warehouse_cursor.execute("SELECT account_id, clientAcc_id FROM DimClientAccount")
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            warehouse_cursor.execute("SELECT date, date_id FROM DimDate")
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process transaction data
            trans_records = []
            for (trans_id, account_id, trans_date, trans_type, operation, 
                 amount, balance, k_symbol, bank, account) in transactions:
                
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue
                    
                date_key = str(trans_date) if trans_date else None
                date_id = date_mappings.get(date_key, 1)
                
                # Clean data
                account_int = 0
                if account:
                    try:
                        account_int = int(account)
                    except (ValueError, TypeError):
                        account_int = 0
                
                trans_records.append((
                    trans_id, clientAcc_id, date_id, account_int,
                    trans_type if trans_type else 'UNKNOWN',
                    operation if operation else 'UNKNOWN',
                    k_symbol if k_symbol else '',
                    bank if bank else '',
                    float(amount) if amount else 0.0,
                    float(balance) if balance else 0.0
                ))
            
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

def load_fact_loan(source_conn, warehouse_conn):
    """Load FactLoan fact table"""
    logger.info("Loading FactLoan fact table...")
    
    try:
        with source_conn.cursor() as source_cursor:
            loan_query = """
            SELECT l.loan_id, l.account_id, l.newdate, l.amount, l.duration,
                   l.payments, l.status, COALESCE(ls.description, 'Unknown') as description
            FROM loan l
            LEFT JOIN ref_loanstatus ls ON l.status = ls.status
            ORDER BY l.loan_id
            """
            source_cursor.execute(loan_query)
            loans = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get mappings
            warehouse_cursor.execute("SELECT account_id, clientAcc_id FROM DimClientAccount")
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            warehouse_cursor.execute("SELECT date, date_id FROM DimDate")
            date_mappings = {str(date_val): date_id for date_val, date_id in warehouse_cursor.fetchall()}
            
            # Process loan data
            loan_records = []
            for (loan_id, account_id, loan_date, amount, duration, 
                 payments, status, description) in loans:
                
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue
                    
                date_key = str(loan_date) if loan_date else None
                date_id = date_mappings.get(date_key, 1)
                
                loan_records.append((
                    loan_id, clientAcc_id, date_id,
                    status if status else 'U',
                    int(amount) if amount else 0,
                    int(duration) if duration else 0,
                    float(payments) if payments else 0.0,
                    description
                ))
            
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

def load_fact_order(source_conn, warehouse_conn):
    """Load FactOrder fact table"""
    logger.info("Loading FactOrder fact table...")
    
    try:
        with source_conn.cursor() as source_cursor:
            order_query = """
            SELECT order_id, account_id, bank_to, account_to, amount, k_symbol
            FROM orders
            ORDER BY order_id
            """
            source_cursor.execute(order_query)
            orders = source_cursor.fetchall()
            
        with warehouse_conn.cursor() as warehouse_cursor:
            # Get client account mappings
            warehouse_cursor.execute("SELECT account_id, clientAcc_id FROM DimClientAccount")
            client_account_mappings = {account_id: clientAcc_id for account_id, clientAcc_id in warehouse_cursor.fetchall()}
            
            # Process order data
            order_records = []
            for order_id, account_id, bank_to, account_to, amount, k_symbol in orders:
                clientAcc_id = client_account_mappings.get(account_id)
                if not clientAcc_id:
                    continue
                
                order_records.append((
                    order_id, clientAcc_id,
                    int(account_to) if account_to else 0,
                    float(amount) if amount else 0.0,
                    bank_to if bank_to else '',
                    k_symbol if k_symbol else ''
                ))
            
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

def validate_data_quality(warehouse_conn):
    """Perform data quality checks on the warehouse"""
    logger.info("Performing data quality validation...")
    
    try:
        with warehouse_conn.cursor() as cursor:
            # Count records in each table
            tables = ['DimDate', 'DimDistrict', 'DimClientAccount', 'DimCard', 'FactTrans', 'FactLoan', 'FactOrder']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"{table}: {count:,} records")
            
            # Check for orphaned records in fact tables
            cursor.execute("""
                SELECT COUNT(*) FROM FactTrans ft 
                LEFT JOIN DimClientAccount dca ON ft.clientAcc_id = dca.clientAcc_id 
                WHERE dca.clientAcc_id IS NULL
            """)
            orphaned_trans = cursor.fetchone()[0]
            if orphaned_trans > 0:
                logger.warning(f"Found {orphaned_trans} orphaned transactions")
            
            logger.info("Data quality validation completed")
            
    except Exception as e:
        logger.error(f"Error during data quality validation: {e}")
        raise

def run_etl_pipeline():
    """Main ETL pipeline execution function"""
    logger.info("=" * 60)
    logger.info("Starting Financial Data Warehouse ETL Pipeline")
    logger.info("=" * 60)
    
    start_time = time.time()
    source_conn = None
    warehouse_conn = None
    
    try:
        # Establish connections
        logger.info("Establishing database connections...")
        source_conn = get_source_connection()
        warehouse_conn = get_warehouse_connection()
        
        # Execute ETL phases
        logger.info("Phase 0: Creating Warehouse Schema")
        create_warehouse_schema(warehouse_conn)
        
        logger.info("Phase 1: Loading Dimension Tables")
        load_dim_date(source_conn, warehouse_conn)
        load_dim_district(source_conn, warehouse_conn)
        load_dim_client_account(source_conn, warehouse_conn)
        load_dim_card(source_conn, warehouse_conn)
        
        logger.info("Phase 2: Loading Fact Tables")
        load_fact_trans(source_conn, warehouse_conn)
        load_fact_loan(source_conn, warehouse_conn)
        load_fact_order(source_conn, warehouse_conn)
        
        logger.info("Phase 3: Data Quality Validation")
        validate_data_quality(warehouse_conn)
        
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
        print("\n ETL Pipeline completed successfully!")
    except Exception as e:
        print(f"\n ETL Pipeline failed: {e}")