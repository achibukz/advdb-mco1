"""
Reports module for querying warehouse data and generating analytics
"""
import sys
import json
import pandas as pd
from database import get_warehouse_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_district_net_cash(level='district', parent=None):
    """
    Get net cash data with drill-down capability
    
    Args:
        level (str): 'region', 'district', or 'account'
        parent (str): Parent filter (region name or district name)
    
    Returns:
        dict: Data formatted for Chart.js
    """
    conn = None
    try:
        conn = get_warehouse_connection()
        cursor = conn.cursor()
        
        if level == 'region':
            # Roll up to region level
            query = """
                SELECT 
                    d.region AS label,
                    SUM(fa.amount) AS total_net_cash,
                    COUNT(DISTINCT d.district_id) AS district_count,
                    COUNT(DISTINCT fa.account) AS account_count
                FROM FactTrans fa
                JOIN DimClientAccount ca ON fa.clientAcc_id = ca.clientAcc_id
                JOIN DimDistrict d ON ca.distAcc_id = d.district_id
                GROUP BY d.region
                ORDER BY total_net_cash DESC
            """
            cursor.execute(query)
            
        elif level == 'district':
            # District level (optionally filtered by region)
            if parent:
                query = """
                    SELECT 
                        d.district_name AS label,
                        d.region,
                        SUM(fa.amount) AS total_net_cash,
                        COUNT(DISTINCT fa.account) AS account_count
                    FROM FactTrans fa
                    JOIN DimClientAccount ca ON fa.clientAcc_id = ca.clientAcc_id
                    JOIN DimDistrict d ON ca.distAcc_id = d.district_id
                    WHERE d.region = %s
                    GROUP BY d.district_name, d.region
                    ORDER BY total_net_cash DESC
                """
                cursor.execute(query, (parent,))
            else:
                query = """
                    SELECT 
                        d.district_name AS label,
                        d.region,
                        SUM(fa.amount) AS total_net_cash,
                        COUNT(DISTINCT fa.account) AS account_count
                    FROM FactTrans fa
                    JOIN DimClientAccount ca ON fa.clientAcc_id = ca.clientAcc_id
                    JOIN DimDistrict d ON ca.distAcc_id = d.district_id
                    GROUP BY d.district_name, d.region
                    ORDER BY total_net_cash DESC
                """
                cursor.execute(query)
                
        elif level == 'account':
            # Account level (optionally filtered by district)
            if parent:
                query = """
                    SELECT 
                        fa.account AS label,
                        d.district_name,
                        d.region,
                        SUM(fa.amount) AS total_net_cash,
                        COUNT(fa.trans_id) AS transaction_count
                    FROM FactTrans fa
                    JOIN DimClientAccount ca ON fa.clientAcc_id = ca.clientAcc_id
                    JOIN DimDistrict d ON ca.distAcc_id = d.district_id
                    WHERE d.district_name = %s
                    GROUP BY fa.account, d.district_name, d.region
                    ORDER BY total_net_cash DESC
                    LIMIT 50
                """
                cursor.execute(query, (parent,))
            else:
                query = """
                    SELECT 
                        fa.account AS label,
                        d.district_name,
                        d.region,
                        SUM(fa.amount) AS total_net_cash,
                        COUNT(fa.trans_id) AS transaction_count
                    FROM FactTrans fa
                    JOIN DimClientAccount ca ON fa.clientAcc_id = ca.clientAcc_id
                    JOIN DimDistrict d ON ca.distAcc_id = d.district_id
                    GROUP BY fa.account, d.district_name, d.region
                    ORDER BY total_net_cash DESC
                    LIMIT 50
                """
                cursor.execute(query)
        
        # Fetch results
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        data = []
        for row in results:
            row_dict = dict(zip(columns, row))
            # Convert Decimal to float for JSON serialization
            for key, value in row_dict.items():
                if hasattr(value, 'real'):  # Check if it's a number type
                    row_dict[key] = float(value)
            data.append(row_dict)
        
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'level': level,
            'parent': parent,
            'data': data,
            'count': len(data)
        }
        
    except Exception as e:
        logger.error(f"Error fetching district net cash: {e}")
        if conn:
            conn.close()
        return {
            'success': False,
            'error': str(e)
        }


def main():
    """
    Main function for command-line usage
    Usage: python reports.py [level] [parent]
    """
    level = sys.argv[1] if len(sys.argv) > 1 else 'district'
    parent = sys.argv[2] if len(sys.argv) > 2 else None
    
    result = get_district_net_cash(level, parent)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
