#!/usr/bin/env python3
"""
Query Performance Benchmarking Tool
Runs EXPLAIN ANALYZE queries multiple times and calculates average runtime
"""

import pymysql
import time
import re
import statistics
from typing import List, Dict, Tuple
import json

class QueryBenchmark:
    def __init__(self, host='localhost', port=3305, user='root', password='rootpass', database='warehouse_db'):
        """Initialize database connection"""
        self.connection_params = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4'
        }
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = pymysql.connect(**self.connection_params)
            print(f"Connected to {self.connection_params['database']} on port {self.connection_params['port']}")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'connection'):
            self.connection.close()
            print("Database connection closed")
    
    def extract_runtime_from_explain(self, explain_output: str) -> float:
        """
        Extract actual execution time from EXPLAIN ANALYZE output
        Looks for patterns like 'actual time=X..Y' and takes the end time
        """
        # Pattern to match actual time values
        time_pattern = r'actual time=[\d.]+\.\.([\d.]+)'
        matches = re.findall(time_pattern, explain_output)
        
        if matches:
            # Get the maximum time (usually the total query time)
            return max(float(match) for match in matches)
        
        # Fallback: look for any time pattern
        fallback_pattern = r'time=([\d.]+)'
        fallback_matches = re.findall(fallback_pattern, explain_output)
        if fallback_matches:
            return max(float(match) for match in fallback_matches)
        
        return 0.0
    
    def run_explain_analyze(self, query: str) -> Tuple[float, str]:
        """
        Run EXPLAIN ANALYZE on a query and return execution time and full output
        Handles both single queries and multi-statement queries (with temp tables)
        """
        try:
            with self.connection.cursor() as cursor:
                start_time = time.perf_counter()
                
                # Check if query contains multiple statements (temp table queries)
                if 'CREATE TEMPORARY TABLE' in query.upper():
                    # Execute multi-statement query
                    statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
                    explain_output = ""
                    
                    for i, statement in enumerate(statements):
                        if statement.upper().startswith('CREATE TEMPORARY'):
                            cursor.execute(statement)
                            explain_output += f"Statement {i+1}: CREATE TEMPORARY TABLE executed\n"
                        elif statement.upper().startswith('DROP TEMPORARY'):
                            cursor.execute(statement)
                            explain_output += f"Statement {i+1}: DROP TEMPORARY TABLE executed\n"
                        elif statement.upper().startswith('SELECT'):
                            # This is the main query - run EXPLAIN ANALYZE on it
                            explain_query = f"EXPLAIN ANALYZE {statement}"
                            cursor.execute(explain_query)
                            result = cursor.fetchall()
                            explain_output += f"Statement {i+1}: EXPLAIN ANALYZE results:\n"
                            explain_output += '\n'.join([str(row[0]) for row in result])
                        else:
                            cursor.execute(statement)
                            explain_output += f"Statement {i+1}: {statement[:50]}... executed\n"
                else:
                    # Single statement query
                    explain_query = f"EXPLAIN ANALYZE {query}"
                    cursor.execute(explain_query)
                    result = cursor.fetchall()
                    explain_output = '\n'.join([str(row[0]) for row in result])
                
                end_time = time.perf_counter()
                
                # Extract MySQL's reported execution time
                mysql_time = self.extract_runtime_from_explain(explain_output)
                
                # Use MySQL's time if available, otherwise use Python timing
                execution_time = mysql_time if mysql_time > 0 else (end_time - start_time) * 1000
                
                return execution_time, explain_output
                
        except Exception as e:
            print(f"Error executing query: {e}")
            return 0.0, str(e)
    
    def benchmark_query(self, query: str, iterations: int = 10, query_name: str = "Query") -> Dict:
        """
        Run a query multiple times and calculate performance statistics
        """
        print(f"\nBenchmarking: {query_name}")
        print(f"Running {iterations} iterations...")
        print("-" * 60)
        
        execution_times = []
        explain_outputs = []
        
        for i in range(iterations):
            execution_time, explain_output = self.run_explain_analyze(query)
            
            if execution_time > 0:
                execution_times.append(execution_time)
                explain_outputs.append(explain_output)
                print(f"Run {i+1:2d}: {execution_time:8.2f} ms")
            else:
                print(f"Run {i+1:2d}: ERROR")
        
        if not execution_times:
            return {"error": "All query executions failed"}
        
        # Calculate statistics
        stats = {
            "query_name": query_name,
            "query": query.strip(),
            "iterations": len(execution_times),
            "execution_times": execution_times,
            "avg_time_ms": statistics.mean(execution_times),
            "min_time_ms": min(execution_times),
            "max_time_ms": max(execution_times),
            "median_time_ms": statistics.median(execution_times),
            "std_dev_ms": statistics.stdev(execution_times) if len(execution_times) > 1 else 0,
            "first_explain_output": explain_outputs[0] if explain_outputs else ""
        }
        
        # Print summary
        print("-" * 60)
        print(f"PERFORMANCE SUMMARY - {query_name}")
        print(f"Average Time: {stats['avg_time_ms']:8.2f} ms")
        print(f"Minimum Time: {stats['min_time_ms']:8.2f} ms") 
        print(f"Maximum Time: {stats['max_time_ms']:8.2f} ms")
        print(f"Median Time:  {stats['median_time_ms']:8.2f} ms")
        print(f"Std Deviation:{stats['std_dev_ms']:8.2f} ms")
        print(f"Successful Runs: {stats['iterations']}/{iterations}")
        
        return stats
    
    def benchmark_multiple_queries(self, queries: List[Tuple[str, str]], iterations: int = 10) -> List[Dict]:
        """
        Benchmark multiple queries and return results
        queries: List of (query_name, query_sql) tuples
        """
        results = []
        
        print(f"\nBENCHMARKING {len(queries)} QUERIES")
        print(f"{iterations} iterations per query")
        print("=" * 80)
        
        for query_name, query_sql in queries:
            result = self.benchmark_query(query_sql, iterations, query_name)
            results.append(result)
        
        # Print comparison summary
        print("\n" + "=" * 80)
        print("COMPARISON SUMMARY")
        print("=" * 80)
        print(f"{'Query Name':<25} {'Avg Time (ms)':<15} {'Min (ms)':<10} {'Max (ms)':<10}")
        print("-" * 65)
        
        for result in results:
            if "error" not in result:
                print(f"{result['query_name']:<25} {result['avg_time_ms']:<15.2f} {result['min_time_ms']:<10.2f} {result['max_time_ms']:<10.2f}")
        
        return results
    
    def save_results(self, results: List[Dict], filename: str = "benchmark_results.json"):
        """Save benchmark results to JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nResults saved to {filename}")
        except Exception as e:
            print(f"Error saving results: {e}")

def main():
    """Example usage with your OLAP queries"""
    
    # Your complete OLAP queries for benchmarking
    queries = [
        ("Query 1: Loan Rollup by Year", """
            SELECT d.year,
                   ROUND(AVG(fl.amount),2) AS avg_loan,
                   COUNT(*) AS loan_count
            FROM FactLoan fl
            JOIN DimDate d ON fl.date_id = d.date_id
            GROUP BY d.year
            ORDER BY d.year;
        """),
        
        ("Query 2: Loan Drilldown by Month", """
            SELECT d.month,
                   ROUND(AVG(fl.amount),2) AS avg_loan,
                   COUNT(*) AS loan_count
            FROM FactLoan fl
            JOIN DimDate d ON fl.date_id = d.date_id
            WHERE d.year = 1995
            GROUP BY d.month
            ORDER BY d.month;
        """),
        
        ("Query 3: Regional Cash Flow", """
            SELECT dist.region AS region_name,
                   ROUND(SUM(ft.amount),2) AS net_cash
            FROM FactTrans ft
            JOIN DimClientAccount ca ON ft.clientAcc_id = ca.clientAcc_id
            JOIN DimDistrict dist ON ca.distAcc_id = dist.district_id
            GROUP BY dist.region
            ORDER BY net_cash DESC;
        """),
        
        ("Query 4: Regional Drilldown - East Bohemia", """
            SELECT dist.district_name,
                   ROUND(SUM(ft.amount),2) AS net_cash
            FROM FactTrans ft
            JOIN DimClientAccount ca ON ft.clientAcc_id = ca.clientAcc_id
            JOIN DimDistrict dist ON ca.distAcc_id = dist.district_id
            WHERE dist.region = 'east Bohemia'
            GROUP BY dist.district_name
            ORDER BY net_cash DESC;
        """),
        
        ("Query 5: Loan Status Pivot by Region", """
            SELECT 
                dd.region,
                SUM(CASE WHEN fl.status = 'A' THEN 1 ELSE 0 END) AS finished_no_problems,
                SUM(CASE WHEN fl.status = 'B' THEN 1 ELSE 0 END) AS finished_pending_payments,
                SUM(CASE WHEN fl.status = 'C' THEN 1 ELSE 0 END) AS active_ok,
                SUM(CASE WHEN fl.status = 'D' THEN 1 ELSE 0 END) AS active_in_debt,
                SUM(CASE WHEN fl.status IN ('A', 'B') THEN 1 ELSE 0 END) AS total_completed,
                SUM(CASE WHEN fl.status IN ('C', 'D') THEN 1 ELSE 0 END) AS total_ongoing,
                COUNT(fl.loan_id) AS total_loans
            FROM FactLoan fl
            JOIN DimClientAccount dca ON fl.clientAcc_id = dca.clientAcc_id
            JOIN DimDistrict dd ON dca.distCli_id = dd.district_id
            GROUP BY dd.region
            ORDER BY total_loans DESC;
        """),
        
        ("Query 6: Transaction Operations Pivot", """
            SELECT 
                dd.district_name,
                dd.region,
                SUM(CASE WHEN ft.operation = 'Credit in Cash' THEN 1 ELSE 0 END) AS credit_in_cash,
                SUM(CASE WHEN ft.operation = 'Collection from Another Bank' THEN 1 ELSE 0 END) AS collection_from_bank,
                SUM(CASE WHEN ft.operation = 'Withdrawal in Cash' THEN 1 ELSE 0 END) AS withdrawal_in_cash,
                SUM(CASE WHEN ft.operation = 'Remittance to Another Bank' THEN 1 ELSE 0 END) AS remittance_to_bank,
                SUM(CASE WHEN ft.operation = 'Credit Card Withdrawal' THEN 1 ELSE 0 END) AS credit_card_withdrawal,
                COUNT(ft.trans_id) AS total_transactions,
                ROUND(AVG(ft.amount), 2) AS avg_transaction_amount,
                ROUND(SUM(ft.amount), 2) AS total_money_transferred
            FROM FactTrans ft
            JOIN DimClientAccount dca ON ft.clientAcc_id = dca.clientAcc_id
            JOIN DimDistrict dd ON dca.distCli_id = dd.district_id
            GROUP BY dd.district_id, dd.district_name, dd.region
            ORDER BY total_transactions DESC;
        """),
        
        ("Query 7: Card-Loan Analysis", """
            SELECT 
                dd.year,
                dc.type AS card_type,
                SUM(fl.payments) AS total_payments
            FROM DimDate dd
            JOIN FactLoan fl ON dd.date_id = fl.date_id
            JOIN DimCard dc ON fl.clientAcc_id = dc.clientAcc_id
            WHERE 
                dd.year = 1997 
                AND dc.type = 'Gold'
            GROUP BY 
                dd.year,
                dc.type;
        """),
        
        ("Query 8: Optimized Regional Cash Flow (Temp Table)", """
            CREATE TEMPORARY TABLE PreAggregatedTrans AS
            SELECT clientAcc_id,
                   SUM(amount) AS total_amount
            FROM FactTrans
            GROUP BY clientAcc_id;

            SELECT dist.region AS region_name,
                   ROUND(SUM(pt.total_amount), 2) AS net_cash
            FROM PreAggregatedTrans pt
            JOIN DimClientAccount ca ON pt.clientAcc_id = ca.clientAcc_id
            JOIN DimDistrict dist ON ca.distAcc_id = dist.district_id
            GROUP BY dist.region
            ORDER BY net_cash DESC;
            
            DROP TEMPORARY TABLE PreAggregatedTrans;
        """),
        
        ("Query 9: Optimized Operations Pivot (Temp Table)", """
            CREATE TEMPORARY TABLE PreAggregatedFactTrans AS
            SELECT 
                clientAcc_id,
                SUM(CASE WHEN operation = 'Credit in Cash' THEN 1 ELSE 0 END) AS credit_in_cash,
                SUM(CASE WHEN operation = 'Collection from Another Bank' THEN 1 ELSE 0 END) AS collection_from_bank,
                SUM(CASE WHEN operation = 'Withdrawal in Cash' THEN 1 ELSE 0 END) AS withdrawal_in_cash,
                SUM(CASE WHEN operation = 'Remittance to Another Bank' THEN 1 ELSE 0 END) AS remittance_to_bank,
                SUM(CASE WHEN operation = 'Credit Card Withdrawal' THEN 1 ELSE 0 END) AS credit_card_withdrawal,
                COUNT(trans_id) AS total_transactions,
                ROUND(AVG(amount), 2) AS avg_transaction_amount,
                ROUND(SUM(amount), 2) AS total_money_transferred
            FROM FactTrans
            GROUP BY clientAcc_id;

            SELECT 
                dd.district_name,
                dd.region,
                SUM(pt.credit_in_cash) AS credit_in_cash,
                SUM(pt.collection_from_bank) AS collection_from_bank,
                SUM(pt.withdrawal_in_cash) AS withdrawal_in_cash,
                SUM(pt.remittance_to_bank) AS remittance_to_bank,
                SUM(pt.credit_card_withdrawal) AS credit_card_withdrawal,
                SUM(pt.total_transactions) AS total_transactions,
                ROUND(AVG(pt.avg_transaction_amount), 2) AS avg_transaction_amount,
                ROUND(SUM(pt.total_money_transferred), 2) AS total_money_transferred
            FROM PreAggregatedFactTrans pt
            JOIN DimClientAccount dca ON pt.clientAcc_id = dca.clientAcc_id
            JOIN DimDistrict dd ON dca.distCli_id = dd.district_id
            GROUP BY dd.district_id, dd.district_name, dd.region
            ORDER BY total_transactions DESC;
            
            DROP TEMPORARY TABLE PreAggregatedFactTrans;
        """)
    ]
    
    # Initialize benchmarker
    benchmark = QueryBenchmark()
    
    if not benchmark.connect():
        return
    
    try:
        # Run benchmarks
        results = benchmark.benchmark_multiple_queries(queries, iterations=10)
        
        # Save results
        benchmark.save_results(results)
        
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
    except Exception as e:
        print(f"Benchmark error: {e}")
    finally:
        benchmark.close()

if __name__ == "__main__":
    main()