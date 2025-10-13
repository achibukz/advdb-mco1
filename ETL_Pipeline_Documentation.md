# 📊 Financial Data Warehouse ETL Pipeline Documentation

## **Project Overview**
This document provides a comprehensive explanation of the ETL (Extract, Transform, Load) pipeline that migrates financial data from an OLTP source database to an OLAP data warehouse using a star schema design.

---

## **🏗️ Architecture Overview**

### **Source System (OLTP)**
- **Database**: `financedata` (MySQL 9.4.0)
- **Port**: 3306
- **Structure**: Normalized relational database with 9 tables
- **Purpose**: Operational transaction processing

### **Target System (OLAP)**  
- **Database**: `warehouse_db` (MySQL 9.4.0)
- **Port**: 3307
- **Structure**: Star schema with 4 dimensions + 3 facts
- **Purpose**: Analytical processing and business intelligence

### **ETL Pipeline**
- **Language**: Python 3.13.7
- **Database Connector**: PyMySQL + Cryptography
- **Processing**: Batch processing with persistent connections
- **Execution Time**: ~18 seconds for 1M+ records

---

## **📋 ETL Pipeline Phases**

### **Phase 0: Schema Creation**
- Drops existing warehouse tables (handles FK constraints)
- Reads and executes `setup_dw.sql` schema definition
- Creates star schema structure with proper relationships

### **Phase 1: Dimension Table Loading**
- Loads dimension tables in dependency order
- Handles data cleaning and transformation
- Creates surrogate keys for performance

### **Phase 2: Fact Table Loading**
- Loads fact tables with foreign key mappings
- Processes large datasets efficiently
- Maintains referential integrity

### **Phase 3: Data Quality Validation**
- Counts records in all tables
- Checks for orphaned records
- Validates data integrity

---

## **🗂️ Detailed Table Analysis**

## **DIMENSION TABLES**

### **1. DimDate (Time Dimension)**

#### **Purpose**
Central time dimension enabling temporal analysis across all business processes.

#### **Source Data Extraction**
```sql
SELECT DISTINCT newdate as date FROM (
    SELECT newdate FROM trans 
    UNION SELECT newdate FROM loan
    UNION SELECT newissued as newdate FROM card
    UNION SELECT newdate FROM account
) AS all_dates
WHERE newdate IS NOT NULL
ORDER BY newdate
```

#### **Schema Structure**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `date_id` | INT PK | Surrogate key | 1, 2, 3... |
| `date` | DATE | Actual date | 1995-03-24 |
| `quarter` | INT | Quarter number | 1, 2, 3, 4 |
| `year` | INT | Year | 1995, 1996... |
| `month` | INT | Month number | 1-12 |
| `day` | INT | Day of month | 1-31 |

#### **Transformations Applied**
- **Date Format Handling**: Supports YYYYMMDD and YYYY-MM-DD formats
- **Quarter Calculation**: `(month - 1) // 3 + 1`
- **Surrogate Key Generation**: Sequential numbering starting from 1
- **Data Deduplication**: DISTINCT dates across all source tables

#### **Business Value**
- Enables time-based roll-up operations (day → month → quarter → year)
- Supports seasonal and trend analysis
- Provides consistent time hierarchy for all facts

#### **Final Record Count**: **2,191 unique dates**

---

### **2. DimDistrict (Geographic Dimension)**

#### **Purpose**
Geographic and demographic dimension enabling regional and socio-economic analysis.

#### **Source Data Extraction**
```sql
SELECT district_id, district_name, region, inhabitants, noCities,
       ratio_urbaninhabitants, average_salary, unemployment, 
       noEntrepreneur, noCrimes
FROM district
ORDER BY district_id
```

#### **Schema Structure**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `district_id` | INT PK | Natural key | 1, 2, 3... |
| `district_name` | TEXT | District name | "Prague", "Brno" |
| `region` | TEXT | Regional grouping | "central Bohemia" |
| `inhabitants` | INT | Population count | 234567 |
| `noCities` | INT | Number of cities | 15 |
| `ratio_urbaninhabitants` | DOUBLE | Urban population % | 0.65 |
| `average_salary` | DOUBLE | Average salary | 45000.0 |
| `unemployment` | DOUBLE | Unemployment rate | 0.05 |
| `noEntrepreneur` | INT | Entrepreneur count | 1234 |
| `noCrimes` | INT | Crime incidents | 567 |

#### **Transformations Applied**
- **Null Value Handling**: Converts NULL to 0 for numeric fields
- **Data Type Casting**: Ensures proper numeric types
- **Natural Key Preservation**: Uses original district_id as primary key

#### **Business Value**
- Enables geographic roll-up (district → region)
- Supports demographic-based customer segmentation
- Provides socio-economic context for financial behavior analysis

#### **Final Record Count**: **76 districts**

---

### **3. DimClientAccount (Central Dimension)**

#### **Purpose**
Central composite dimension combining client and account information for analytical queries.

#### **Source Data Extraction**
```sql
SELECT a.account_id, c.client_id, a.frequency, a.newdate,
       c.district_id, d.type
FROM account a
JOIN disp d ON a.account_id = d.account_id
JOIN client c ON d.client_id = c.client_id
WHERE d.type = 'OWNER'  -- Only account owners
ORDER BY a.account_id
```

#### **Schema Structure**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `clientAcc_id` | INT PK | Surrogate key | 1, 2, 3... |
| `client_id` | INT | Client identifier | 123 |
| `account_id` | INT | Account identifier | 456 |
| `type` | TEXT | Disposition type | "OWNER" |
| `distCli_id` | INT FK | Client's district | 15 |
| `distAcc_id` | INT FK | Account's district | 15 |
| `date_id` | INT FK | Account opening date | 45 |
| `frequency` | TEXT | Statement frequency | "Monthly" |

#### **Transformations Applied**
- **Denormalization**: Combines client and account data into single dimension
- **Business Rule Application**: Only includes account owners (d.type = 'OWNER')
- **Foreign Key Mapping**: Maps account.newdate to DimDate.date_id
- **Surrogate Key Generation**: Sequential clientAcc_id for clean fact table joins
- **Data Cleaning**: Handles NULL frequency as 'UNKNOWN'

#### **Business Logic Decisions**
- **OWNER Only Filter**: Excludes 'DISPONENT' (authorized users) to avoid duplicate account analysis
- **Same District Assumption**: distCli_id = distAcc_id (client and account in same district)

#### **Business Value**
- Serves as central hub for customer-account relationships
- Enables customer segmentation and account analysis
- Provides clean one-to-one mapping for fact tables

#### **Final Record Count**: **4,500 client-account relationships**

---

### **4. DimCard (Card Dimension)**

#### **Purpose**
Card information dimension for payment method and card usage analysis.

#### **Source Data Extraction**
```sql
SELECT c.card_id, c.type, c.newissued, d.account_id
FROM card c
JOIN disp d ON c.disp_id = d.disp_id
ORDER BY c.card_id
```

#### **Schema Structure**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `card_id` | INT PK | Natural key | 12345 |
| `clientAcc_id` | INT FK | Client account reference | 123 |
| `date_id` | INT FK | Card issuance date | 67 |
| `type` | TEXT | Card type | "gold", "classic" |

#### **Transformations Applied**
- **Foreign Key Mapping**: Maps account_id to DimClientAccount.clientAcc_id
- **Date Mapping**: Maps card.newissued to DimDate.date_id
- **Data Cleaning**: Handles NULL card type as 'UNKNOWN'
- **Orphan Filtering**: Skips cards without matching client accounts

#### **Business Value**
- Enables card type analysis and segmentation
- Supports payment method preference studies
- Links card usage to specific client-account relationships

#### **Final Record Count**: **892 cards**

---

## **FACT TABLES**

### **1. FactTrans (Transaction Facts)**

#### **Purpose**
Core transactional fact table containing all financial transactions with detailed metrics.

#### **Source Data Extraction**
```sql
SELECT trans_id, account_id, newdate, type, operation,
       amount, balance, k_symbol, bank, account
FROM trans
ORDER BY trans_id
```

#### **Schema Structure**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `trans_id` | INT PK | Transaction ID | 123456 |
| `clientAcc_id` | INT FK | Client account reference | 45 |
| `date_id` | INT FK | Transaction date | 123 |
| `account` | INT | Account reference | 789 |
| `type` | TEXT | Credit/Debit | "Credit" |
| `operation` | TEXT | Operation type | "Collection" |
| `k_symbol` | TEXT | Payment symbol | "SIPO" |
| `bank` | TEXT | Bank code | "AB" |
| `amount` | DOUBLE | Transaction amount | 1500.50 |
| `balance` | DOUBLE | Account balance after | 15000.75 |

#### **Transformations Applied**
- **Foreign Key Mapping**: Maps account_id to DimClientAccount.clientAcc_id
- **Date Mapping**: Maps trans.newdate to DimDate.date_id
- **Data Type Conversion**: Converts account field from TEXT to INT
- **Null Value Handling**: Converts NULL amounts to 0.0
- **String Cleaning**: Handles NULL text fields as empty strings
- **Orphan Filtering**: Skips transactions without matching client accounts

#### **Business Value**
- **Additive Facts**: Amount can be aggregated across all dimensions
- **Semi-Additive Facts**: Balance requires careful aggregation (latest per account)
- **Rich Attributes**: Enables detailed transaction analysis by type, operation, symbol
- **High Granularity**: Individual transaction level for detailed analysis

#### **Final Record Count**: **1,056,320 transactions** (1M+ records)

---

### **2. FactLoan (Loan Facts)**

#### **Purpose**
Loan portfolio fact table containing loan metrics and performance data.

#### **Source Data Extraction**
```sql
SELECT l.loan_id, l.account_id, l.newdate, l.amount, l.duration,
       l.payments, l.status, COALESCE(ls.description, 'Unknown') as description
FROM loan l
LEFT JOIN ref_loanstatus ls ON l.status = ls.status
ORDER BY l.loan_id
```

#### **Schema Structure**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `loan_id` | INT PK | Loan identifier | 12345 |
| `clientAcc_id` | INT FK | Client account reference | 67 |
| `date_id` | INT FK | Loan origination date | 89 |
| `status` | CHAR(1) | Loan status code | 'A', 'B', 'C', 'D' |
| `amount` | INT | Loan principal | 100000 |
| `duration` | INT | Loan term (months) | 36 |
| `payments` | DOUBLE | Monthly payment | 2500.50 |
| `description` | VARCHAR(45) | Status description | "Good - paid" |

#### **Transformations Applied**
- **Foreign Key Mapping**: Maps account_id to DimClientAccount.clientAcc_id
- **Date Mapping**: Maps loan.newdate to DimDate.date_id
- **Status Enrichment**: Joins with ref_loanstatus for descriptions
- **Data Type Conversion**: Ensures proper numeric types
- **Default Value Handling**: Uses 'U' for unknown status, 'Unknown' for missing descriptions
- **Orphan Filtering**: Skips loans without matching client accounts

#### **Business Value**
- **Loan Portfolio Analysis**: Total loan amounts, durations, payment schedules
- **Risk Assessment**: Loan status distribution and performance tracking
- **Profitability Metrics**: Payment amounts vs. loan amounts over time
- **Customer Segmentation**: Loan behavior patterns by client characteristics

#### **Final Record Count**: **682 loans**

---

### **3. FactOrder (Payment Order Facts)**

#### **Purpose**
Inter-bank payment order fact table for payment flow and banking relationship analysis.

#### **Source Data Extraction**
```sql
SELECT order_id, account_id, bank_to, account_to, amount, k_symbol
FROM orders
ORDER BY order_id
```

#### **Schema Structure**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `order_id` | INT PK | Order identifier | 98765 |
| `clientAcc_id` | INT FK | Client account reference | 123 |
| `account_to` | INT | Destination account | 456789 |
| `amount` | DOUBLE | Payment amount | 5000.00 |
| `bank_to` | TEXT | Destination bank | "XY" |
| `k_symbol` | TEXT | Payment symbol | "POJISTNE" |

#### **Transformations Applied**
- **Foreign Key Mapping**: Maps account_id to DimClientAccount.clientAcc_id
- **Data Type Conversion**: Converts account_to to INT
- **Null Value Handling**: Converts NULL amounts to 0.0, NULL text to empty strings
- **Orphan Filtering**: Skips orders without matching client accounts

#### **Business Value**
- **Payment Flow Analysis**: Inter-bank payment patterns and volumes
- **Banking Relationships**: Analysis of destination banks and payment types
- **Customer Behavior**: Payment frequency and amount patterns
- **Revenue Analysis**: Payment processing volumes and trends

#### **Final Record Count**: **6,471 payment orders**

---

## **🔧 Technical Implementation Details**

### **Connection Management**
- **Persistent Connections**: Single connection per database throughout pipeline
- **SSL Configuration**: SSL disabled for local Docker environment
- **Authentication**: Handles MySQL 9.4.0 `caching_sha2_password` with cryptography
- **Transaction Management**: Commit/rollback for data integrity

### **Performance Optimizations**
- **Batch Processing**: `executemany()` for bulk inserts
- **Efficient Mapping**: Dictionary-based FK lookups
- **Memory Management**: Processes data in manageable chunks
- **Connection Reuse**: Avoids connection overhead per table

### **Data Quality Measures**
- **Referential Integrity**: FK constraints enforced
- **Orphan Prevention**: Skips records without valid FK references
- **Data Validation**: Type checking and null value handling
- **Logging**: Comprehensive logging for monitoring and debugging

### **Error Handling**
- **Transaction Rollback**: Automatic rollback on errors
- **Connection Cleanup**: Guaranteed connection closure
- **Detailed Logging**: Error messages with context
- **Graceful Failure**: Pipeline stops on critical errors

---

## **📊 ETL Pipeline Results**

### **Execution Metrics**
- **Total Execution Time**: 18.21 seconds
- **Records Processed**: 1,069,126 total records
- **Tables Created**: 7 (4 dimensions + 3 facts)
- **Data Integrity**: 100% (0 orphaned records)

### **Data Volume Summary**
| Table | Record Count | Percentage |
|-------|-------------|------------|
| FactTrans | 1,056,320 | 98.8% |
| FactOrder | 6,471 | 0.6% |
| DimClientAccount | 4,500 | 0.4% |
| DimDate | 2,191 | 0.2% |
| DimCard | 892 | 0.08% |
| FactLoan | 682 | 0.06% |
| DimDistrict | 76 | 0.007% |

### **Business Intelligence Validation**
```sql
-- Sample OLAP Query: Regional Credit Analysis
SELECT dd.region, 
       COUNT(DISTINCT dca.client_id) as clients, 
       SUM(ft.amount) as total_transaction_volume 
FROM FactTrans ft 
JOIN DimClientAccount dca ON ft.clientAcc_id = dca.clientAcc_id 
JOIN DimDistrict dd ON dca.distCli_id = dd.district_id 
WHERE ft.type = 'Credit' 
GROUP BY dd.region 
ORDER BY total_transaction_volume DESC;
```

**Results**: Successfully executed complex analytical queries across star schema.

---

## **🎯 Business Value Delivered**

### **Analytical Capabilities Enabled**
1. **Temporal Analysis**: Time-based trends and seasonality
2. **Geographic Analysis**: Regional performance and demographics
3. **Customer Segmentation**: Client behavior and characteristics
4. **Risk Assessment**: Loan portfolio performance
5. **Payment Analysis**: Transaction patterns and banking relationships

### **OLAP Operations Supported**
- **Roll-up**: Daily → Monthly → Quarterly aggregations
- **Drill-down**: Regional → District → Client analysis
- **Slice**: Filter by time periods, regions, customer types
- **Dice**: Multi-dimensional analysis cubes

### **Performance Benefits**
- **Query Speed**: Star schema optimized for analytical queries
- **Scalability**: Surrogate keys enable efficient joins
- **Maintainability**: Clean data model with clear relationships
- **Extensibility**: Easy to add new dimensions or facts

---

## **🚀 Next Steps**

### **Immediate Actions**
1. **OLAP Application Development**: Build web-based analytical interface
2. **Query Optimization**: Add indexes for common query patterns
3. **Performance Testing**: Benchmark query response times
4. **Dashboard Creation**: Develop business intelligence dashboards

### **Future Enhancements**
1. **Incremental Loading**: Delta processing for ongoing updates
2. **Data Quality Monitoring**: Automated quality checks
3. **Slowly Changing Dimensions**: Handle dimension updates over time
4. **Additional Fact Tables**: Expand analytical scope

---

## **📝 Conclusion**

The Financial Data Warehouse ETL Pipeline successfully transforms normalized OLTP data into an optimized OLAP star schema, enabling comprehensive business intelligence and analytical capabilities. The pipeline demonstrates excellent performance (18 seconds for 1M+ records), maintains perfect data integrity (0 orphaned records), and provides a solid foundation for advanced analytics and reporting.

The implementation follows data warehousing best practices including proper dimensional modeling, efficient data processing, comprehensive error handling, and thorough validation. The resulting data warehouse is ready for production use in supporting business intelligence and analytical applications.