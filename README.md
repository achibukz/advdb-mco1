# Data Warehouse Project

A MySQL-based3. **Connect to source database:**
   ```bash
   mysql -h localhost -P 3306 -u app_user -prootpass financedata
   ```a warehouse project using Docker for containerization.

## Project Structure

```
├── docker-compose.yml          # Docker services configuration
├── sql/
│   ├── source_init/           # Source database initialization
│   │   ├── 01_create_tables.sql
│   │   └── 02_insert_sample_data.sql
│   └── warehouse_init/        # Data warehouse initialization
│       ├── 01_create_warehouse_schema.sql
│       └── 02_populate_dimensions.sql
└── etl/
    ├── Dockerfile
    ├── requirements.txt
    └── etl_pipeline.py        # ETL template (implement your logic here)
```

## Services

- **mysql-source**: Source OLTP database (port 3306)
- **mysql-warehouse**: Data warehouse OLAP database (port 3307)  
- **etl-service**: Python ETL service container

## Getting Started

1. **Add your dataset (Required):**
   ```bash
   # Place your SQL dataset file in the source_init folder:
   # sql/source_init/your-dataset.sql
   # 
   # The SQL file should create and populate your source database
   ```

2. **Start the services:**
   ```bash
   docker-compose up -d
   ```

3. **Check if databases are running:**
   ```bash
   docker-compose ps
   ```

4. **Wait for data loading to complete:**
   ```bash
   # Watch logs to see when your dataset finishes loading
   docker-compose logs mysql-source
   ```

5. **Connect to source database:**
   ```bash
   mysql -h localhost -P 3306 -u app_user -prootpass financedata
   ```

6. **Connect to warehouse database:**
   ```bash
   mysql -h localhost -P 3307 -u warehouse_user -prootpass warehouse_db
   ```

## Dataset Setup

This project requires a SQL dataset file for the source database. 

### For Team Members:
1. **Obtain the dataset** from your team lead or project source
2. **Place the SQL file** in `sql/source_init/` directory
3. **File should contain** `CREATE DATABASE` and `USE` statements
4. **Supported formats**: `.sql` files that MySQL can execute

### Dataset Requirements:
- Must create and populate a database (e.g., `financedata`)
- Should contain transactional/operational data (OLTP structure)
- File will be automatically executed when containers start for the first time

## Database Credentials

### Source Database (OLTP)
- Host: localhost
- Port: 3306
- Database: financedata
- User: app_user
- Password: rootpass
- Root Password: rootpass

### Warehouse Database (OLAP)
- Host: localhost
- Port: 3307
- Database: warehouse_db
- User: warehouse_user
- Password: rootpass
- Root Password: rootpass

## ETL Implementation

The ETL pipeline template is provided in `etl/etl_pipeline.py`. You need to implement:

1. **Extract**: Pull data from source database
2. **Transform**: Clean and transform data according to business rules
3. **Load**: Insert transformed data into warehouse dimensions and facts

## Stopping Services

```bash
docker-compose down
```

To remove volumes (will delete all data):
```bash
docker-compose down -v
```

