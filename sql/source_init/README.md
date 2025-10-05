# Source Database Initialization

Place your SQL dataset files in this directory.

## Requirements:
- Files must have `.sql` extension
- Should create and populate your source database
- Files are executed in alphabetical order
- Example: `01_create_database.sql`, `02_load_data.sql`

## Example SQL file structure:
```sql
CREATE DATABASE IF NOT EXISTS your_database;
USE your_database;

CREATE TABLE your_table (...);
INSERT INTO your_table VALUES (...);
```

Your dataset files are gitignored for security and size reasons.