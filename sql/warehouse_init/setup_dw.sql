USE warehouse_db;

-- Dimesional Tables

-- DimDate - Time dimension
CREATE TABLE DimDate(
    date_id INT PRIMARY KEY,
    date DATE,
    quarter INT,
    year INT,
    month INT,
    day INT
);

-- DimDistrict - Geographic dimension
CREATE TABLE DimDistrict(
    district_id INT PRIMARY KEY,
    district_name TEXT,
    region TEXT,
    inhabitants INT,
    noCities INT,
    ratio_urbaninhabitants DOUBLE,
    average_salary DOUBLE,
    unemployment DOUBLE,
    noEntrepreneur INT,
    noCrimes INT
);

-- DimClientAccount - Central dimension
CREATE TABLE DimClientAccount (
    clientAcc_id INT PRIMARY KEY,
    client_id INT,
    account_id INT,
    type TEXT,
    district_id INT,
    date_id INT,
    frequency TEXT,
    FOREIGN KEY (district_id) REFERENCES DimDistrict(district_id),
    FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

-- DimCard - Card dimension
CREATE TABLE DimCard(
    card_id INT PRIMARY KEY,
    clientAcc_id INT,
    date_id INT,
    type TEXT,
    FOREIGN KEY (clientAcc_id) REFERENCES DimClientAccount(clientAcc_id),
    FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

-- Fact Tables

-- FactTrans - Main transaction fact table
CREATE TABLE FactTrans (
    trans_id INT PRIMARY KEY,
    clientAcc_id INT,
    date_id INT,
    account INT,
    type TEXT,
    operation TEXT,
    k_symbol TEXT,
    bank TEXT,
    amount DOUBLE,
    balance DOUBLE,
    FOREIGN KEY (clientAcc_id) REFERENCES DimClientAccount(clientAcc_id),
    FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

-- FactLoan - Loan transactions fact table
CREATE TABLE FactLoan (
    loan_id INT PRIMARY KEY,
    clientAcc_id INT,
    date_id INT,
    status CHAR(1),
    amount INT,
    duration INT,
    payments DOUBLE,
    description VARCHAR(45),
    FOREIGN KEY (clientAcc_id) REFERENCES DimClientAccount(clientAcc_id),
    FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

-- FactOrder - Order transactions fact table
CREATE TABLE FactOrder (
    order_id INT PRIMARY KEY,
    clientAcc_id INT,
    account_to INT,
    amount DOUBLE,
    bank_to TEXT,
    k_symbol TEXT,
    FOREIGN KEY (clientAcc_id) REFERENCES DimClientAccount(clientAcc_id)
);

-- SUCCESS MESSAGE

SELECT 'Data Warehouse Schema Created Successfully!' as STATUS;
