USE warehouse_db;


-- For Query 2
CREATE INDEX idx_dimdate_year ON DimDate(year);

-- DimDistrict Indexes  
-- For Query 4
CREATE INDEX idx_dimdistrict_region ON DimDistrict(region(255));

-- DimClientAccount Indexes
-- For Queries 3,4,5,6
CREATE INDEX idx_dimclientacc_distacc ON DimClientAccount(distAcc_id);
CREATE INDEX idx_dimclientacc_distcli ON DimClientAccount(distCli_id);

-- DimCard Indexes
-- For Query 7
CREATE INDEX idx_dimcard_type ON DimCard(type(50));

-- FactTrans Indexes (CRITICAL for Performance)
-- For Queries 3,6
CREATE INDEX idx_facttrans_clientacc ON FactTrans(clientAcc_id);

-- FactLoan Indexes
-- For Query 7
CREATE INDEX idx_factloan_clientacc ON FactLoan(clientAcc_id);


SELECT 
    TABLE_NAME,
    INDEX_NAME,
    COLUMN_NAME,
    SEQ_IN_INDEX,
    CARDINALITY
FROM information_schema.STATISTICS 
WHERE TABLE_SCHEMA = 'warehouse_db' 
    AND INDEX_NAME LIKE 'idx_%'
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;

-- SUCCESS MESSAGE
SELECT 'Performance indexes added successfully to warehouse_db!' as STATUS;
