-- =========================
-- CLEAR EXISTING DATA
-- =========================

SET FOREIGN_KEY_CHECKS = 0;

-- Delete all existing data from fact tables first (due to foreign keys)
DELETE FROM FactTrans;
DELETE FROM FactLoan;
DELETE FROM FactOrder;

-- Delete all existing data from dimension tables
DELETE FROM DimCard;
DELETE FROM DimClientAccount;
DELETE FROM DimDistrict;
DELETE FROM DimDate;

SET FOREIGN_KEY_CHECKS = 1;


-- =========================
-- DIM TABLES
-- =========================


-- DimDate (expanded to cover more dates)
INSERT INTO DimDate VALUES
(1, '2025-01-10', 1, 2025, 1, 10),
(2, '2025-02-15', 1, 2025, 2, 15),
(3, '2025-03-20', 1, 2025, 3, 20),
(4, '2025-04-05', 2, 2025, 4, 5),
(5, '2025-05-22', 2, 2025, 5, 22),
(6, '2025-06-30', 2, 2025, 6, 30),
(7, '2025-07-10', 3, 2025, 7, 10),
(8, '2025-08-18', 3, 2025, 8, 18),
(9, '2025-09-01', 3, 2025, 9, 1),
(10, '2025-10-05', 4, 2025, 10, 5),
(11, '2025-01-15', 1, 2025, 1, 15),
(12, '2025-01-20', 1, 2025, 1, 20),
(13, '2025-02-10', 1, 2025, 2, 10),
(14, '2025-02-25', 1, 2025, 2, 25),
(15, '2025-03-05', 1, 2025, 3, 5),
(16, '2025-03-15', 1, 2025, 3, 15),
(17, '2025-04-12', 2, 2025, 4, 12),
(18, '2025-04-20', 2, 2025, 4, 20),
(19, '2025-05-08', 2, 2025, 5, 8),
(20, '2025-05-30', 2, 2025, 5, 30),
(21, '2025-06-10', 2, 2025, 6, 10),
(22, '2025-06-20', 2, 2025, 6, 20),
(23, '2025-07-15', 3, 2025, 7, 15),
(24, '2025-07-25', 3, 2025, 7, 25),
(25, '2025-08-05', 3, 2025, 8, 5),
(26, '2025-08-28', 3, 2025, 8, 28),
(27, '2025-09-10', 3, 2025, 9, 10),
(28, '2025-09-22', 3, 2025, 9, 22),
(29, '2025-10-12', 4, 2025, 10, 12),
(30, '2025-10-20', 4, 2025, 10, 20);


-- DimDistrict (expanded with more districts)
INSERT INTO DimDistrict VALUES
(1, 'Prague', 'Central', 1200000, 10, 0.85, 35000, 0.04, 50000, 1500),
(2, 'Brno', 'South', 380000, 5, 0.75, 28000, 0.05, 15000, 600),
(3, 'Ostrava', 'North', 290000, 4, 0.78, 26000, 0.06, 12000, 450),
(4, 'Plzen', 'West', 170000, 3, 0.80, 27000, 0.05, 8000, 300),
(5, 'Liberec', 'North', 100000, 2, 0.77, 25000, 0.07, 5000, 220),
(6, 'Olomouc', 'East', 230000, 4, 0.79, 26500, 0.055, 10000, 400),
(7, 'Usti nad Labem', 'North', 95000, 2, 0.72, 24000, 0.08, 4500, 200),
(8, 'Hradec Kralove', 'East', 93000, 2, 0.81, 27500, 0.045, 4200, 190),
(9, 'Ceske Budejovice', 'South', 94000, 2, 0.76, 26000, 0.06, 4300, 195),
(10, 'Pardubice', 'East', 90000, 2, 0.80, 27000, 0.05, 4100, 185);


-- DimClientAccount (expanded to 35 accounts)
-- Schema: clientAcc_id, client_id, account_id, type, distCli_id, distAcc_id, date_id, frequency
INSERT INTO DimClientAccount VALUES
(1001, 5001, 3001, 'Standard', 1, 1, 1, 'Monthly'),
(1002, 5002, 3002, 'Premium', 2, 2, 2, 'Weekly'),
(1003, 5003, 3003, 'Standard', 3, 3, 3, 'Monthly'),
(1004, 5004, 3004, 'Business', 1, 1, 4, 'Quarterly'),
(1005, 5005, 3005, 'Standard', 2, 2, 5, 'Monthly'),
(1006, 5006, 3006, 'Premium', 3, 3, 6, 'Weekly'),
(1007, 5007, 3007, 'Standard', 4, 4, 7, 'Monthly'),
(1008, 5008, 3008, 'Business', 5, 5, 8, 'Quarterly'),
(1009, 5009, 3009, 'Standard', 1, 1, 9, 'Monthly'),
(1010, 5010, 3010, 'Premium', 2, 2, 10, 'Weekly'),
(1011, 5011, 3011, 'Standard', 6, 6, 11, 'Monthly'),
(1012, 5012, 3012, 'Premium', 7, 7, 12, 'Weekly'),
(1013, 5013, 3013, 'Standard', 8, 8, 13, 'Monthly'),
(1014, 5014, 3014, 'Business', 9, 9, 14, 'Quarterly'),
(1015, 5015, 3015, 'Standard', 10, 10, 15, 'Monthly'),
(1016, 5016, 3016, 'Premium', 1, 1, 16, 'Weekly'),
(1017, 5017, 3017, 'Standard', 2, 2, 17, 'Monthly'),
(1018, 5018, 3018, 'Business', 3, 3, 18, 'Quarterly'),
(1019, 5019, 3019, 'Standard', 4, 4, 19, 'Monthly'),
(1020, 5020, 3020, 'Premium', 5, 5, 20, 'Weekly'),
(1021, 5021, 3021, 'Standard', 6, 6, 21, 'Monthly'),
(1022, 5022, 3022, 'Standard', 7, 7, 22, 'Monthly'),
(1023, 5023, 3023, 'Premium', 8, 8, 23, 'Weekly'),
(1024, 5024, 3024, 'Business', 9, 9, 24, 'Quarterly'),
(1025, 5025, 3025, 'Standard', 10, 10, 25, 'Monthly'),
(1026, 5026, 3026, 'Standard', 1, 1, 26, 'Monthly'),
(1027, 5027, 3027, 'Premium', 2, 2, 27, 'Weekly'),
(1028, 5028, 3028, 'Business', 3, 3, 28, 'Quarterly'),
(1029, 5029, 3029, 'Standard', 4, 4, 29, 'Monthly'),
(1030, 5030, 3030, 'Premium', 5, 5, 30, 'Weekly'),
(1031, 5031, 3031, 'Standard', 6, 6, 1, 'Monthly'),
(1032, 5032, 3032, 'Business', 7, 7, 2, 'Quarterly'),
(1033, 5033, 3033, 'Standard', 8, 8, 3, 'Monthly'),
(1034, 5034, 3034, 'Premium', 9, 9, 4, 'Weekly'),
(1035, 5035, 3035, 'Standard', 10, 10, 5, 'Monthly');


-- DimCard (expanded to 35 cards)
-- Schema: card_id, clientAcc_id, date_id, type
INSERT INTO DimCard VALUES
(9001, 1001, 1, 'Debit'),
(9002, 1002, 2, 'Credit'),
(9003, 1003, 3, 'Debit'),
(9004, 1004, 4, 'Credit'),
(9005, 1005, 5, 'Debit'),
(9006, 1006, 6, 'Debit'),
(9007, 1007, 7, 'Credit'),
(9008, 1008, 8, 'Debit'),
(9009, 1009, 9, 'Credit'),
(9010, 1010, 10, 'Debit'),
(9011, 1011, 11, 'Debit'),
(9012, 1012, 12, 'Credit'),
(9013, 1013, 13, 'Debit'),
(9014, 1014, 14, 'Credit'),
(9015, 1015, 15, 'Debit'),
(9016, 1016, 16, 'Credit'),
(9017, 1017, 17, 'Debit'),
(9018, 1018, 18, 'Credit'),
(9019, 1019, 19, 'Debit'),
(9020, 1020, 20, 'Credit'),
(9021, 1021, 21, 'Debit'),
(9022, 1022, 22, 'Debit'),
(9023, 1023, 23, 'Credit'),
(9024, 1024, 24, 'Debit'),
(9025, 1025, 25, 'Credit'),
(9026, 1026, 26, 'Debit'),
(9027, 1027, 27, 'Credit'),
(9028, 1028, 28, 'Debit'),
(9029, 1029, 29, 'Credit'),
(9030, 1030, 30, 'Debit'),
(9031, 1031, 1, 'Debit'),
(9032, 1032, 2, 'Credit'),
(9033, 1033, 3, 'Debit'),
(9034, 1034, 4, 'Credit'),
(9035, 1035, 5, 'Debit');


-- =========================
-- FACT TABLES
-- =========================


-- FactOrder (expanded to 35 orders)
-- Schema: order_id, clientAcc_id, account_to, amount, bank_to, k_symbol
INSERT INTO FactOrder VALUES
(2001, 1001, 5020, 500.00, 'KBANK', 'SAVING'),
(2002, 1002, 5030, 1200.50, 'KBANK', 'TRANSFER'),
(2003, 1001, 5040, 850.00, 'MBANK', 'TRANSFER'),
(2004, 1003, 5050, 300.00, 'KBANK', 'INSURANCE'),
(2005, 1004, 5060, 760.25, 'MBANK', 'SAVING'),
(2006, 1005, 5070, 940.75, 'KBANK', 'RENT'),
(2007, 1006, 5080, 1100.00, 'OBANK', 'SAVING'),
(2008, 1007, 5090, 600.00, 'MBANK', 'TRANSFER'),
(2009, 1008, 5100, 450.50, 'KBANK', 'INSURANCE'),
(2010, 1009, 5110, 1300.00, 'KBANK', 'TRANSFER'),
(2011, 1010, 5120, 725.00, 'OBANK', 'SAVING'),
(2012, 1011, 5130, 890.25, 'KBANK', 'TRANSFER'),
(2013, 1012, 5140, 1150.00, 'MBANK', 'INSURANCE'),
(2014, 1013, 5150, 625.50, 'KBANK', 'RENT'),
(2015, 1014, 5160, 980.00, 'OBANK', 'SAVING'),
(2016, 1015, 5170, 440.75, 'KBANK', 'TRANSFER'),
(2017, 1016, 5180, 1320.00, 'MBANK', 'INSURANCE'),
(2018, 1017, 5190, 575.25, 'KBANK', 'SAVING'),
(2019, 1018, 5200, 820.50, 'OBANK', 'TRANSFER'),
(2020, 1019, 5210, 1050.00, 'KBANK', 'RENT'),
(2021, 1020, 5220, 695.75, 'MBANK', 'SAVING'),
(2022, 1021, 5230, 915.00, 'KBANK', 'TRANSFER'),
(2023, 1022, 5240, 1280.50, 'OBANK', 'INSURANCE'),
(2024, 1023, 5250, 535.25, 'KBANK', 'RENT'),
(2025, 1024, 5260, 1420.00, 'MBANK', 'SAVING'),
(2026, 1025, 5270, 780.75, 'KBANK', 'TRANSFER'),
(2027, 1026, 5280, 955.00, 'OBANK', 'INSURANCE'),
(2028, 1027, 5290, 670.50, 'KBANK', 'SAVING'),
(2029, 1028, 5300, 1190.25, 'MBANK', 'TRANSFER'),
(2030, 1029, 5310, 845.00, 'KBANK', 'RENT'),
(2031, 1030, 5320, 1075.75, 'OBANK', 'SAVING'),
(2032, 1031, 5330, 520.00, 'KBANK', 'TRANSFER'),
(2033, 1032, 5340, 1365.50, 'MBANK', 'INSURANCE'),
(2034, 1033, 5350, 715.25, 'KBANK', 'RENT'),
(2035, 1034, 5360, 990.00, 'OBANK', 'SAVING');


-- FactLoan (expanded to 35 loans)
-- Schema: loan_id, clientAcc_id, date_id, status, amount, duration, payments, description
INSERT INTO FactLoan VALUES
(3001, 1001, 1, 'A', 50000, 12, 4200.00, 'Car Loan'),
(3002, 1002, 2, 'B', 80000, 24, 3500.00, 'House Loan'),
(3003, 1001, 3, 'A', 20000, 6, 3400.00, 'Appliance Loan'),
(3004, 1003, 4, 'A', 40000, 18, 2500.00, 'Business Loan'),
(3005, 1004, 5, 'B', 60000, 24, 3100.00, 'Education Loan'),
(3006, 1005, 6, 'C', 45000, 12, 3700.00, 'House Loan'),
(3007, 1006, 7, 'A', 55000, 12, 3000.00, 'Car Loan'),
(3008, 1007, 8, 'B', 40000, 6, 4100.00, 'Appliance Loan'),
(3009, 1008, 9, 'A', 35000, 6, 3800.00, 'Education Loan'),
(3010, 1009, 10, 'C', 50000, 24, 3600.00, 'Car Loan'),
(3011, 1010, 11, 'B', 65000, 18, 3900.00, 'House Loan'),
(3012, 1011, 12, 'A', 28000, 12, 3200.00, 'Car Loan'),
(3013, 1012, 13, 'C', 75000, 24, 3650.00, 'Business Loan'),
(3014, 1013, 14, 'A', 42000, 6, 4050.00, 'Appliance Loan'),
(3015, 1014, 15, 'B', 58000, 18, 3450.00, 'Education Loan'),
(3016, 1015, 16, 'A', 48000, 12, 3850.00, 'House Loan'),
(3017, 1016, 17, 'C', 52000, 24, 3300.00, 'Car Loan'),
(3018, 1017, 18, 'A', 38000, 6, 4250.00, 'Appliance Loan'),
(3019, 1018, 19, 'B', 62000, 18, 3550.00, 'Business Loan'),
(3020, 1019, 20, 'B', 70000, 24, 3750.00, 'House Loan'),
(3021, 1020, 21, 'A', 32000, 12, 3950.00, 'Car Loan'),
(3022, 1021, 22, 'C', 54000, 18, 3400.00, 'Education Loan'),
(3023, 1022, 23, 'A', 46000, 6, 4150.00, 'Appliance Loan'),
(3024, 1023, 24, 'B', 68000, 24, 3600.00, 'House Loan'),
(3025, 1024, 25, 'A', 44000, 12, 3800.00, 'Business Loan'),
(3026, 1025, 26, 'C', 56000, 18, 3500.00, 'Car Loan'),
(3027, 1026, 27, 'A', 36000, 6, 4300.00, 'Appliance Loan'),
(3028, 1027, 28, 'B', 72000, 24, 3700.00, 'House Loan'),
(3029, 1028, 29, 'A', 50000, 12, 4000.00, 'Education Loan'),
(3030, 1029, 30, 'C', 60000, 18, 3600.00, 'Business Loan'),
(3031, 1030, 1, 'A', 40000, 6, 4200.00, 'Car Loan'),
(3032, 1031, 2, 'B', 78000, 24, 3550.00, 'House Loan'),
(3033, 1032, 3, 'A', 34000, 12, 3900.00, 'Appliance Loan'),
(3034, 1033, 4, 'C', 64000, 18, 3650.00, 'Education Loan'),
(3035, 1034, 5, 'B', 48000, 24, 3750.00, 'Business Loan');


-- FactTrans (expanded to 40+ transactions)
-- Schema: trans_id, clientAcc_id, date_id, account, type, operation, k_symbol, bank, amount, balance
INSERT INTO FactTrans VALUES
(4001, 1001, 1, 5011, 'Credit', 'Deposit', 'SAVING', 'KBANK', 500.00, 5500.00),
(4002, 1001, 2, 5011, 'Debit', 'Withdrawal', 'CASH', 'KBANK', 200.00, 5300.00),
(4003, 1002, 2, 5012, 'Credit', 'Transfer', 'TRANSFER', 'KBANK', 800.00, 6200.00),
(4004, 1003, 3, 5013, 'Debit', 'Bill Payment', 'RENT', 'MBANK', 400.00, 3800.00),
(4005, 1004, 4, 5014, 'Credit', 'Deposit', 'SAVING', 'OBANK', 1000.00, 7200.00),
(4006, 1005, 5, 5015, 'Credit', 'Deposit', 'SAVING', 'KBANK', 650.00, 7800.00),
(4007, 1006, 6, 5016, 'Debit', 'Withdrawal', 'CASH', 'KBANK', 500.00, 7300.00),
(4008, 1007, 7, 5017, 'Credit', 'Transfer', 'TRANSFER', 'MBANK', 950.00, 8600.00),
(4009, 1008, 8, 5018, 'Debit', 'Payment', 'RENT', 'OBANK', 400.00, 8200.00),
(4010, 1009, 9, 5019, 'Credit', 'Deposit', 'SAVING', 'KBANK', 1200.00, 9200.00),
(4011, 1010, 10, 5020, 'Credit', 'Deposit', 'SAVING', 'KBANK', 750.00, 8950.00),
(4012, 1011, 11, 5021, 'Debit', 'Withdrawal', 'CASH', 'MBANK', 300.00, 6700.00),
(4013, 1012, 12, 5022, 'Credit', 'Transfer', 'TRANSFER', 'OBANK', 1100.00, 9300.00),
(4014, 1013, 13, 5023, 'Debit', 'Payment', 'RENT', 'KBANK', 550.00, 7450.00),
(4015, 1014, 14, 5024, 'Credit', 'Deposit', 'SAVING', 'KBANK', 890.00, 8340.00),
(4016, 1015, 15, 5025, 'Credit', 'Transfer', 'TRANSFER', 'MBANK', 720.00, 9060.00),
(4017, 1016, 16, 5026, 'Debit', 'Withdrawal', 'CASH', 'OBANK', 450.00, 8610.00),
(4018, 1017, 17, 5027, 'Credit', 'Deposit', 'SAVING', 'KBANK', 980.00, 9590.00),
(4019, 1018, 18, 5028, 'Debit', 'Bill Payment', 'INSURANCE', 'KBANK', 620.00, 8970.00),
(4020, 1019, 19, 5029, 'Credit', 'Transfer', 'TRANSFER', 'MBANK', 1050.00, 10020.00),
(4021, 1020, 20, 5030, 'Credit', 'Deposit', 'SAVING', 'OBANK', 840.00, 10860.00),
(4022, 1021, 21, 5031, 'Debit', 'Withdrawal', 'CASH', 'KBANK', 380.00, 10480.00),
(4023, 1022, 22, 5032, 'Credit', 'Transfer', 'TRANSFER', 'KBANK', 925.00, 11405.00),
(4024, 1023, 23, 5033, 'Debit', 'Payment', 'RENT', 'MBANK', 475.00, 10930.00),
(4025, 1024, 24, 5034, 'Credit', 'Deposit', 'SAVING', 'OBANK', 1150.00, 12080.00),
(4026, 1025, 25, 5035, 'Credit', 'Deposit', 'SAVING', 'KBANK', 685.00, 12765.00),
(4027, 1026, 26, 5036, 'Debit', 'Withdrawal', 'CASH', 'KBANK', 520.00, 12245.00),
(4028, 1027, 27, 5037, 'Credit', 'Transfer', 'TRANSFER', 'MBANK', 1080.00, 13325.00),
(4029, 1028, 28, 5038, 'Debit', 'Bill Payment', 'INSURANCE', 'OBANK', 590.00, 12735.00),
(4030, 1029, 29, 5039, 'Credit', 'Deposit', 'SAVING', 'KBANK', 970.00, 13705.00),
(4031, 1030, 30, 5040, 'Credit', 'Transfer', 'TRANSFER', 'KBANK', 815.00, 14520.00),
(4032, 1031, 1, 5041, 'Debit', 'Withdrawal', 'CASH', 'MBANK', 425.00, 14095.00),
(4033, 1032, 2, 5042, 'Credit', 'Deposit', 'SAVING', 'OBANK', 1240.00, 15335.00),
(4034, 1033, 3, 5043, 'Debit', 'Payment', 'RENT', 'KBANK', 640.00, 14695.00),
(4035, 1034, 4, 5044, 'Credit', 'Transfer', 'TRANSFER', 'KBANK', 1095.00, 15790.00),
(4036, 1035, 5, 5045, 'Credit', 'Deposit', 'SAVING', 'MBANK', 755.00, 16545.00),
(4037, 1001, 11, 5011, 'Credit', 'Deposit', 'SAVING', 'KBANK', 600.00, 17145.00),
(4038, 1002, 12, 5012, 'Debit', 'Withdrawal', 'CASH', 'KBANK', 350.00, 16795.00),
(4039, 1003, 13, 5013, 'Credit', 'Transfer', 'TRANSFER', 'MBANK', 880.00, 17675.00),
(4040, 1004, 14, 5014, 'Credit', 'Deposit', 'SAVING', 'OBANK', 1125.00, 18800.00),
(4041, 1005, 15, 5015, 'Debit', 'Payment', 'INSURANCE', 'KBANK', 495.00, 18305.00),
(4042, 1006, 16, 5016, 'Credit', 'Transfer', 'TRANSFER', 'KBANK', 1015.00, 19320.00);


SET FOREIGN_KEY_CHECKS = 1;


-- SUCCESS MESSAGE
SELECT 'Sample Data Inserted Successfully!' AS STATUS;