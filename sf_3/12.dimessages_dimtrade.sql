-- Insert into dimessages for invalid trade commission
INSERT INTO master.dimessages
SELECT 
    NOW() AS messagedateandtime,
    1 AS batchid,
    'DimTrade' AS MessageSource,
    'Invalid trade commission' AS MessageType,
    'Alert' AS MessageType,
    CONCAT('T_ID = ', tradeid, ', T_COMM = ', commission) AS MessageData
FROM master.dimtrade
WHERE commission IS NOT NULL
  AND commission > (tradeprice * quantity);

-- Insert into dimessages for invalid trade fee
INSERT INTO master.dimessages
SELECT 
    NOW() AS messagedateandtime,
    1 AS batchid,
    'DimTrade' AS MessageSource,
    'Invalid trade fee' AS MessageType,
    'Alert' AS MessageType,
    CONCAT('T_ID = ', tradeid, ', T_CHRG = ', fee) AS MessageData
FROM master.dimtrade
WHERE fee IS NOT NULL
  AND fee > (tradeprice * quantity);
