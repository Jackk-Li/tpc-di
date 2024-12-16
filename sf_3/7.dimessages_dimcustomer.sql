-- Insert into dimessages for invalid customer tier
INSERT INTO master.dimessages
SELECT 
    NOW() AS messagedateandtime,
    1 AS batchid,
    'DimCustomer' AS MessageSource,
    'Invalid customer tier' AS MessageType,
    'Alert' AS MessageType,
    CONCAT('C_ID = ', customerid, ', C_TIER = ', tier) AS MessageData
FROM master.dimcustomer
WHERE tier NOT BETWEEN 1 AND 3;

-- Insert into dimessages for DOB out of range
INSERT INTO master.dimessages
SELECT 
    NOW() AS messagedateandtime,
    1 AS batchid,
    'DimCustomer' AS MessageSource,
    'DOB out of range' AS MessageType,
    'Alert' AS MessageType,
    CONCAT('C_ID = ', customerid, ', C_DOB = ', dob) AS MessageData
FROM master.dimcustomer
WHERE dob < DATE_SUB((SELECT batchdate FROM staging.batchdate), INTERVAL 100 YEAR)
   OR dob > (SELECT batchdate FROM staging.batchdate);
