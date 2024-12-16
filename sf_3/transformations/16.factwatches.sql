-- Step 1: Ensure indexes exist
-- Create indexes on commonly used columns for joins and filters
-- Check and create index on watchhistory (w_c_id, w_s_symb)
DROP INDEX IF EXISTS idx_watchhistory_c_id_symb ON staging.watchhistory;
CREATE INDEX idx_watchhistory_c_id_symb ON staging.watchhistory (w_c_id, w_s_symb);

-- Check and create index on watchhistory (w_action)
DROP INDEX IF EXISTS idx_watchhistory_action ON staging.watchhistory;
CREATE INDEX idx_watchhistory_action ON staging.watchhistory (w_action);

-- Check and create index on dimcustomer (customerid)
DROP INDEX IF EXISTS idx_dimcustomer_customerid ON master.dimcustomer;
CREATE INDEX idx_dimcustomer_customerid ON master.dimcustomer (customerid);

-- Check and create index on dimsecurity (symbol)
DROP INDEX IF EXISTS idx_dimsecurity_symbol ON master.dimsecurity;
CREATE INDEX idx_dimsecurity_symbol ON master.dimsecurity (symbol);

-- Check and create index on dimdate (datevalue)
DROP INDEX IF EXISTS idx_dimdate_datevalue ON master.dimdate;
CREATE INDEX idx_dimdate_datevalue ON master.dimdate (datevalue);


-- Step 2: Truncate the target table
TRUNCATE TABLE master.factwatches;

-- Step 3: Optimized query to insert data into `factwatches`
INSERT INTO master.factwatches (
    sk_customerid,
    sk_securityid,
    sk_dateid_dateplaced,
    sk_dateid_dateremoved,
    batchid
)
SELECT 
    c.sk_customerid,
    s.sk_securityid,
    d1.sk_dateid AS sk_dateid_dateplaced,
    d2.sk_dateid AS sk_dateid_dateremoved,
    1 AS batchid
FROM staging.watchhistory w1
INNER JOIN staging.watchhistory w2
    ON w1.w_c_id = w2.w_c_id
    AND w1.w_s_symb = w2.w_s_symb
    AND w1.w_action = 'ACTV'
    AND w2.w_action = 'CNCL'
INNER JOIN master.dimcustomer c
    ON w1.w_c_id = c.customerid
INNER JOIN master.dimsecurity s
    ON w1.w_s_symb = s.symbol
INNER JOIN master.dimdate d1
    ON DATE(w1.w_dts) = d1.datevalue
INNER JOIN master.dimdate d2
    ON DATE(w2.w_dts) = d2.datevalue;
