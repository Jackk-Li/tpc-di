-- Step 1: Ensure indexes exist
-- Create indexes on commonly used columns for joins and filters

-- 创建索引
CREATE INDEX idx_watchhistory_c_id_symb ON staging.watchhistory (w_c_id, w_s_symb);
CREATE INDEX idx_watchhistory_action ON staging.watchhistory (w_action);
CREATE INDEX idx_dimcustomer_customerid ON master.dimcustomer (customerid);
CREATE INDEX idx_dimsecurity_symbol ON master.dimsecurity (symbol);
CREATE INDEX idx_dimdate_datevalue ON master.dimdate (datevalue);

-- Step 2: Truncate the target table
TRUNCATE TABLE master.factwatches;

-- Step 3: 重写查询，使用子查询替代 WITH 子句
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
FROM (
    SELECT 
        w_c_id,
        w_s_symb,
        MAX(CASE WHEN w_action = 'ACTV' THEN w_dts END) AS actv_dts,
        MAX(CASE WHEN w_action = 'CNCL' THEN w_dts END) AS cncl_dts
    FROM staging.watchhistory
    WHERE w_action IN ('ACTV', 'CNCL')
    GROUP BY w_c_id, w_s_symb
    HAVING actv_dts IS NOT NULL AND cncl_dts IS NOT NULL
) AS fw
INNER JOIN master.dimcustomer c
    ON fw.w_c_id = c.customerid
INNER JOIN master.dimsecurity s
    ON fw.w_s_symb = s.symbol
INNER JOIN master.dimdate d1
    ON DATE(fw.actv_dts) = d1.datevalue
INNER JOIN master.dimdate d2
    ON DATE(fw.cncl_dts) = d2.datevalue;

-- 删除索引（如果不再需要）
DROP INDEX idx_watchhistory_c_id_symb ON staging.watchhistory;
DROP INDEX idx_watchhistory_action ON staging.watchhistory;
DROP INDEX idx_dimcustomer_customerid ON master.dimcustomer;
DROP INDEX idx_dimsecurity_symbol ON master.dimsecurity;
DROP INDEX idx_dimdate_datevalue ON master.dimdate;