-- Truncate the `dimsecurity` table
TRUNCATE TABLE master.dimsecurity;

-- Insert into `dimsecurity` table
INSERT INTO master.dimsecurity
SELECT 
    ROW_NUMBER() OVER() AS sk_securityid,
    f.symbol,
    f.issuetype AS issue,
    s.st_name AS status,
    f.name,
    f.exid AS exchangeid,
    c.sk_companyid AS sk_companyid,
    CAST(f.shout AS DECIMAL(12)) AS sharesoutstanding,
    STR_TO_DATE(LEFT(f.firsttradedate, 8), '%Y%m%d') AS firsttradedate,
    STR_TO_DATE(LEFT(f.firsttradeexchg, 8), '%Y%m%d') AS firsttradeonexchange,
    CAST(f.dividend AS DECIMAL(10, 2)) AS dividend,
    CASE 
        WHEN (
            SELECT MAX(batchdate) 
            FROM staging.batchdate
        ) IS NULL THEN TRUE 
        ELSE FALSE 
    END AS iscurrent,
    1 AS batchid,
    STR_TO_DATE(LEFT(f.pts, 8), '%Y%m%d') AS effectivedate,
    STR_TO_DATE('9999-12-31', '%Y-%m-%d') AS enddate
FROM
    staging.finwire_sec f
JOIN staging.statustype s 
    ON f.status = s.st_id
JOIN master.dimcompany c
    ON (
        TRIM(LEADING '0' FROM f.conameorcik) = CAST(c.companyid AS CHAR) 
        OR f.conameorcik = c.name
    )
WHERE 
    STR_TO_DATE(LEFT(f.pts, 8), '%Y%m%d') >= c.effectivedate
    AND STR_TO_DATE(LEFT(f.pts, 8), '%Y%m%d') < c.enddate;
