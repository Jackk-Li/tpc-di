-- factmarkethistory
TRUNCATE TABLE master.factmarkethistory;

INSERT INTO master.factmarkethistory
WITH market_dates_daily AS (
    SELECT 
        dm.dm_s_symb,
        dm.dm_date,
        dm.dm_close,
        dm.dm_high,
        dm.dm_low,
        dm.dm_vol,
        dd.sk_dateid
    FROM staging.dailymarket dm
    INNER JOIN master.dimdate dd 
        ON dm.dm_date = dd.datevalue
    WHERE dm.dm_date >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) -- Filter for the last 5 years
    ORDER BY dm.dm_s_symb, dm.dm_date DESC
),

quarters AS (
    SELECT
        f.sk_companyid,
        f.fi_qtr_start_date,
        SUM(fi_basic_eps) OVER (PARTITION BY c.companyid ORDER BY f.fi_qtr_start_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS eps_qtr_sum,
        (SELECT MIN(f2.fi_qtr_start_date)
         FROM master.financial f2
         WHERE f2.sk_companyid = f.sk_companyid
           AND f2.fi_qtr_start_date > f.fi_qtr_start_date) AS next_qtr_start
    FROM master.financial f
    INNER JOIN master.dimcompany c
        ON f.sk_companyid = c.sk_companyid
    WHERE f.fi_qtr_start_date >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) -- Filter for the last 5 years
),

high_low AS (
    SELECT
        dm_s_symb,
        dm_date,
        dm_close,
        dm_high,
        dm_low,
        dm_vol,
        MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date ROWS BETWEEN 363 PRECEDING AND CURRENT ROW) AS fiftytwoweekhigh,
        MIN(dm_low) OVER (PARTITION BY dm_s_symb ORDER BY dm_date ROWS BETWEEN 363 PRECEDING AND CURRENT ROW) AS fiftytwoweeklow
    FROM market_dates_daily
),

high_date AS (
    SELECT
        hl.dm_s_symb,
        hl.dm_date,
        hl.dm_close,
        hl.dm_high,
        hl.dm_low,
        hl.dm_vol,
        hl.fiftytwoweekhigh,
        hl.fiftytwoweeklow,
        MAX(mdd.dm_date) AS sk_fiftytwoweekhighdate
    FROM high_low hl
    INNER JOIN market_dates_daily mdd
        ON hl.dm_s_symb = mdd.dm_s_symb
        AND hl.fiftytwoweekhigh = mdd.dm_high
        AND mdd.dm_date <= hl.dm_date
        AND mdd.dm_date >= DATE_SUB(hl.dm_date, INTERVAL 2 YEAR) -- Narrow to the last 2 year for high
    GROUP BY hl.dm_s_symb, hl.dm_date, hl.dm_close, hl.dm_high, hl.dm_low, hl.dm_vol, hl.fiftytwoweekhigh, hl.fiftytwoweeklow
),

low_date AS (
    SELECT
        hl.dm_s_symb,
        hl.dm_date,
        hl.dm_close,
        hl.dm_high,
        hl.dm_low,
        hl.dm_vol,
        hl.fiftytwoweekhigh,
        hl.fiftytwoweeklow,
        hl.sk_fiftytwoweekhighdate,
        MAX(mdd.dm_date) AS sk_fiftytwoweeklowdate
    FROM high_date hl
    INNER JOIN market_dates_daily mdd
        ON hl.dm_s_symb = mdd.dm_s_symb
        AND hl.fiftytwoweeklow = mdd.dm_low
        AND mdd.dm_date <= hl.dm_date
        AND mdd.dm_date >= DATE_SUB(hl.dm_date, INTERVAL 2 YEAR) -- Narrow to the last 2 year for low
    GROUP BY hl.dm_s_symb, hl.dm_date, hl.dm_close, hl.dm_high, hl.dm_low, hl.dm_vol, hl.fiftytwoweekhigh, hl.fiftytwoweeklow, hl.sk_fiftytwoweekhighdate
),

final_output AS (
    SELECT
        s.sk_securityid,
        s.sk_companyid,
        DATE_FORMAT(ld.dm_date, '%Y%m%d') AS sk_dateid,
        CASE
            WHEN q.eps_qtr_sum IS NOT NULL AND q.eps_qtr_sum != 0 THEN ROUND(ld.dm_close / q.eps_qtr_sum, 2)
            ELSE NULL
        END AS peratio,
        CASE
            WHEN ld.dm_close != 0 THEN ROUND((s.dividend / ld.dm_close) * 100, 2)
            ELSE NULL
        END AS yield,
        ld.fiftytwoweekhigh,
        DATE_FORMAT(ld.sk_fiftytwoweekhighdate, '%Y%m%d') AS sk_fiftytwoweekhighdate,
        ld.fiftytwoweeklow,
        DATE_FORMAT(ld.sk_fiftytwoweeklowdate, '%Y%m%d') AS sk_fiftytwoweeklowdate,
        ld.dm_close AS closeprice,
        ld.dm_high AS dayhigh,
        ld.dm_low AS daylow,
        ld.dm_vol AS volume,
        1 AS batchid
    FROM low_date ld 
    INNER JOIN master.dimsecurity s 
        ON ld.dm_s_symb = s.symbol 
        AND ld.dm_date BETWEEN s.effectivedate AND s.enddate
    INNER JOIN quarters q 
        ON s.sk_companyid = q.sk_companyid 
        AND q.fi_qtr_start_date <= ld.dm_date 
        AND q.next_qtr_start > ld.dm_date
)
SELECT * FROM final_output;
