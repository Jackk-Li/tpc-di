-- Truncate the factcashbalances table
TRUNCATE TABLE master.factcashbalances;

-- Insert transformed and aggregated data into factcashbalances
INSERT INTO master.factcashbalances
WITH agg AS (
    SELECT 
        a.sk_customerid AS sk_customerid,
        a.sk_accountid AS sk_accountid,
        d.sk_dateid AS sk_dateid,
        SUM(c.ct_amt) AS ct_amt_day
    FROM staging.cashtransaction c
    INNER JOIN master.dimaccount a
        ON c.ct_ca_id = a.accountid
        AND DATE(c.ct_dts) >= a.effectivedate
        AND DATE(c.ct_dts) < a.enddate
    INNER JOIN master.dimdate d
        ON DATE(c.ct_dts) = d.datevalue
    GROUP BY 
        a.sk_customerid,
        a.sk_accountid,
        d.sk_dateid
),
final_output AS (
    SELECT
        sk_customerid,
        sk_accountid,
        sk_dateid,
        SUM(ct_amt_day) OVER (
            PARTITION BY sk_accountid 
            ORDER BY sk_dateid 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cash,
        1 AS batchid
    FROM agg
)
SELECT * FROM final_output;
