-- Step 1: Truncate the target table
TRUNCATE TABLE master.dimtrade;

-- Step 2: Insert data

INSERT INTO master.dimtrade
WITH trade_creation AS (
    SELECT
        th.th_t_id AS t_id,
        MIN(CONCAT(
            LPAD(DATE_FORMAT(th.th_dts, '%Y%m%d'), 8, '0'),
            LPAD(DATE_FORMAT(th.th_dts, '%H%i%s'), 6, '0')
        )) AS trade_creation
    FROM staging.tradehistory th
    WHERE th.th_st_id IN ('SBMT', 'PNDG') -- Filter early
    GROUP BY th.th_t_id
),

latest_trade_history AS (
    SELECT
        th.th_t_id AS t_id,
        th.th_st_id,
        th.th_dts,
        ROW_NUMBER() OVER (PARTITION BY th.th_t_id ORDER BY th.th_dts DESC) AS rn
    FROM staging.tradehistory th
    WHERE th.th_st_id IN ('CMPT', 'CNCL', 'SBMT', 'PNDG') -- Filter early
),

trades AS (
    SELECT
        t.t_id,
        a.sk_brokerid,
        CASE
            WHEN (lth.th_st_id = 'SBMT' AND t.t_tt_id IN ('TMB', 'TMS')) OR lth.th_st_id = 'PNDG'
            THEN DATE_FORMAT(lth.th_dts, '%Y%m%d') + 0
            ELSE NULL
        END AS sk_createdateid,
        CASE
            WHEN (lth.th_st_id = 'SBMT' AND t.t_tt_id IN ('TMB', 'TMS')) OR lth.th_st_id = 'PNDG'
            THEN DATE_FORMAT(lth.th_dts, '%H%i%s') + 0
            ELSE NULL
        END AS sk_createtimeid,
        CASE
            WHEN lth.th_st_id IN ('CMPT', 'CNCL')
            THEN DATE_FORMAT(lth.th_dts, '%Y%m%d') + 0
            ELSE NULL
        END AS sk_closedateid,
        CASE
            WHEN lth.th_st_id IN ('CMPT', 'CNCL')
            THEN DATE_FORMAT(lth.th_dts, '%H%i%s') + 0
            ELSE NULL
        END AS sk_closetimeid,
        st.st_name,
        tt.tt_name,
        t.t_is_cash = 1 AS t_is_cash,
        s.sk_securityid,
        s.sk_companyid,
        t.t_qty,
        t.t_bid_price,
        a.sk_customerid,
        a.sk_accountid,
        t.t_exec_name,
        t.t_trade_price,
        t.t_chrg,
        t.t_comm,
        t.t_tax,
        1 AS batchid,
        tc.trade_creation
    FROM staging.trade t
    INNER JOIN latest_trade_history lth
        ON t.t_id = lth.t_id AND lth.rn = 1
    LEFT JOIN trade_creation tc
        ON t.t_id = tc.t_id
    INNER JOIN master.dimaccount a
        ON t.t_ca_id = a.accountid
        AND lth.th_dts BETWEEN a.effectivedate AND a.enddate
    INNER JOIN master.statustype st
        ON t.t_st_id = st.st_id
    INNER JOIN master.tradetype tt
        ON t.t_tt_id = tt.tt_id
    INNER JOIN master.dimsecurity s
        ON t.t_s_symb = s.symbol
        AND lth.th_dts BETWEEN s.effectivedate AND s.enddate
)

SELECT 
    t_id,
    sk_brokerid,
    COALESCE(sk_createdateid, LEFT(trade_creation, 8) + 0) AS sk_createdateid,
    COALESCE(sk_createtimeid, RIGHT(trade_creation, 6) + 0) AS sk_createtimeid,
    sk_closedateid,
    sk_closetimeid,
    st_name,
    tt_name,
    t_is_cash,
    sk_securityid,
    sk_companyid,
    t_qty,
    t_bid_price,
    sk_customerid,
    sk_accountid,
    t_exec_name,
    t_trade_price,
    t_chrg,
    t_comm,
    t_tax,
    batchid
FROM trades;
