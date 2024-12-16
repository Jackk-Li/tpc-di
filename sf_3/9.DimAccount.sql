-- Truncate the `dimaccount` table
TRUNCATE TABLE master.dimaccount;

-- Insert into `dimaccount` table
INSERT INTO master.dimaccount
WITH max_actionts AS (
    SELECT
        ca_id,
        MAX(actionts) AS max_actionts
    FROM
        staging.customermgmt
    GROUP BY
        ca_id
),
account AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cm.ca_id) AS sk,
        cm.ca_id,
        b.sk_brokerid,
        c.sk_customerid,
        CASE
            WHEN cm.actiontype IN ('NEW', 'ADDACCT', 'UPDACCT', 'UPDCUST') THEN 'ACTIVE'
            ELSE 'INACTIVE'
        END AS status,
        cm.ca_name,
        cm.ca_tax_st,
        CASE
            WHEN cm.actionts = ma.max_actionts THEN TRUE
            ELSE FALSE
        END AS iscurrent,
        1 AS batchid,
        DATE(cm.actionts) AS effectivedate,
        DATE('9999-12-31') AS enddate,
        cm.actiontype
    FROM
        staging.customermgmt cm
    CROSS JOIN
        staging.batchdate bd
    LEFT JOIN
        master.dimbroker b ON cm.ca_b_id = b.brokerid
    LEFT JOIN
        master.dimcustomer c ON cm.c_id = c.customerid
        AND DATE(cm.actionts) BETWEEN c.effectivedate AND c.enddate
    LEFT JOIN
        max_actionts ma ON cm.ca_id = ma.ca_id
    WHERE
        cm.actiontype IN ('NEW', 'ADDACCT', 'UPDACCT', 'CLOSEACCT', 'UPDCUST', 'INACT')
),
ca_new AS (
    SELECT *
    FROM account
    WHERE actiontype = 'NEW'
),
ca_not_new AS (
    SELECT
        COALESCE(a.sk, cn.sk) AS sk,
        COALESCE(a.ca_id, cn.ca_id) AS ca_id,
        COALESCE(a.sk_brokerid, cn.sk_brokerid) AS sk_brokerid,
        COALESCE(a.sk_customerid, cn.sk_customerid) AS sk_customerid,
        COALESCE(a.status, cn.status) AS status,
        COALESCE(a.ca_name, cn.ca_name) AS ca_name,
        COALESCE(a.ca_tax_st, cn.ca_tax_st) AS ca_tax_st,
        COALESCE(a.iscurrent, cn.iscurrent) AS iscurrent,
        COALESCE(a.batchid, cn.batchid) AS batchid,
        COALESCE(a.effectivedate, cn.effectivedate) AS effectivedate,
        COALESCE(a.enddate, cn.enddate) AS enddate,
        a.actiontype
    FROM account a
    INNER JOIN ca_new cn ON a.ca_id = cn.ca_id
    WHERE a.actiontype != 'NEW'
),
ca_all AS (
    SELECT * FROM ca_new
    UNION ALL
    SELECT * FROM ca_not_new
)
SELECT
    sk,
    ca_id,
    sk_brokerid,
    sk_customerid,
    status,
    ca_name,
    ca_tax_st,
    iscurrent,
    batchid,
    effectivedate,
    enddate
FROM ca_all;