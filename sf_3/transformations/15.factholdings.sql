-- Truncate the factholdings table
TRUNCATE TABLE master.factholdings;

-- Insert data into factholdings
CREATE INDEX idx_hh_t_id ON staging.holdinghistory (hh_t_id);
CREATE INDEX idx_tradeid ON master.dimtrade (tradeid);

INSERT INTO master.factholdings (
    tradeid,
    currenttradeid,
    sk_customerid,
    sk_accountid,
    sk_securityid,
    sk_companyid,
    sk_dateid,
    sk_timeid,
    currentprice,
    currentholding,
    batchid
)
SELECT
    h.hh_h_t_id AS tradeid,
    t.tradeid AS currenttradeid,
    t.sk_customerid AS sk_customerid,
    t.sk_accountid AS sk_accountid,
    t.sk_securityid AS sk_securityid,
    t.sk_companyid AS sk_companyid,
    t.sk_closedateid AS sk_dateid,
    t.sk_closetimeid AS sk_timeid,
    t.tradeprice AS currentprice,
    h.hh_after_qty AS currentholding,
    1 AS batchid
FROM staging.holdinghistory h
JOIN master.dimtrade t
    ON h.hh_t_id = t.tradeid;
