-- Insert alerts into dimessages for FactMarketHistory
INSERT INTO master.dimessages (
    messagedateandtime,
    batchid,
    messagesource,
    messagetype,
    messagetext,
    messagedata
)
SELECT
    NOW() AS messagedateandtime,
    1 AS batchid,
    'FactMarketHistory' AS messagesource,
    'Alert' AS messagetype,
    'No earnings for company' AS messagetext,
    CONCAT('DM_S_SYMB = ', s.symbol) AS messagedata
FROM master.factmarkethistory fmh
INNER JOIN master.dimsecurity s
    ON fmh.sk_securityid = s.sk_securityid
WHERE fmh.peratio IS NULL
   OR fmh.peratio = 0;
