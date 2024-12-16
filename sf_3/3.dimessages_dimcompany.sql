-- Truncate the dimessages table
TRUNCATE TABLE master.dimessages;

-- Insert alerts into dimessages for DimCompany
INSERT INTO master.dimessages (
    messagedateandtime,
    batchid,
    messagesource,
    messagetext,
    messagetype,
    messagedata
)
SELECT 
    NOW() AS messagedateandtime,
    1 AS batchid,
    'DimCompany' AS messagesource,
    'Invalid SPRating' AS messagetext,
    'Alert' AS messagetype,
    CONCAT('CO_ID = ', CAST(cik AS CHAR), ', CO_SP_RATE = ', CAST(sprating AS CHAR)) AS messagedata
FROM staging.finwire_cmp
WHERE sprating NOT IN ('AAA', 'AA', 'AA+', 'AA-', 'A', 'A+', 'A-', 'BBB', 'BBB+', 'BBB-', 
                       'BB', 'BB+', 'BB-', 'B', 'B+', 'B-', 
                       'CCC', 'CCC+', 'CCC-', 'CC', 'C', 'D');
