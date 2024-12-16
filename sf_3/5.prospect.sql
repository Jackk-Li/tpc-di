-- Truncate the prospect table
TRUNCATE TABLE master.prospect;

-- Insert transformed data into prospect
INSERT INTO master.prospect
SELECT
    p.agencyid,
    dd.sk_dateid AS sk_recorddateid,
    dd.sk_dateid AS sk_updatedateid,
    1 AS batchid,
    FALSE AS iscustomer,
    p.lastname,
    p.firstname,
    p.middleinitial,
    p.gender,
    p.addressline1,
    p.addressline2,
    p.postalcode,
    p.city,
    p.state,
    p.country,
    p.phone,
    p.income,
    p.numbercars,
    p.numberchildren,
    p.maritalstatus,
    p.age,
    p.creditrating,
    p.ownorrentflag,
    p.employer,
    p.numbercreditcards,
    p.networth,
    NULLIF(
        TRIM(
            CONCAT_WS(
                '+',
                CASE WHEN p.networth > 1000000 OR p.income > 200000 THEN 'HighValue' ELSE NULL END,
                CASE WHEN p.numberchildren > 3 OR p.numbercreditcards > 5 THEN 'Expenses' ELSE NULL END,
                CASE WHEN p.age > 45 THEN 'Boomer' ELSE NULL END,
                CASE WHEN p.income < 50000 OR p.creditrating < 600 OR p.networth < 100000 THEN 'MoneyAlert' ELSE NULL END,
                CASE WHEN p.numbercars > 3 OR p.numbercreditcards > 7 THEN 'Spender' ELSE NULL END,
                CASE WHEN p.age < 25 AND p.networth > 1000000 THEN 'Inherited' ELSE NULL END
            )
        ), ''
    ) AS marketingnameplate
FROM 
    staging.prospect p
    JOIN staging.batchdate bd ON TRUE
    JOIN master.dimdate dd ON dd.datevalue = bd.batchdate;
