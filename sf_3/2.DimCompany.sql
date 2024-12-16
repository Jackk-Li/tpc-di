-- Clear the table
TRUNCATE TABLE master.dimcompany;

-- Insert data into dimcompany
INSERT INTO master.dimcompany
SELECT 
    ROW_NUMBER() OVER (ORDER BY CAST(cik AS UNSIGNED)) AS sk,
    CAST(cik AS UNSIGNED) AS companyid,
    s.st_name AS status,
    companyname AS name,
    i.in_name AS industry,
    CASE 
        WHEN sprating NOT IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D') 
            THEN NULL
        ELSE f.sprating 
    END AS sprating,
    CASE
        WHEN sprating NOT IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D')
            THEN NULL
        WHEN f.sprating LIKE 'A%' OR f.sprating LIKE 'BBB%' 
            THEN FALSE
        ELSE TRUE
    END AS islowgrade,
    ceoname AS ceo,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprovince,
    country,
    description,
    STR_TO_DATE(foundingdate, '%Y%m%d') AS foundingdate, 
    CASE 
        WHEN LEAD(CAST(bd.batchdate AS DATE)) OVER (PARTITION BY cik ORDER BY pts ASC) IS NULL 
            THEN TRUE 
        ELSE FALSE 
    END AS iscurrent,
    1 AS batchid,
    STR_TO_DATE(SUBSTRING(f.pts, 1, 8), '%Y%m%d') AS effectivedate,
    '9999-12-31' AS enddate
FROM 
    staging.finwire_cmp f
    JOIN staging.statustype s ON f.status = s.st_id
    JOIN staging.industry i ON f.industryid = i.in_id
    JOIN staging.batchdate bd; -- Join with batchdate