-- Truncate the target table
TRUNCATE TABLE master.dimcustomer;

-- Insert data into master.dimcustomer
INSERT INTO master.dimcustomer (
    sk_customerid,
    customerid,
    taxid,
    status,
    lastname,
    firstname,
    middleinitial,
    gender,
    tier,
    dob,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    phone1,
    phone2,
    phone3,
    email1,
    email2,
    nationaltaxratedesc,
    nationaltaxrate,
    localtaxratedesc,
    localtaxrate,
    agencyid,
    creditrating,
    networth,
    marketingnameplate,
    iscurrent,
    batchid,
    effectivedate,
    enddate
)
SELECT
    ROW_NUMBER() OVER (ORDER BY cm.c_id) AS sk_customerid,
    cm.c_id AS customerid,
    cm.c_tax_id AS taxid,
    CASE
        WHEN cm.actiontype = 'INACT' THEN 'INACTIVE'
        ELSE 'ACTIVE'
    END AS status,
    cm.c_l_name AS lastname,
    cm.c_f_name AS firstname,
    cm.c_m_name AS middleinitial,
    CASE
        WHEN UPPER(cm.c_gndr) IN ('M', 'F') THEN UPPER(cm.c_gndr)
        ELSE 'U'
    END AS gender,
    cm.c_tier AS tier,
    cm.c_dob AS dob,
    cm.c_adline1 AS addressline1,
    cm.c_adline2 AS addressline2,
    cm.c_zipcode AS postalcode,
    cm.c_city AS city,
    cm.c_state_prov AS stateprov,
    cm.c_ctry AS country,
    CASE
        WHEN cm.c_p_1_ctry_code IS NOT NULL AND cm.c_p_1_area_code IS NOT NULL AND cm.c_p_1_local IS NOT NULL
        THEN CONCAT('+', cm.c_p_1_ctry_code, ' (', cm.c_p_1_area_code, ') ', cm.c_p_1_local, COALESCE(cm.c_p_1_ext, ''))
        WHEN cm.c_p_1_ctry_code IS NULL AND cm.c_p_1_area_code IS NOT NULL AND cm.c_p_1_local IS NOT NULL
        THEN CONCAT('(', cm.c_p_1_area_code, ') ', cm.c_p_1_local, COALESCE(cm.c_p_1_ext, ''))
        WHEN cm.c_p_1_area_code IS NULL AND cm.c_p_1_local IS NOT NULL
        THEN CONCAT(cm.c_p_1_local, COALESCE(cm.c_p_1_ext, ''))
        ELSE NULL
    END AS phone1,
    CASE
        WHEN cm.c_p_2_ctry_code IS NOT NULL AND cm.c_p_2_area_code IS NOT NULL AND cm.c_p_2_local IS NOT NULL
        THEN CONCAT('+', cm.c_p_2_ctry_code, ' (', cm.c_p_2_area_code, ') ', cm.c_p_2_local, COALESCE(cm.c_p_2_ext, ''))
        WHEN cm.c_p_2_ctry_code IS NULL AND cm.c_p_2_area_code IS NOT NULL AND cm.c_p_2_local IS NOT NULL
        THEN CONCAT('(', cm.c_p_2_area_code, ') ', cm.c_p_2_local, COALESCE(cm.c_p_2_ext, ''))
        WHEN cm.c_p_2_area_code IS NULL AND cm.c_p_2_local IS NOT NULL
        THEN CONCAT(cm.c_p_2_local, COALESCE(cm.c_p_2_ext, ''))
        ELSE NULL
    END AS phone2,
    CASE
        WHEN cm.c_p_3_ctry_code IS NOT NULL AND cm.c_p_3_area_code IS NOT NULL AND cm.c_p_3_local IS NOT NULL
        THEN CONCAT('+', cm.c_p_3_ctry_code, ' (', cm.c_p_3_area_code, ') ', cm.c_p_3_local, COALESCE(cm.c_p_3_ext, ''))
        WHEN cm.c_p_3_ctry_code IS NULL AND cm.c_p_3_area_code IS NOT NULL AND cm.c_p_3_local IS NOT NULL
        THEN CONCAT('(', cm.c_p_3_area_code, ') ', cm.c_p_3_local, COALESCE(cm.c_p_3_ext, ''))
        WHEN cm.c_p_3_area_code IS NULL AND cm.c_p_3_local IS NOT NULL
        THEN CONCAT(cm.c_p_3_local, COALESCE(cm.c_p_3_ext, ''))
        ELSE NULL
    END AS phone3,
    cm.c_prim_email AS email1,
    cm.c_alt_email AS email2,
    ntr.tx_name AS nationaltaxratedesc,
    ntr.tx_rate AS nationaltaxrate,
    ltr.tx_name AS localtaxratedesc,
    ltr.tx_rate AS localtaxrate,
    NULL AS agencyid, -- Populate if available
    NULL AS creditrating, -- Populate if available
    NULL AS networth, -- Populate if available
    NULL AS marketingnameplate, -- Populate if available
    MAX(cm.actionts) OVER (PARTITION BY cm.c_id) = cm.actionts AS iscurrent,
    1 AS batchid,
    cm.actionts AS effectivedate,
    '9999-12-31' AS enddate
FROM staging.customermgmt cm
LEFT JOIN master.taxrate ntr ON cm.c_nat_tx_id = ntr.tx_id
LEFT JOIN master.taxrate ltr ON cm.c_lcl_tx_id = ltr.tx_id
WHERE cm.actiontype IN ('NEW', 'UPDCUST', 'INACT');

