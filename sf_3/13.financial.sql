-- Truncate the financial table
TRUNCATE TABLE master.financial;

-- Insert transformed data into financial with validation
INSERT INTO master.financial 
SELECT 
    c.sk_companyid AS sk_companyid,
    -- Validate year is numeric
    CASE 
        WHEN f.year REGEXP '^[0-9]+$' THEN CAST(f.year AS UNSIGNED)
        ELSE NULL
    END AS fi_year,
    -- Validate quarter is numeric
    CASE 
        WHEN f.quarter REGEXP '^[0-9]+$' THEN CAST(f.quarter AS UNSIGNED)
        ELSE NULL
    END AS fi_qtr,
    STR_TO_DATE(f.qtrstartdate, '%Y%m%d') AS fi_qtr_start_date,
    -- Handle numeric conversions with validation
    CASE 
        WHEN f.revenue REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.revenue AS DECIMAL(18, 2))
        ELSE 0.00
    END AS fi_revenue,
    CASE 
        WHEN f.earnings REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.earnings AS DECIMAL(18, 2))
        ELSE 0.00
    END AS fi_net_earn,
    CASE 
        WHEN f.eps REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.eps AS DECIMAL(10, 2))
        ELSE 0.00
    END AS fi_basic_eps,
    CASE 
        WHEN f.dilutedeps REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.dilutedeps AS DECIMAL(10, 2))
        ELSE 0.00
    END AS fi_dilut_eps,
    CASE 
        WHEN f.margin REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.margin AS DECIMAL(10, 2))
        ELSE 0.00
    END AS fi_margin,
    CASE 
        WHEN f.inventory REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.inventory AS DECIMAL(18, 2))
        ELSE 0.00
    END AS fi_inventory,
    CASE 
        WHEN f.assets REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.assets AS DECIMAL(18, 2))
        ELSE 0.00
    END AS fi_assets,
    CASE 
        WHEN f.liability REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.liability AS DECIMAL(18, 2))
        ELSE 0.00
    END AS fi_liability,
    CASE 
        WHEN f.shout REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.shout AS DECIMAL(18, 2))
        ELSE 0.00
    END AS fi_out_basic,
    CASE 
        WHEN f.dilutedshout REGEXP '^-?[0-9]+\.?[0-9]*$' THEN CAST(f.dilutedshout AS DECIMAL(18, 2))
        ELSE 0.00
    END AS fi_out_dilut
FROM staging.finwire_fin f
INNER JOIN master.dimcompany c
    ON (TRIM(LEADING '0' FROM f.conameorcik) = CAST(c.companyid AS CHAR))
    OR (f.conameorcik = c.name)
WHERE 
    STR_TO_DATE(LEFT(f.pts, 8), '%Y%m%d') >= c.effectivedate
    AND STR_TO_DATE(LEFT(f.pts, 8), '%Y%m%d') < c.enddate
    -- Additional validation to ensure required fields are numeric
    AND f.year REGEXP '^[0-9]+$'
    AND f.quarter REGEXP '^[0-9]+$';