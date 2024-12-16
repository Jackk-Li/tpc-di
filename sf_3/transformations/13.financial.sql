-- Truncate the financial table
TRUNCATE TABLE master.financial;

-- Insert transformed data into financial
INSERT INTO master.financial 
SELECT 
    c.sk_companyid AS sk_companyid,
    CAST(f.year AS UNSIGNED) AS fi_year,
    CAST(f.quarter AS UNSIGNED) AS fi_qtr,
    DATE(f.qtrstartdate) AS fi_qtr_start_date,
    CAST(f.revenue AS DECIMAL(18, 2)) AS fi_revenue,
    CAST(f.earnings AS DECIMAL(18, 2)) AS fi_net_earn,
    CAST(f.eps AS DECIMAL(10, 2)) AS fi_basic_eps,
    CAST(f.dilutedeps AS DECIMAL(10, 2)) AS fi_dilut_eps,
    CAST(f.margin AS DECIMAL(10, 2)) AS fi_margin,
    CAST(f.inventory AS DECIMAL(18, 2)) AS fi_inventory,
    CAST(f.assets AS DECIMAL(18, 2)) AS fi_assets,
    CAST(f.liability AS DECIMAL(18, 2)) AS fi_liability,
    CAST(f.shout AS DECIMAL(18, 2)) AS fi_out_basic,
    CAST(f.dilutedshout AS DECIMAL(18, 2)) AS fi_out_dilut
FROM staging.finwire_fin f
INNER JOIN master.dimcompany c
    ON (f.conameorcik = c.companyid)
    OR (f.conameorcik = c.name)
WHERE DATE(STR_TO_DATE(LEFT(f.pts, 8), '%Y%m%d')) >= c.effectivedate
  AND DATE(STR_TO_DATE(LEFT(f.pts, 8), '%Y%m%d')) < c.enddate;
