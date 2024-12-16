

    INSERT INTO staging.finwire_cmp (
        pts, rectype, companyname, cik, status, industryid, sprating,
        foundingdate, addressline1, addressline2, postalcode, city,
        stateprovince, country, ceoname, description
    )
    SELECT
        TRIM(SUBSTRING(finwire, 1, 15)) AS pts,
        TRIM(SUBSTRING(finwire, 16, 3)) AS rectype,
        TRIM(SUBSTRING(finwire, 19, 60)) AS companyname,
        TRIM(SUBSTRING(finwire, 79, 10)) AS cik,
        TRIM(SUBSTRING(finwire, 89, 4)) AS status,
        TRIM(SUBSTRING(finwire, 93, 2)) AS industryid,
        TRIM(SUBSTRING(finwire, 95, 4)) AS sprating,
        TRIM(SUBSTRING(finwire, 99, 8)) AS foundingdate,
        TRIM(SUBSTRING(finwire, 107, 80)) AS addressline1,
        TRIM(SUBSTRING(finwire, 187, 80)) AS addressline2,
        TRIM(SUBSTRING(finwire, 267, 12)) AS postalcode,
        TRIM(SUBSTRING(finwire, 279, 25)) AS city,
        TRIM(SUBSTRING(finwire, 304, 20)) AS stateprovince,
        TRIM(SUBSTRING(finwire, 324, 24)) AS country,
        TRIM(SUBSTRING(finwire, 348, 46)) AS ceoname,
        TRIM(SUBSTRING(finwire, 394, 150)) AS description
    FROM staging.finwire
    WHERE SUBSTRING(finwire, 16, 3) = 'CMP';


    INSERT INTO staging.finwire_sec (
        pts, rectype, symbol, issuetype, status, name, exid, shout,
        firsttradedate, firsttradeexchg, dividend, conameorcik
    )
    SELECT
        TRIM(SUBSTRING(finwire, 1, 15)) AS pts,
        TRIM(SUBSTRING(finwire, 16, 3)) AS rectype,
        TRIM(SUBSTRING(finwire, 19, 15)) AS symbol,
        TRIM(SUBSTRING(finwire, 34, 6)) AS issuetype,
        TRIM(SUBSTRING(finwire, 40, 4)) AS status,
        TRIM(SUBSTRING(finwire, 44, 70)) AS name,
        TRIM(SUBSTRING(finwire, 114, 6)) AS exid,
        TRIM(SUBSTRING(finwire, 120, 13)) AS shout,
        TRIM(SUBSTRING(finwire, 133, 8)) AS firsttradedate,
        TRIM(SUBSTRING(finwire, 141, 8)) AS firsttradeexchg,
        TRIM(SUBSTRING(finwire, 149, 12)) AS dividend,
        TRIM(SUBSTRING(finwire, 161, 60)) AS conameorcik
    FROM staging.finwire
    WHERE SUBSTRING(finwire, 16, 3) = 'SEC';


    INSERT INTO staging.finwire_fin (
        pts, rectype, year, quarter, qtrstartdate, postingdate,
        revenue, earnings, eps, dilutedeps, margin, inventory,
        assets, liability, shout, dilutedshout, conameorcik
    )
    SELECT
        TRIM(SUBSTRING(finwire, 1, 15)) AS pts,
        TRIM(SUBSTRING(finwire, 16, 3)) AS rectype,
        TRIM(SUBSTRING(finwire, 19, 4)) AS year,
        TRIM(SUBSTRING(finwire, 23, 1)) AS quarter,
        TRIM(SUBSTRING(finwire, 24, 8)) AS qtrstartdate,
        TRIM(SUBSTRING(finwire, 32, 8)) AS postingdate,
        TRIM(SUBSTRING(finwire, 40, 17)) AS revenue,
        TRIM(SUBSTRING(finwire, 57, 17)) AS earnings,
        TRIM(SUBSTRING(finwire, 74, 12)) AS eps,
        TRIM(SUBSTRING(finwire, 86, 12)) AS dilutedeps,
        TRIM(SUBSTRING(finwire, 98, 12)) AS margin,
        TRIM(SUBSTRING(finwire, 110, 17)) AS inventory,
        TRIM(SUBSTRING(finwire, 127, 17)) AS assets,
        TRIM(SUBSTRING(finwire, 144, 17)) AS liability,
        TRIM(SUBSTRING(finwire, 161, 13)) AS shout,
        TRIM(SUBSTRING(finwire, 174, 13)) AS dilutedshout,
        TRIM(SUBSTRING(finwire, 187, 60)) AS conameorcik
    FROM staging.finwire
    WHERE SUBSTRING(finwire, 16, 3) = 'FIN';

