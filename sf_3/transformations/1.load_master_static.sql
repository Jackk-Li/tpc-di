-- tradetype
TRUNCATE TABLE master.tradetype;
INSERT INTO master.tradetype
SELECT * FROM staging.tradetype;

-- statustype
TRUNCATE TABLE master.statustype;
INSERT INTO master.statustype
SELECT * FROM staging.statustype;

-- taxrate
TRUNCATE TABLE master.taxrate;
INSERT INTO master.taxrate
SELECT * FROM staging.taxrate;

-- industry
TRUNCATE TABLE master.industry;
INSERT INTO master.industry
SELECT * FROM staging.industry;

-- dimdate
TRUNCATE TABLE master.dimdate;
INSERT INTO master.dimdate
SELECT 
    sk_dateid,
    CAST(datevalue AS DATE) AS datevalue,
    datedesc,
    calendaryearid,
    calendaryeardesc,
    calendarqtrid,
    calendarqtrdesc,
    calendarmonthid,
    calendarmonthdesc,
    calendarweekid,
    calendarweekdesc,
    dayofweeknum,
    dayofweekdesc,
    fiscalyearid,
    fiscalyeardesc,
    fiscalqtrid,
    fiscalqtrdesc,
    holidayflag
FROM staging.date;

-- dimtime
TRUNCATE TABLE master.dimtime;
INSERT INTO master.dimtime
SELECT 
    sk_timeid,
    CAST(timevalue AS TIME) AS timevalue,
    hourid,
    hourdesc,
    minuteid,
    minutedesc,
    secondid,
    seconddesc,
    markethoursflag,
    officehoursflag
FROM staging.time;
