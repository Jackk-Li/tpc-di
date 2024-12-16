/* Script for Creating Staging Schema in MariaDB */
DROP SCHEMA IF EXISTS staging;
CREATE SCHEMA staging;

USE staging;

/* Table: batchdate */
DROP TABLE IF EXISTS batchdate;
CREATE TABLE batchdate (
    batchdate DATE NOT NULL
);

/* Table: cashtransaction */
DROP TABLE IF EXISTS cashtransaction;
CREATE TABLE cashtransaction (
    ct_ca_id NUMERIC(11) NOT NULL,
    ct_dts TIMESTAMP NOT NULL,
    ct_amt NUMERIC(10, 2) NOT NULL,
    ct_name CHAR(100) NOT NULL
);

/* Table: customermgmt */
DROP TABLE IF EXISTS customermgmt;
CREATE TABLE customermgmt (
    actiontype CHAR(9) NOT NULL,
    actionts VARCHAR(255) NOT NULL,
    c_id NUMERIC(11) NOT NULL,
    c_tax_id CHAR(20),
    c_gndr CHAR(1),
    c_tier NUMERIC(1),
    c_dob DATE,
    c_l_name CHAR(25),
    c_f_name CHAR(20),
    c_m_name CHAR(1),
    c_adline1 CHAR(80),
    c_adline2 CHAR(80),
    c_zipcode CHAR(12),
    c_city CHAR(25),
    c_state_prov CHAR(20),
    c_ctry CHAR(24),
    c_prim_email CHAR(50),
    c_alt_email CHAR(50),
    c_p_1_ctry_code CHAR(20),
    c_p_1_area_code CHAR(20),
    c_p_1_local CHAR(20),
    c_p_1_ext CHAR(20),
    c_p_2_ctry_code CHAR(20),
    c_p_2_area_code CHAR(20),
    c_p_2_local CHAR(20),
    c_p_2_ext CHAR(20),
    c_p_3_ctry_code CHAR(20),
    c_p_3_area_code CHAR(20),
    c_p_3_local CHAR(20),
    c_p_3_ext CHAR(20),
    c_lcl_tx_id CHAR(4),
    c_nat_tx_id CHAR(4),
    ca_id NUMERIC(11),
    ca_tax_st NUMERIC(1),
    ca_b_id NUMERIC(11),
    ca_name CHAR(50)
);

/* Table: dailymarket */
DROP TABLE IF EXISTS dailymarket;
CREATE TABLE dailymarket (
    dm_date DATE NOT NULL,
    dm_s_symb CHAR(15) NOT NULL,
    dm_close NUMERIC(8, 2) NOT NULL,
    dm_high NUMERIC(8, 2) NOT NULL,
    dm_low NUMERIC(8, 2) NOT NULL,
    dm_vol NUMERIC(12) NOT NULL
);

/* Table: date */
DROP TABLE IF EXISTS date;
CREATE TABLE date (
    sk_dateid NUMERIC(11) NOT NULL,
    datevalue CHAR(20) NOT NULL,
    datedesc CHAR(20) NOT NULL,
    calendaryearid NUMERIC(4) NOT NULL,
    calendaryeardesc CHAR(20) NOT NULL,
    calendarqtrid NUMERIC(5) NOT NULL,
    calendarqtrdesc CHAR(20) NOT NULL,
    calendarmonthid NUMERIC(6) NOT NULL,
    calendarmonthdesc CHAR(20) NOT NULL,
    calendarweekid NUMERIC(6) NOT NULL,
    calendarweekdesc CHAR(20) NOT NULL,
    dayofweeknum NUMERIC(1) NOT NULL,
    dayofweekdesc CHAR(10) NOT NULL,
    fiscalyearid NUMERIC(4) NOT NULL,
    fiscalyeardesc CHAR(20) NOT NULL,
    fiscalqtrid NUMERIC(5) NOT NULL,
    fiscalqtrdesc CHAR(20) NOT NULL,
    holidayflag BOOLEAN
);

/* Table: finwire */
DROP TABLE IF EXISTS finwire;
CREATE TABLE finwire (
    finwire text NOT NULL
);

/* Table: finwire_cmp */
DROP TABLE IF EXISTS finwire_cmp;
CREATE TABLE finwire_cmp (
    pts CHAR(15) NOT NULL,
    rectype CHAR(3) NOT NULL,
    companyname CHAR(60) NOT NULL,
    cik CHAR(10) NOT NULL,
    status CHAR(4) NOT NULL,
    industryid CHAR(2) NOT NULL,
    sprating CHAR(4) NOT NULL,
    foundingdate CHAR(8),
    addressline1 CHAR(80) NOT NULL,
    addressline2 CHAR(80),
    postalcode CHAR(12) NOT NULL,
    city CHAR(25) NOT NULL,
    stateprovince CHAR(20) NOT NULL,
    country CHAR(24),
    ceoname CHAR(46) NOT NULL,
    description CHAR(150) NOT NULL
);

/* Table: finwire_sec */
DROP TABLE IF EXISTS finwire_sec;
CREATE TABLE finwire_sec (
    pts CHAR(15) NOT NULL,
    rectype CHAR(3) NOT NULL,
    symbol CHAR(15) NOT NULL,
    issuetype CHAR(6) NOT NULL,
    status CHAR(4) NOT NULL,
    name CHAR(70) NOT NULL,
    exid CHAR(6) NOT NULL,
    shout CHAR(13) NOT NULL,
    firsttradedate CHAR(8) NOT NULL,
    firsttradeexchg CHAR(8) NOT NULL,
    dividend CHAR(12) NOT NULL,
    conameorcik CHAR(60) NOT NULL
);

/* Table: finwire_fin */
DROP TABLE IF EXISTS finwire_fin;
CREATE TABLE finwire_fin (
    pts CHAR(15) NOT NULL,
    rectype CHAR(3) NOT NULL,
    year CHAR(4) NOT NULL,
    quarter CHAR(1) NOT NULL,
    qtrstartdate CHAR(8) NOT NULL,
    postingdate CHAR(8) NOT NULL,
    revenue CHAR(17) NOT NULL,
    earnings CHAR(17) NOT NULL,
    eps CHAR(12) NOT NULL,
    dilutedeps CHAR(12) NOT NULL,
    margin CHAR(12) NOT NULL,
    inventory CHAR(17) NOT NULL,
    assets CHAR(17) NOT NULL,
    liability CHAR(17) NOT NULL,
    shout CHAR(13) NOT NULL,
    dilutedshout CHAR(13) NOT NULL,
    conameorcik CHAR(60) NOT NULL
);

/* Table: holdinghistory */
DROP TABLE IF EXISTS holdinghistory;
CREATE TABLE holdinghistory (
    hh_h_t_id NUMERIC(15) NOT NULL,
    hh_t_id NUMERIC(15) NOT NULL,
    hh_before_qty NUMERIC(6) NOT NULL,
    hh_after_qty NUMERIC(6) NOT NULL
);

/* Table: hr */
DROP TABLE IF EXISTS hr;
CREATE TABLE hr (
    employeeid NUMERIC(11) NOT NULL,
    managerid NUMERIC(11) NOT NULL,
    employeefirstname CHAR(30) NOT NULL,
    employeelastname CHAR(30) NOT NULL,
    employeemi CHAR(1),
    employeejobcode NUMERIC(3),
    employeebranch CHAR(30),
    employeeoffice CHAR(10),
    employeephone CHAR(14)
);

/* Table: industry */
DROP TABLE IF EXISTS industry;
CREATE TABLE industry (
    in_id CHAR(2) NOT NULL,
    in_name CHAR(50) NOT NULL,
    in_sc_id CHAR(4) NOT NULL
);

/* Table: prospect */
DROP TABLE IF EXISTS prospect;
CREATE TABLE prospect (
    agencyid CHAR(30) NOT NULL,
    lastname CHAR(30) NOT NULL,
    firstname CHAR(30) NOT NULL,
    middleinitial CHAR(1),
    gender CHAR(1),
    addressline1 CHAR(80),
    addressline2 CHAR(80),
    postalcode CHAR(12),
    city CHAR(25) NOT NULL,
    state CHAR(20) NOT NULL,
    country CHAR(24),
    phone CHAR(30),
    income NUMERIC(9),
    numbercars NUMERIC(2),
    numberchildren NUMERIC(2),
    maritalstatus CHAR(1),
    age NUMERIC(3),
    creditrating NUMERIC(4),
    ownorrentflag CHAR(1),
    employer CHAR(30),
    numbercreditcards NUMERIC(2),
    networth NUMERIC(12)
);

/* Table: statustype */
DROP TABLE IF EXISTS statustype;
CREATE TABLE statustype (
    st_id CHAR(4) NOT NULL,
    st_name CHAR(10) NOT NULL
);

/* Table: taxrate */
DROP TABLE IF EXISTS taxrate;
CREATE TABLE taxrate (
    tx_id CHAR(4) NOT NULL,
    tx_name CHAR(50) NOT NULL,
    tx_rate NUMERIC(6, 5) NOT NULL
);

/* Table: time */
DROP TABLE IF EXISTS time;
CREATE TABLE time (
    sk_timeid NUMERIC(11) NOT NULL,
    timevalue CHAR(20) NOT NULL,
    hourid NUMERIC(2) NOT NULL,
    hourdesc CHAR(20) NOT NULL,
    minuteid NUMERIC(2) NOT NULL,
    minutedesc CHAR(20) NOT NULL,
    secondid NUMERIC(2) NOT NULL,
    seconddesc CHAR(20) NOT NULL,
    markethoursflag BOOLEAN,
    officehoursflag BOOLEAN
);

/* Table: tradehistory */
DROP TABLE IF EXISTS tradehistory;
CREATE TABLE tradehistory (
    th_t_id NUMERIC(15) NOT NULL,
    th_dts TIMESTAMP NOT NULL,
    th_st_id CHAR(4) NOT NULL
);

/* Table: trade */
DROP TABLE IF EXISTS trade;
CREATE TABLE trade (
    t_id NUMERIC(15) NOT NULL,
    t_dts TIMESTAMP NOT NULL,
    t_st_id CHAR(4) NOT NULL,
    t_tt_id CHAR(3) NOT NULL,
    t_is_cash INTEGER,
    t_s_symb CHAR(15) NOT NULL,
    t_qty NUMERIC(6),
    t_bid_price NUMERIC(8, 2),
    t_ca_id NUMERIC(11) NOT NULL,
    t_exec_name CHAR(49) NOT NULL,
    t_trade_price NUMERIC(8, 2),
    t_chrg NUMERIC(10, 2),
    t_comm NUMERIC(10, 2),
    t_tax NUMERIC(10, 2)
);

/* Table: tradetype */
DROP TABLE IF EXISTS tradetype;
CREATE TABLE tradetype (
    tt_id CHAR(3) NOT NULL,
    tt_name CHAR(12) NOT NULL,
    tt_is_sell NUMERIC(1) NOT NULL,
    tt_is_mrkt NUMERIC(1) NOT NULL
);

/* Table: watchhistory */
DROP TABLE IF EXISTS watchhistory;
CREATE TABLE watchhistory (
    w_c_id NUMERIC(11) NOT NULL,
    w_s_symb CHAR(15) NOT NULL,
    w_dts TIMESTAMP NOT NULL,
    w_action CHAR(4) NOT NULL
);

/* Table: audit */
DROP TABLE IF EXISTS audit;
CREATE TABLE audit (
    dataset CHAR(20) NOT NULL,
    batchid NUMERIC(5),
    date DATE,
    attribute CHAR(50) NOT NULL,
    value NUMERIC(15),
    dvalue NUMERIC(15, 5)
);
