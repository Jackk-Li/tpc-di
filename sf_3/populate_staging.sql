LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/BatchDate.txt'
INTO TABLE staging.batchdate
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/CashTransaction.txt'
INTO TABLE staging.cashtransaction
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/DailyMarket.txt'
INTO TABLE staging.dailymarket
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/Date.txt'
INTO TABLE staging.date
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/HoldingHistory.txt'
INTO TABLE staging.holdinghistory
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/HR.csv'
INTO TABLE staging.hr
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/Industry.txt'
INTO TABLE staging.industry
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/Prospect.csv'
INTO TABLE staging.prospect
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/StatusType.txt'
INTO TABLE staging.statustype
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/TaxRate.txt'
INTO TABLE staging.taxrate
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/Time.txt'
INTO TABLE staging.time
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/TradeHistory.txt'
INTO TABLE staging.tradehistory
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/Trade.txt'
INTO TABLE staging.trade
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/TradeType.txt'
INTO TABLE staging.tradetype
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1/WatchHistory.txt'
INTO TABLE staging.watchhistory
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE '/opt/airflow/dags/sf_3/data/Batch1_audit.csv'
INTO TABLE staging.audit
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
