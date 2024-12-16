# filepath: /e:/Data apps/Airflow/dags/sf_3/full_historical_load_etl_dag.py

import sys
import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from customermgmt_conversion import connect_mariadb, customermgmt_convert, load_data_to_mariadb

dag_folder = os.path.dirname(os.path.abspath(__file__))
if dag_folder not in sys.path:
    sys.path.append(dag_folder)

# database connection config
DB_CONFIG = {
    'host': 'host.docker.internal',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'staging'
}

# Default arguments for dag
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 14)
}

# Create dag
tpcdi_dag = DAG(
    dag_id="tpcdi_historical_load",
    default_args=default_args,
    schedule_interval=None,
)

# Task 1 - Create staging schema
create_staging_schema = MySqlOperator(
    task_id="create_staging_schema",
    mysql_conn_id="mariadb_tpcdi",
    sql="create_staging_schema.sql",
    dag=tpcdi_dag
)

# Task 2 - Load text and csv source files to staging
load_txt_csv_to_staging = MySqlOperator(
    task_id="load_txt_csv_to_staging",
    mysql_conn_id="mariadb_tpcdi",
    sql="populate_staging.sql",
    split_statements=True,
    autocommit=True,
    dag=tpcdi_dag
)

# Task 3 - Load FINWIRE to staging
load_finwire_to_staging = MySqlOperator(
    task_id="load_finwire_to_staging", 
    mysql_conn_id="mariadb_tpcdi",
    sql="populate_staging_finwire.sql",
    split_statements=True,
    autocommit=True,
    dag=tpcdi_dag
)

# Task 4 - Parse FINWIRE
parse_finwire = MySqlOperator(
    task_id="parse_finwire",
    mysql_conn_id="mariadb_tpcdi", 
    sql="load_staging_separate_finware.sql",
    dag=tpcdi_dag
)

# Define the main function that will be called by Airflow
def process_customermgmt():
    cur, connection = connect_mariadb(DB_CONFIG)
    if cur is not None and connection is not None:
        try:
            csv_file_path = customermgmt_convert()
            load_data_to_mariadb(cur, csv_file_path)
        finally:
            cur.close()
            connection.close()

# Task 5 - Convert customermgmt data and load to staging
convert_load_customermgmt = PythonOperator(
    task_id="convert_load_customermgmt",
    python_callable=process_customermgmt,
    dag=tpcdi_dag
)

# Task 6 - Create master schema
create_master_schema = MySqlOperator(
    task_id="create_master_schema",
    mysql_conn_id="mariadb_tpcdi",
    sql="create_master_schema.sql",
    dag=tpcdi_dag
)

# Task 7 - Load static master tables
load_master_static = MySqlOperator(
    task_id="load_master_static",
    mysql_conn_id="mariadb_tpcdi",
    sql="1.load_master_static.sql",
    dag=tpcdi_dag
)

# Task 8 - Transform and load DimCompany
transform_load_dimcompany = MySqlOperator(
    task_id="transform_load_dimcompany",
    mysql_conn_id="mariadb_tpcdi",
    sql="2.DimCompany.sql",
    dag=tpcdi_dag
)

# Task 9 - Load DimMessages for DimCompany
load_dimessages_dimcompany = MySqlOperator(
    task_id="load_dimessages_dimcompany",
    mysql_conn_id="mariadb_tpcdi",
    sql="3.dimessages_dimcompany.sql",
    dag=tpcdi_dag
)

# Task 10 - Transform and load DimBroker
transform_load_dimbroker = MySqlOperator(
    task_id="transform_load_dimbroker",
    mysql_conn_id="mariadb_tpcdi",
    sql="4.DimBroker.sql",
    dag=tpcdi_dag
)

# Task 11 - Transform and load Prospect
transform_load_prospect = MySqlOperator(
    task_id="transform_load_prospect",
    mysql_conn_id="mariadb_tpcdi",
    sql="5.prospect.sql",
    dag=tpcdi_dag
)

# Task 12 - Transform and load DimCustomer
transform_load_dimcustomer = MySqlOperator(
    task_id="transform_load_dimcustomer",
    mysql_conn_id="mariadb_tpcdi",
    sql="6.DimCustomer.sql",
    dag=tpcdi_dag
)

# Task 13 - Load DimMessages for DimCustomer
load_dimessages_dimcustomer = MySqlOperator(
    task_id="load_dimessages_dimcustomer",
    mysql_conn_id="mariadb_tpcdi",
    sql="7.dimessages_dimcustomer.sql",
    dag=tpcdi_dag
)

# Task 14 - Update Prospect
update_prospect = MySqlOperator(
    task_id="update_prospect",
    mysql_conn_id="mariadb_tpcdi",
    sql="8.prospect_update.sql",
    dag=tpcdi_dag
)

# Task 15 - Transform and load DimAccount
transform_load_dimaccount = MySqlOperator(
    task_id="transform_load_dimaccount",
    mysql_conn_id="mariadb_tpcdi",
    sql="9.DimAccount.sql",
    dag=tpcdi_dag
)

# Task 16 - Transform and load DimSecurity
transform_load_dimsecurity = MySqlOperator(
    task_id="transform_load_dimsecurity",
    mysql_conn_id="mariadb_tpcdi",
    sql="10.DimSecurity.sql",
    dag=tpcdi_dag
)

# Task 17 - Transform and load DimTrade
transform_load_dimtrade = MySqlOperator(
    task_id="transform_load_dimtrade",
    mysql_conn_id="mariadb_tpcdi",
    sql="11.DimTrade.sql",
    dag=tpcdi_dag
)

# Task 18 - Load DimMessages for DimTrade
load_dimessages_dimtrade = MySqlOperator(
    task_id="load_dimessages_dimtrade",
    mysql_conn_id="mariadb_tpcdi",
    sql="12.dimessages_dimtrade.sql",
    dag=tpcdi_dag
)

# Task 19 - Transform and load Financial
transform_load_financial = MySqlOperator(
    task_id="transform_load_financial",
    mysql_conn_id="mariadb_tpcdi",
    sql="13.financial.sql",
    dag=tpcdi_dag
)

# Task 20 - Transform and load FactCashBalance
transform_load_factcashbalance = MySqlOperator(
    task_id="transform_load_factcashbalance",
    mysql_conn_id="mariadb_tpcdi",
    sql="14.factcashbalance.sql",
    dag=tpcdi_dag
)

# Task 21 - Transform and load FactHoldings
transform_load_factholdings = MySqlOperator(
    task_id="transform_load_factholdings",
    mysql_conn_id="mariadb_tpcdi",
    sql="15.factholdings.sql",
    dag=tpcdi_dag
)

# Task 22 - Transform and load FactWatches
transform_load_factwatches = MySqlOperator(
    task_id="transform_load_factwatches",
    mysql_conn_id="mariadb_tpcdi",
    sql="16.factwatches.sql",
    dag=tpcdi_dag
)

# Task 23 - Transform and load FactMarketHistory
transform_load_factmarkethistory = MySqlOperator(
    task_id="transform_load_factmarkethistory",
    mysql_conn_id="mariadb_tpcdi",
    sql="17.factmarkethistory.sql",
    dag=tpcdi_dag
)

# Task 24 - Load DimMessages for FactMarketHistory
load_dimessages_factmarkethistory = MySqlOperator(
    task_id="load_dimessages_factmarkethistory",
    mysql_conn_id="mariadb_tpcdi",
    sql="18.dimessages_factmakethistory.sql",
    dag=tpcdi_dag
)

# Set up task dependencies
# Staging schema dependency
create_staging_schema >> load_txt_csv_to_staging
create_staging_schema >> load_finwire_to_staging >> parse_finwire
create_staging_schema >> convert_load_customermgmt

# Master schema dependency
load_txt_csv_to_staging >> create_master_schema
parse_finwire >> create_master_schema
convert_load_customermgmt >> create_master_schema

# Static tables loading
create_master_schema >> load_master_static

# Company dimension and its messages
create_master_schema >> transform_load_dimcompany >> load_dimessages_dimcompany

# Broker and Prospect loading
load_master_static >> transform_load_dimbroker
load_master_static >> transform_load_prospect

# Customer dimension and related tasks
transform_load_prospect >> transform_load_dimcustomer
load_master_static >> transform_load_dimcustomer
transform_load_dimcustomer >> load_dimessages_dimcustomer
transform_load_dimcustomer >> update_prospect

# Account dimension
transform_load_dimbroker >> transform_load_dimaccount 
transform_load_dimcustomer >> transform_load_dimaccount

# Security dimension
transform_load_dimcompany >> transform_load_dimsecurity

# Trade dimension and its messages
transform_load_dimaccount >> transform_load_dimtrade 
transform_load_dimsecurity >> transform_load_dimtrade 
load_master_static >> transform_load_dimtrade
transform_load_dimtrade >> load_dimessages_dimtrade

# Financial facts
transform_load_dimcompany >> transform_load_financial

# Cash balances fact
transform_load_dimaccount >> transform_load_factcashbalance 
load_master_static >> transform_load_factcashbalance

# Holdings fact
transform_load_dimtrade >> transform_load_factholdings

# Watches fact
transform_load_dimcustomer >> transform_load_factwatches 
transform_load_dimsecurity >> transform_load_factwatches
load_master_static >> transform_load_factwatches

# Market history fact and its messages
transform_load_financial >> transform_load_factmarkethistory
transform_load_dimcompany >> transform_load_factmarkethistory
transform_load_dimsecurity >> transform_load_factmarkethistory 
load_master_static >> transform_load_factmarkethistory

# Load messages for Market history fact
transform_load_factmarkethistory >> load_dimessages_factmarkethistory