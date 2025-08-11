from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Test_sql_server_Connection',
    default_args=default_args,
    description='A simple DAG to connect to SQL Server using MsSqlOperator',
    schedule=None,  # Set to None to run only once
    catchup=False,  # Ensure the DAG does not backfill
)

run_query = MsSqlOperator(
    task_id='run_query',
    mssql_conn_id='mssql_default',
    sql='SELECT TOP 1 * FROM ITEM_STORE_INFO',
    dag=dag,
)

run_query
