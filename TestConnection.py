from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def query_sql_server():
    # Use Airflow connection: create 'mssql_default' in Admin > Connections
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    rows = hook.get_records("SELECT TOP 1 * FROM ITEM_STORE_INFO")
    print(f"Result: {rows}")
    for row in rows:
        print(f"Output of DB : {row}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Test_sql_server_Connection',
    default_args=default_args,
    description='A simple DAG to connect to SQL Server using pyodbc',
    schedule=None,  # Set to None to run only once
    catchup=False,  # Ensure the DAG does not backfill
)

run_query = PythonOperator(
    task_id='run_query',
    python_callable=query_sql_server,
    dag=dag,
)

run_query
