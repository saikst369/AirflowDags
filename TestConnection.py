from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyodbc

def test_query_sql_server():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'SERVER=test41-eus2-ins-sqlepi-foundation.database.windows.net;'
        'DATABASE=ItemLookupServiceTestDB;'
        'UID=test41_sql_foundation_tdm_user;'
        'PWD=FDsuz8lAsnTY='
    )
def query_sql_server():
    prod_conn_str = (
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'SERVER=prod-ins-sqlfg-foundation-listener.secondary.database.windows.net;'
        'DATABASE=ItemLookupServiceProdDB;'
        'UID=prod11_sql_foundation_generic_ro_user;'
        'PWD=Prod@F11!$*O'
    )
    dev_conn_str = (
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'SERVER=dev81-eus2-ins-sqlepi-foundation.database.windows.net;'
        'DATABASE=ItemLookupServiceDevDB;'
        'UID=dev81_sql_foundation_tdm_user;'
        'PWD=GksuzZW6lAsnHk='
    )
    prod_conn = pyodbc.connect(prod_conn_str)
    dev_conn = pyodbc.connect(dev_conn_str)

    prod_cursor = prod_conn.cursor()
    dev_cursor = dev_conn.cursor()
    #cursor = prod_conn_str.cursor()
    print(f"Prod Cursor output: {prod_cursor}")
    print(f"Dev Cursor output: {dev_cursor}")
    
    dev_cursor.execute("SELECT TOP 1 * FROM ITEM_STORE_INFO")
    result = dev_cursor.fetchall()
    print(f"Result: {result}")
    for row in result:
        print(f"Output of DB : {row}")
    prod_conn.close()
    dev_conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Test_sql_server_Connection',
    default_args=default_args,
    description='A simple DAG to connect to SQL Server using pyodbc',
    schedule_interval=None,  # Set to None to run only once
    catchup=False,  # Ensure the DAG does not backfill
)

run_query = PythonOperator(
    task_id='run_query',
    python_callable=query_sql_server,
    dag=dag,
)

run_query
