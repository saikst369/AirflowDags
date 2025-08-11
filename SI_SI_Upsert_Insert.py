import os
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def upsert_and_insert_data():
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

    print("Disabling Foreign key")
    # Disable foreign key constraint
    dev_cursor.execute("ALTER TABLE dbo.ITEM_STORE_INFO NOCHECK CONSTRAINT FK_ITEM_STORE_INFO_STORE_ITEM")
    print("Enable idnetity")
    # Enable IDENTITY_INSERT for STORE_ITEM
    dev_cursor.execute("SET IDENTITY_INSERT dbo.STORE_ITEM ON")
    print("Delete")
    # Delete existing data in development ITEM_STORE_INFO
    dev_cursor.execute("DELETE FROM dbo.ITEM_STORE_INFO WHERE STORE_ITEM_ID IN (SELECT STORE_ITEM_ID FROM dbo.STORE_ITEM WHERE DIVISION_NO='014' AND STORE_NO='00142')")
    print("Deleted existing data in development ITEM_STORE_INFO")
    # Delete existing data in development STORE_ITEM
    dev_cursor.execute("DELETE FROM dbo.STORE_ITEM WHERE DIVISION_NO='014' AND STORE_NO='00142'")
    print("Deleted existing data in development STORE_ITEM")

    # Fetch data from production STORE_ITEM in bulk
    prod_cursor.execute("SELECT * FROM dbo.STORE_ITEM WHERE DIVISION_NO='014' AND STORE_NO='00142' ")
    print("Fetch data from production STORE_ITEM in bulk")
    store_item_rows = prod_cursor.fetchall()
    print(f"Store item row: {store_item_rows}")

    # Insert data into development STORE_ITEM
    for row in store_item_rows:
        print(f" Row: {row}")
        dev_cursor.execute("""
            MERGE INTO dbo.STORE_ITEM AS target
            USING (SELECT ? AS STORE_ITEM_ID, ? AS DIVISION_NO, ? AS STORE_NO, ? AS UPC_NO, 
                          ? AS BASE_UPC_FLG, ? AS DIVISION_COMMODITY_CD, ? AS SKU_NO, 
                          ? AS ORDER_ITEM_ID, ? AS ORDER_ITEM_TYPE_CD, ? AS ETL_ROW_CREATE_TS, 
                          ? AS ETL_ROW_UPDATE_TS) AS source
            ON (target.STORE_ITEM_ID = source.STORE_ITEM_ID)
            WHEN MATCHED THEN
                UPDATE SET DIVISION_NO = source.DIVISION_NO, STORE_NO = source.STORE_NO, 
                           UPC_NO = source.UPC_NO, BASE_UPC_FLG = source.BASE_UPC_FLG, 
                           DIVISION_COMMODITY_CD = source.DIVISION_COMMODITY_CD, SKU_NO = source.SKU_NO, 
                           ORDER_ITEM_ID = source.ORDER_ITEM_ID, ORDER_ITEM_TYPE_CD = source.ORDER_ITEM_TYPE_CD, 
                           ETL_ROW_CREATE_TS = source.ETL_ROW_CREATE_TS, ETL_ROW_UPDATE_TS = source.ETL_ROW_UPDATE_TS
            WHEN NOT MATCHED THEN
                INSERT (STORE_ITEM_ID, DIVISION_NO, STORE_NO, UPC_NO, BASE_UPC_FLG, DIVISION_COMMODITY_CD, SKU_NO, 
                        ORDER_ITEM_ID, ORDER_ITEM_TYPE_CD, ETL_ROW_CREATE_TS, ETL_ROW_UPDATE_TS) 
                VALUES (source.STORE_ITEM_ID, source.DIVISION_NO, source.STORE_NO, source.UPC_NO, source.BASE_UPC_FLG, 
                        source.DIVISION_COMMODITY_CD, source.SKU_NO, source.ORDER_ITEM_ID, source.ORDER_ITEM_TYPE_CD, 
                        source.ETL_ROW_CREATE_TS, source.ETL_ROW_UPDATE_TS);
        """, row)

    # Disable IDENTITY_INSERT for STORE_ITEM
    dev_cursor.execute("SET IDENTITY_INSERT dbo.STORE_ITEM OFF")
    print(f"Disable IDENTITY_INSERT for STORE_ITEM")

    # Enable IDENTITY_INSERT for ITEM_STORE_INFO
    dev_cursor.execute("SET IDENTITY_INSERT dbo.ITEM_STORE_INFO ON")
    print(f"Enable IDENTITY_INSERT for ITEM_STORE_INFO")

    # Fetch data from production ITEM_STORE_INFO for the fetched STORE_ITEM_IDs in bulk
    store_item_ids = [row.STORE_ITEM_ID for row in store_item_rows]
    query = "SELECT * FROM dbo.ITEM_STORE_INFO WHERE STORE_ITEM_ID IN ({})".format(','.join('?' * len(store_item_ids)))
    prod_cursor.execute(query, store_item_ids)
    item_store_info_rows = prod_cursor.fetchall()
    
    print(f"items store infor rows")
    # Insert data into development ITEM_STORE_INFO
    for row in item_store_info_rows:
        dev_cursor.execute("""
            INSERT INTO dbo.ITEM_STORE_INFO (ITEM_STORE_INFO_ID, STORE_ITEM_ID, ALLOCATIONS_CNT, L1L2_NO, CASE_QTY, 
                                             CATALOG_ID, CATALOG_NME, SATH_AUTHORIZATION_FLG, DSD_VENDOR_TXT, DATA_LOAD_TS, 
                                             ORDER_ENTRY_NO, KIOSK_MINIMUM_QTY, SHELF_MINIMUM_QTY, SATH_INFO_VLU, SRC_ID, 
                                             ITEM_DESCRIPTION_TXT, LAST_MOVEMENT_DT, STATUS_CD, CAT_ID, ETL_ROW_CREATE_TS, 
                                             ETL_ROW_UPDATE_TS)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

    # Disable IDENTITY_INSERT for ITEM_STORE_INFO
    dev_cursor.execute("SET IDENTITY_INSERT dbo.ITEM_STORE_INFO OFF")
    print("Disable IDENTITY_INSERT for ITEM_STORE_INFOd")
    # Enable foreign key constraint
    #dev_cursor.execute("ALTER TABLE dbo.ITEM_STORE_INFO WITH CHECK CHECK CONSTRAINT FK_ITEM_STORE_INFO_STORE_ITEM") ## issue
    print("Enable foreign key constraintd")
    dev_conn.commit()
    print("Commit doned")

    prod_conn.close()
    dev_conn.close()
    print("Connection closed")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('ISI_SI_Upsert_Insert',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:


    upsert_and_insert_task = PythonOperator(
        task_id='upsert_and_insert_data',
        python_callable=upsert_and_insert_data
    )

    upsert_and_insert_task
