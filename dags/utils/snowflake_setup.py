import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from airflow.hooks.base import BaseHook
import logging

logger = logging.getLogger(__name__)
if __name__ == "__main__":
      logging.basicConfig(level=logging.INFO)

def snowflake_connection():
    try:
        connector = BaseHook.get_connection("snowflake_conn")
        # Access attributes
        logger.info(f"Login: {connector.login}")
        logger.info(f"Host: {connector.host}")
        conn = snowflake.connector.connect(
            user = connector.login,
            password = connector.password,
            account = connector.host
        )
        return conn
    except Exception as e:
        logger.error(f"Error in connection: {e}")
        return None

def create_wh():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("USE ROLE ACCOUNTADMIN;")
        sql = '''
        CREATE WAREHOUSE IF NOT EXISTS STOCK_WH;
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logger.info(" Warehouse STOCK_WH created")
    except Exception as e:
        logger.error(f"Error in creating warehouse: {e}")
        return None

def create_db():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        sql = '''
        CREATE DATABASE IF NOT EXISTS STOCKS_DB;
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logger.info("Database STOCKS_DB created")
    except Exception as e:
        logger.error(f"Error in creating database: {e}")
        return None

def create_schema():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("USE DATABASE STOCKS_DB;")
        sql = '''
        CREATE SCHEMA IF NOT EXISTS STOCKS;
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logger.info("Schema STOCKS created")
    except Exception as e:
        logger.error(f"Error in creating schema: {e}")
        return None
    
def create_table():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("USE ROLE ACCOUNTADMIN;")
        cursor.execute("USE DATABASE STOCKS_DB;")
        cursor.execute("USE SCHEMA STOCKS;")
        sql = '''
        CREATE OR ALTER TABLE PRICE (
            DATETIME TIMESTAMP_TZ,
            TICKER TEXT,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME FLOAT,
            CONSTRAINT PK_PRICE PRIMARY KEY (DATETIME, TICKER)
        )
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logger.info("Table PRICE created")
    except Exception as e:
        logger.error(f"Error in creating table: {e}")
        return None
    

    
def create_role():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        sql = '''
        USE ROLE ACCOUNTADMIN;
        CREATE ROLE IF NOT EXISTS STOCK_ANALYST;
        GRANT USAGE ON WAREHOUSE STOCK_WH TO ROLE STOCK_ANALYST;
        GRANT USAGE ON DATABASE STOCKS_DB TO ROLE STOCK_ANALYST;
        GRANT USAGE ON SCHEMA STOCKS_DB.STOCKS TO ROLE STOCK_ANALYST;
        GRANT ROLE STOCK_ANALYST TO USER {};
        '''.format(os.getenv("SNOWFLAKE_USER"))
        cursor.execute(sql, num_statements=6)
        conn.commit()
        conn.close()
        logger.info("Role STOCK_ANALYST created and granted privileges")
    except Exception as e:
        logger.error(f"Error in creating role: {e}")
        return None

def clear_table():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("USE DATABASE STOCKS;")
        sql = '''
        DELETE FROM STOCKS.SILVER.PRICE
        '''
        cursor.execute(sql)
        conn.commit()
        logger.info("Table cleared")
        conn.close()
    except Exception as e:
        logger.error(f"Error in deleting PRICE table: {e}")
        return None

# def write_table(df, overwrite=True):
#     conn = snowflake_connection()
#     try:
#         success, n_chunks, n_rows, _ = write_pandas(
#             conn,
#             df,
#             table_name = "JOBS",
#             database = "PERSONAL",
#             schema = "JOB_TRACKING",
#             auto_create_table = False,
#             overwrite = overwrite
#         )

#         print(f"Table updated: {success}")
#         print(f"Total Rows: {n_rows}")
#     except Exception as e:
#         print(f"Error in writing in JOBS table: {e}")
#         return None
    

