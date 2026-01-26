import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

load_dotenv()

def snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user = os.getenv("SNOWFLAKE_USER"),
            password = os.getenv("SNOWFLAKE_PASSWORD"),
            account = os.getenv("SNOWFLAKE_ACCOUNT"),
            # warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
            # database = os.getenv("SNOWFLAKE_DATABASE"),
            # schema = os.getenv("SNOWFLAKE_SCHEMA")
        )
        return conn
    except Exception as e:
        print(f"Error in connection: {e}")
        return None

def create_wh():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        sql = '''
        CREATE WAREHOUSE IF NOT EXISTS STOCK_WH;
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        print(" Warehouse STOCK_WH created")
    except Exception as e:
        print(f"Error in creating warehouse: {e}")
        return None

def create_db():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        sql = '''
        CREATE DATABASE IF NOT EXISTS STOCKS;
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        print("Database STOCKS created")
    except Exception as e:
        print(f"Error in creating database: {e}")
        return None

def create_schema():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("USE DATABASE STOCKS;")
        sql = '''
        CREATE SCHEMA IF NOT EXISTS SILVER;
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        print("Schema SILVER created")
    except Exception as e:
        print(f"Error in creating schema: {e}")
        return None
    
def create_table():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("USE DATABASE STOCKS;")
        cursor.execute("USE SCHEMA SILVER;")
        sql = '''
        CREATE TABLE IF NOT EXISTS PRICE (
            DATETIME TEXT PRIMARY KEY,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME FLOAT
        )
        '''
        cursor.execute(sql)
        conn.commit()
        conn.close()
        print("Table PRICE created")
    except Exception as e:
        print(f"Error in creating table: {e}")
        return None
    
def create_role():
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        sql = '''
        USE ROLE ACCOUNTADMIN;
        CREATE ROLE IF NOT EXISTS STOCK_ANALYST;
        GRANT USAGE ON WAREHOUSE STOCK_WH TO ROLE STOCK_ANALYST;
        GRANT USAGE ON DATABASE STOCKS TO ROLE STOCK_ANALYST;
        GRANT USAGE ON SCHEMA STOCKS.SILVER TO ROLE STOCK_ANALYST;
        GRANT ROLE STOCK_ANALYST TO USER {};
        '''.format(os.getenv("SNOWFLAKE_USER"))
        cursor.execute(sql, num_statements=6)
        conn.commit()
        conn.close()
        print("Role STOCK_ANALYST created and granted privileges")
    except Exception as e:
        print(f"Error in creating role: {e}")
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
        print("Table cleared")
        conn.close()
    except Exception as e:
        print(f"Error in deleting PRICE table: {e}")
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
    

