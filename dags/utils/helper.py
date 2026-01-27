import yfinance as yf
from snowflake_setup import *
import logging
from snowflake.connector.pandas_tools import write_pandas
import pandas

conn = snowflake_connection()
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
table = os.getenv("SNOWFLAKE_TABLE")
temp_table = os.getenv("SNOWFLAKE_TEMP_TABLE")

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
if __name__ == "__main__":
      logging.basicConfig(level=logging.INFO)

## Run one only
## Configure warehouse, database, schema, table, role
# create_wh()
# create_db()
# create_schema()
# create_table()
# create_role()

def write_table(df, database, schema, table_name, conn):
    try:
        logging.info(f"Updating Temporary Table {table_name}...")
        cursor = conn.cursor()
        cursor.execute("USE ROLE STOCK_ANALYST;")
        cursor.execute(f"USE DATABASE {database};")
        cursor.execute(f"USE SCHEMA {schema};")
        df['DATETIME'] = df['DATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
        records = df.values.tolist()
        print(type(records))
        print(records[0])
        print(type(records[0]))
        cursor.executemany(
            f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s)",
            records
        )
        logging.info(f"Temporary Table {table_name} Updated")
    except Exception as e:
        print(f"Error in writing in TEMP TABLE: {e}")
        return None
    


def fetch_and_staging(ticker="SI=F", period="2d", interval="1h"):
    """
    Fetch data from Yahoo Finance and write to Snowflake temporary table.
    Input:
    - ticker: Stock / Commodity code (SI=F, AAPL)
    - period: Period of the data to be fetched.
    - interval: Interval of the data to be fetched.
    """

    logger.info(f"Fetching {ticker} data from Yahoo Finance")
    silver = yf.Ticker(ticker)
    data = silver.history(period=period, interval=interval)
    data = data.drop(['Dividends', 'Stock Splits'], axis=1)
    data = data.reset_index()
    data["Ticker"] = ticker
    data['Datetime'] = data['Datetime'].dt.tz_convert('UTC')

    data = data[['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
    data.columns = ['DATETIME', 'TICKER', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    data.drop_duplicates(subset=['DATETIME', 'TICKER'], keep='first', inplace=True)
    logger.info(f"Fetched {len(data)} rows for {ticker} from {data['DATETIME'].min()} to {data['DATETIME'].max()}")
    logger.info(f"DataFrame dtypes: {data.dtypes}")
    logger.info(f"Sample datetime: {data['DATETIME'].iloc[0]}")

    try:
        cursor = conn.cursor()
        cursor.execute("USE ROLE STOCK_ANALYST;")
        cursor.execute(f"USE DATABASE {database};")
        cursor.execute(f"USE SCHEMA {schema};")
        sql = f'''
        CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table} (
            DATETIME TIMESTAMP_TZ,
            TICKER TEXT,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME FLOAT
        )
        '''
        cursor.execute(sql)
        conn.commit()
        logging.info(f"Temporary Table {temp_table} created")

        # Write to Temp table
        logging.info(f"Writing to Temporary Table {temp_table}...")

        write_table(data, database, schema, temp_table, conn)

    except Exception as e:
        print(f"Error in creating table: {e}")
        return None

def merge_to_main():
    """
    Merging Snowflake temporary staging table to main table.
    """
    try:
        logging.info(f"Merging Temporary Table {temp_table} to main...")

        cursor = conn.cursor()
        cursor.execute("USE ROLE STOCK_ANALYST;")
        cursor.execute(f"USE DATABASE {database};")
        cursor.execute(f"USE SCHEMA {schema};")
        # Insert merging function to main table
        sql = f'''
        MERGE INTO {table} AS target
        USING {temp_table} AS source
        ON target.DATETIME = source.DATETIME
            AND target.TICKER = source.TICKER
        WHEN NOT MATCHED THEN
            INSERT (DATETIME, TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES (source.DATETIME, source.TICKER, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME)
        '''
        cursor.execute(sql)
        conn.commit()

        # Fetch the one-row result
        result = cursor.fetchone()
        rows_inserted = result[0]
        logging.info(f"Merged tables. Added {rows_inserted} new rows.")

        sql = f"SELECT COUNT(*) FROM {table}"
        cursor.execute(sql)
        total_rows = cursor.fetchone()[0]
        logging.info(f"Merged tables. Total rows in {table}: {total_rows}")
        conn.close() # Temporary table disappears after session closes.

    except Exception as e:
        print(f"Error in merging table: {e}")
        return None

df = fetch_and_staging(ticker="AAPL", period="2d", interval="1h")
merge_to_main() 
