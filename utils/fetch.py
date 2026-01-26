import yfinance as yf
from snowflake_setup import *

# Download Apple historical data
silver = yf.Ticker("SI=F")
data = silver.history(period="1d", interval="1h")
print(data)
print(data.columns)
print(data.drop(['Dividends', 'Stock Splits'], axis=1))


# Connecting to snowflake
snowflake_connection()

# Configure warehouse, database, schema, table, role
create_wh()
create_db()
create_schema()
create_table()
create_role()




