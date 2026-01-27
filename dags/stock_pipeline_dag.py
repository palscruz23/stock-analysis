from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv()

default_args = {
    'owner': os.getenv("USER"),
    'retries': 3,  # How many times to retry on failure?
    'retry_delay': timedelta(minutes=5),
}

with DAG(
      dag_id='Stock Data Pipeline',  # What should you name it?
      default_args=default_args,
      schedule='0 4 * * *',  # Remember: daily at 6am?
      start_date=datetime(2026, 1, 27),
      catchup=False,
  ) as dag: