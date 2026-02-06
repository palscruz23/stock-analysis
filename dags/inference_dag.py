from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping 
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
# from utils.snowflake_setup import snowflake_connection
from utils.ml_helper import *

DBT_PROJECT_PATH = os.path.join(os.environ["AIRFLOW_HOME"], "dbt", 
"stock_analysis")

profile_config = ProfileConfig(
    profile_name="stock_analysis",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
    )
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    models_relative_path="models",
    dbt_vars= {"ticker": Variable.get("TICKER")}
)

default_args = {
    'owner': Variable.get("USER"),
    'retries': 3,  
    'retry_delay': timedelta(minutes=5),
}
with DAG(
      dag_id='inference-pipeline',  
      default_args=default_args,
      schedule='0 20 * * *',  # Remember: daily at 6am?
      start_date=datetime(2026, 1, 27),
      catchup=False,
  ) as dag:
        retrieve_data = DbtTaskGroup(
            group_id="inference-preprocessing",
            project_config=project_config,
            profile_config=profile_config,
            render_config=RenderConfig(
                select=["+inference_features"]
            ),
        )

        task_predict = PythonOperator(
            task_id="run_prediction",
            python_callable=predict, 
        )    

        retrieve_data >> task_predict 

