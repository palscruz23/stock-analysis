from utils.snowflake_setup import snowflake_connection
import logging
import pandas as pd
from airflow.hooks.base import BaseHook
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
import mlflow
from sklearn.metrics import classification_report
from itertools import product
from mlflow import MlflowClient
import json

client = MlflowClient()
MODEL_NAME = "stock-classifier"

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
if __name__ == "__main__":
      logging.basicConfig(level=logging.INFO)

def get_features(ticker="SI=F"):
    try:
        conn = snowflake_connection()
        cursor = conn.cursor()
        connector = BaseHook.get_connection("snowflake_conn")
        database = connector.extra_dejson["database"]
        schema = connector.schema
        table_name = "ML_FEATURES"
        logging.info(f"Accessing {table_name} Table...")

        cursor = conn.cursor()
        cursor.execute("USE ROLE STOCK_ANALYST;")
        cursor.execute(f"USE DATABASE {database};")
        cursor.execute(f"USE SCHEMA {schema};")
        sql = f"SELECT * FROM {table_name} WHERE TICKER=%s;"
        cursor.execute(sql, (ticker,))
        df =  cursor.fetch_pandas_all()
        logging.info(f"Features from Table {table_name} Retrieved")
        conn.close()

        return df

    except Exception as e:
        logging.error(f"Error in retrieving features from {table_name} table: {e}")
        return None
    
def preprocess():
    # Utilise features from Snowflake table and preprocessing the data.
    df = get_features()
    x = df.drop(["LABEL", "DATETIME", "TICKER", "NEXT_PRICE"], axis=1)
    y = df["LABEL"]

    # Split dataset
    xtrain, xtest, ytrain, ytest = train_test_split(x, y, test_size=0.3, random_state=23)

    # Perform feature scaling
    scaler = StandardScaler()
    xtrain_scaled = scaler.fit_transform(xtrain)
    xtest_scaled = scaler.transform(xtest)

    return (xtrain_scaled, xtest_scaled, ytrain, ytest)

def ml_flow(model, params, report):
    with mlflow.start_run():      
        mlflow.log_params(params)                  
        mlflow.log_metrics({
            'accuracy': report['accuracy'],
            'f1_macro': report['macro avg']['f1-score'],
            'precision_macro': report['macro avg']['precision'],
            'recall_macro': report['macro avg']['recall'],
        })
        mlflow.sklearn.log_model(model, model.__class__.__name__)    

def grid_search_rforest(xtrain, xtest, ytrain, ytest):
    # Hyperparameter Tuning of Random Forest 
    # Define the parameter grid to tune the hyperparameters
    logging.info("Performing grid search...")
    param_grid = {
        'n_estimators': [20, 50],
        # 'criterion': ['gini', 'entropy', 'log_loss'],
        'max_depth': [10, 20],
        'min_samples_split': [2],
        'bootstrap': [True]
    }

    # Generate all combinations
    keys = param_grid.keys()
    values = param_grid.values()
    combinations = [dict(zip(keys, v)) for v in product(*values)]  

    #Perform grid search from hyperparameters to find optimal performance
    for params in combinations:
        model = RandomForestClassifier(**params)
        model.fit(xtrain, ytrain)
        ypred_rforest = model.predict(xtest)
        report = classification_report(ytest, ypred_rforest, output_dict=True)
        ml_flow(model, params, report)
    logging.info("Grid Search done...")


def train():
    mlflow.set_tracking_uri("http://mlflow:5000")  
    mlflow.set_experiment("stock-classifier")  
    xtrain, xtest, ytrain, ytest = preprocess()
    grid_search_rforest(xtrain, xtest, ytrain, ytest)

def search_best_run():
    logging.info("Searching for best run...")
    mlflow.set_tracking_uri("http://mlflow:5000")
    best_run = mlflow.search_runs(
        experiment_names=["stock-classifier"],
        order_by=["metrics.f1_macro DESC"],
        max_results=1
    )

    run_id = best_run.iloc[0].run_id
    history_json = best_run.iloc[0]['tags.mlflow.log-model.history']
    history_data = json.loads(history_json)
    model = history_data[0]['artifact_path']

    model_uri = f"runs:/{run_id}/{model}"
    registered_model = mlflow.register_model(model_uri, name=MODEL_NAME)
    
    client.set_registered_model_alias(
        name=MODEL_NAME, 
        alias="challenger", 
        version=registered_model.version # or a specific version number like 1
    )
    logging.info("Best run found. Alias as challenger...")


def challenge_champion():
    try:
        __, xtest, __, ytest = preprocess()
        model_champion = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/champion")
        model_challenger = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/challenger")
        challenger_info = client.get_model_version_by_alias(MODEL_NAME,  "challenger")
        logging.info("Champion and Challenger present. Comparing them and promoting the winner based on F1 metric.")

        ypred_champion = model_champion.predict(xtest)
        report_champion = classification_report(ytest, ypred_champion, output_dict=True)
        f1_champion = report_champion['macro avg']['f1-score']

        ypred_challenger = model_challenger.predict(xtest)
        report_challenger = classification_report(ytest, ypred_challenger, output_dict=True)
        f1_challenger = report_challenger['macro avg']['f1-score']

        if f1_champion < f1_challenger:
            client.set_registered_model_alias(
                name=MODEL_NAME, 
                alias="champion", 
                version=challenger_info.version
            )
            logging.info(f"Challenger wins. Promoting to champion. Challenger {f1_challenger} vs Champion {f1_champion}")
            return None
        else:
            logging.info(f"Champion wins. Challenger {f1_challenger} vs Champion {f1_champion}")
            return None

    except:
        try:
            challenger_info = client.get_model_version_by_alias(MODEL_NAME,  "challenger")
            logging.info("No champion. Promoting challenger to champion.")
            client.set_registered_model_alias(
                name=MODEL_NAME, 
                alias="champion", 
                version=challenger_info.version
            )
            return None
        except:
            logging.info("No champion or challenger")
            return None
            
