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

def train():
    mlflow.set_tracking_uri("http://mlflow:5000")  
    mlflow.set_experiment("stock-classifier")  
    xtrain, xtest, ytrain, ytest = preprocess()
    grid_search_rforest(xtrain, xtest, ytrain, ytest)