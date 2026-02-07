# Stock Analysis MLOps Pipeline

This project is an end-to-end MLOps pipeline to perform stock price movement prediction (Silver Futures) in an hourly basis. It involves orchestration using Airflow to facilitate routine data ingestion, ML training and inference workflows. Model training and registry are captured using MLFlow. Data is stored in Snowflake.

## Architecture

```
API ──► Database (Price) ──► dbt transformations ──► ML Training 
                                        │                  │
                                        │                  ▼
                                        │        MLflow Model Registry
                                        │        (Champion/Challenger)
                                        ▼
          MLflow Model Registry ──► ML Inference ──► Database (Predict_Movement)
                (Champion)
```

### Airflow DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `stock-data-pipeline` | Hourly | Fetches stock data from Yahoo Finance, stages to Snowflake, merges to `PRICE` table |
| `ml-pipeline` | Daily | Runs dbt feature engineering, trains RandomForest with grid search, registers best model, champion/challenger evaluation |
| `inference-pipeline` | Hourly | Runs dbt inference features, loads champion model from MLflow, writes predictions to Snowflake |

### Data Flow

1. **Ingestion**: `stock-data-pipeline` fetches hourly stock data from Yahoo Finance and loads it into Snowflake via a staging/merge pattern.
2. **Feature Engineering (dbt)**: Transforms raw price data into ML features:
   - `stg_price` — filters by ticker
   - `int_price_features` — computes SMA_5, SMA_20, VOL_20, volatility, percentage change
   - `ml_features` — adds labels for training (NEXT_PRICE, LABEL)
   - `inference_features` — latest features for prediction
3. **Training**: `ml-pipeline` performs grid search over RandomForest hyperparameters, logs all runs to MLflow, registers the best model as "challenger", and promotes it to "champion" if it outperforms the current champion on F1 score.
4. **Inference**: `inference-pipeline` loads the champion model, predicts price movement direction, and writes results back to Snowflake's `PREDICT_MOVEMENT` table.

## Tech Stack

- **Orchestration**: Apache Airflow via [Astronomer](https://www.astronomer.io/) (Astro Runtime 3.1)
- **Data Warehouse**: Snowflake
- **Transformations**: dbt (via [astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos))
- **ML Training & Registry**: MLflow + scikit-learn
- **Data Source**: Yahoo Finance (yfinance)

## Project Structure

```
stock-analysis/
├── dags/
│   ├── stock_pipeline_dag.py    # Data ingestion DAG
│   ├── ml_pipeline_dag.py       # Training DAG
│   ├── inference_dag.py         # Inference DAG
│   └── utils/
│       ├── helper.py            # Stock data fetch & Snowflake write utilities
│       ├── ml_helper.py         # Training, registration, inference functions
│       └── snowflake_setup.py   # Snowflake connection & setup utilities
├── dbt/
│   └── stock_analysis/
│       └── models/
│           ├── staging/         # stg_price.sql, sources.yml
│           ├── intermediate/    # int_price_features.sql
│           └── marts/           # ml_features.sql, inference_features.sql
├── docker-compose.override.yml  # MLflow service
├── Dockerfile                   # Astro Runtime base image
├── requirements.txt             # Python dependencies
├── airflow_settings.yaml        # Airflow connections & variables (local)
└── .env                         # Environment variables (not committed)
```

## Setup

### Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Docker Desktop
- Snowflake account

### 1. Configure Environment

Copy the example files and fill in your credentials:

```bash
cp .env.example .env
cp airflow_settings.yaml.example airflow_settings.yaml
```

Edit `.env` with your Snowflake credentials:

```
SNOWFLAKE_USER="your_user"
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_WAREHOUSE="STOCK_WH"
SNOWFLAKE_DATABASE="STOCKS_DB"
SNOWFLAKE_SCHEMA="STOCKS"
```

Edit `airflow_settings.yaml` to configure:
- **Connection** (`snowflake_conn`): Your Snowflake connection details
- **Variables**: `TICKER` (e.g., `SI=F`) and `USER` (DAG owner name)

### 2. Snowflake Setup

Run the setup functions in `snowflake_setup.py` once to create the warehouse, database, schema, table, and role:

```python
create_wh()
create_db()
create_schema()
create_table()
create_role()
```

### 3. Start Services

```bash
astro dev start
```

This launches Airflow (http://localhost:8080) and MLflow (http://localhost:5000).

### 4. Run the Pipelines

1. Enable `stock-data-pipeline` in the Airflow UI to begin ingesting data
2. Once sufficient data is collected, enable `ml-pipeline` to train and register models
3. After a champion model is promoted, enable `inference-pipeline` for hourly predictions

## License

This project is licensed under the [MIT License](LICENSE).
