import os
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

def load_env():
    # Try common locations for .env
    possible_paths = [
        ".env",
        "../.env",
        "../../.env",
        "/home/airflow/gcs/dags/.env",
        "/home/airflow/gcs/data/.env",
    ]
    for path in possible_paths:
        if os.path.exists(path):
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    val = val.strip().strip('"').strip("'")
                    os.environ[key.strip()] = val
            break

# Load env variables
load_env()

# Define constants
PROJECT_ID = os.getenv("PROJECT_ID", "data-analysis-303816")
LOCATION = os.getenv("LOCATION", "US")
SQL_FILE_PATH_1 = os.getenv("SQL_FILE_PATH_BRONZE", "/home/airflow/gcs/data/BQ/bronzeTable.sql")
SQL_FILE_PATH_2 = os.getenv("SQL_FILE_PATH_SILVER", "/home/airflow/gcs/data/BQ/silverTable.sql")
SQL_FILE_PATH_GOLD = os.getenv("SQL_FILE_PATH_GOLD", "/home/airflow/gcs/data/BQ/goldTable.sql")

RETAILER_GCS_BUCKET = os.getenv("RETAILER_GCS_BUCKET", "retailer-datalake-project-cheikh")

# Read SQL query from file
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1).format(
    PROJECT_ID=PROJECT_ID,
    RETAILER_GCS_BUCKET=RETAILER_GCS_BUCKET
)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2).format(
    PROJECT_ID=PROJECT_ID
)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_GOLD).format(
    PROJECT_ID=PROJECT_ID
)

# Define default arguments
ARGS = {
    "owner": "Cheikh Badiane",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="DAG to run the bigquery jobs",
    default_args=ARGS,
    tags=["gcs", "bq", "etl", "marvel"]
) as dag:

    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

# Define dependencies
bronze_tables >> silver_tables >> gold_tables