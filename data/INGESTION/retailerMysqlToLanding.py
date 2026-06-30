

# --------------------------------------------------------------------------------
# NEW CELL
# --------------------------------------------------------------------------------import os

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

from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("RetailerMySQLToLanding").getOrCreate()

# Google Cloud Storage (GCS) Configuration variables
GCS_BUCKET = os.getenv("RETAILER_GCS_BUCKET", "retailer-datalake-project-cheikh")
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/retailer_config.csv"

# BigQuery Configuration
BQ_PROJECT = os.getenv("BQ_PROJECT", "data-analysis-303816")
BQ_DATASET = os.getenv("BQ_DATASET", "temp_dataset")
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# NOTE: move credentials to Secret Manager / environment variables instead of
# hardcoding them here. Example:
#   import os
#   MYSQL_CONFIG = {"password": os.environ["MYSQL_PASSWORD"], ...}
MYSQL_CONFIG = {
    "url": os.getenv("RETAILER_MYSQL_URL", "jdbc:mysql://34.61.30.20:3306/retailerDB?useSSL=true&requireSSL=true&verifyServerCertificate=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=30000"),
    "driver": os.getenv("RETAILER_MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver"),
    "user": os.getenv("RETAILER_MYSQL_USER", "cheikh"),
    "password": os.getenv("RETAILER_MYSQL_PASSWORD", "c12081987B@")
}

storage_client = storage.Client()
bq_client = bigquery.Client()

# Logging Mechanism
log_entries = []  # Stores logs before writing to GCS

##---------------------------------------------------------------------------------------------------##
def log_event(event_type, message, table=None):
    """Log an event and store it in the log list"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")  # Print for visibility

##---------------------------------------------------------------------------------------------------##
def ensure_bq_tables_exist():
    """Create the audit_log and pipeline_logs tables/dataset if they don't already exist."""
    dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
        log_event("INFO", f"Created BigQuery dataset {BQ_DATASET}")

    audit_schema = [
        bigquery.SchemaField("tablename", "STRING"),
        bigquery.SchemaField("load_type", "STRING"),
        bigquery.SchemaField("record_count", "INTEGER"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
    ]
    audit_table_ref = bigquery.TableReference(dataset_ref, "audit_log")
    try:
        bq_client.get_table(audit_table_ref)
    except Exception:
        bq_client.create_table(bigquery.Table(audit_table_ref, schema=audit_schema))
        log_event("INFO", "Created BigQuery table audit_log")

    log_schema = [
        bigquery.SchemaField("timestamp", "STRING"),
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("message", "STRING"),
        bigquery.SchemaField("table", "STRING"),
    ]
    log_table_ref = bigquery.TableReference(dataset_ref, "pipeline_logs")
    try:
        bq_client.get_table(log_table_ref)
    except Exception:
        bq_client.create_table(bigquery.Table(log_table_ref, schema=log_schema))
        log_event("INFO", "Created BigQuery table pipeline_logs")

##---------------------------------------------------------------------------------------------------##
def save_logs_to_gcs():
    """Save logs to a JSON file and upload to GCS"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)

    blob.upload_from_string(json_data, content_type="application/json")
    print(f"✅ Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")


def save_logs_to_bigquery():
    """Save logs to BigQuery"""
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()
        print("✅ Logs stored in BigQuery for future analysis")

##---------------------------------------------------------------------------------------------------##
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "✅ Successfully read the config file")
    return df

##---------------------------------------------------------------------------------------------------##
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/retailer-db/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        # Extract Date from File Name (products_27032025.json)
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        archive_path = f"landing/retailer-db/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()

        log_event("INFO", f"✅ Moved {file} to {archive_path}", table=table)

##---------------------------------------------------------------------------------------------------##
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}'
    """
    try:
        query_job = bq_client.query(query)
        result = query_job.result()
        for row in result:
            return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
        return "1900-01-01 00:00:00"
    except Exception as e:
        log_event("WARNING", f"Could not read watermark for {table_name}, defaulting to 1900-01-01: {str(e)}", table=table_name)
        return "1900-01-01 00:00:00"

##---------------------------------------------------------------------------------------------------##
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full load" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read
                .format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())
        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)

        pandas_df = df.toPandas()
        json_data = pandas_df.to_json(orient="records", lines=True)

        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/retailer-db/{table}/{table}_{today}.json"

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(json_data, content_type="application/json")

        log_event("SUCCESS", f"✅ JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)

        audit_df = spark.createDataFrame([
            (table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")], ["tablename", "load_type", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save())

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)

##---------------------------------------------------------------------------------------------------##
# Main Execution
ensure_bq_tables_exist()
config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == '1':
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()

print("✅ Pipeline completed successfully!")

