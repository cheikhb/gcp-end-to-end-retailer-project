

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

from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
import datetime
from google.cloud import storage

# Initialize Spark Session
spark = SparkSession.builder.appName("CustomerReviewsAPI").getOrCreate()

# API Endpoint
API_URL = os.getenv("RETAILER_API_URL", "https://6a4281337602860e652190ee.mockapi.io/retailer/reviews")

# Step 1: Fetch data from API
response = requests.get(API_URL)

if response.status_code == 200:
    data = response.json()
    print(f"✅ Successfully fetched {len(data)} records.")
else:
    print(f"❌ Failed to fetch data. Status Code: {response.status_code}")
    exit()
    
# Step 2: Convert API Data to Pandas DataFrame
df_pandas = pd.DataFrame(data)

# Step 3: Get Current Date for File Naming
today = datetime.datetime.today().strftime('%Y%m%d')  # Format: YYYYMMDD

# Step 4: Define File Paths with Date
local_parquet_file = f"/tmp/customer_reviews_{today}.parquet"
GCS_BUCKET = os.getenv("RETAILER_GCS_BUCKET", "retailer-datalake-project-cheikh")
GCS_PATH = f"landing/customer_reviews/customer_reviews_{today}.parquet"

# Step 5: Save Pandas DataFrame as Parquet Locally
df_pandas.to_parquet(local_parquet_file, index=False)

# Step 6: Upload Parquet File to GCS
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_PATH)
blob.upload_from_filename(local_parquet_file)

print(f"✅ Data successfully written to gs://{GCS_BUCKET}/{GCS_PATH}")

