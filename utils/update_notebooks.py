import json
import os
import re

LOAD_ENV_CODE = """import os

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
"""

def update_notebook(file_path):
    print(f"Processing notebook: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    py_code = []
    prepend_env = True

    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "code":
            source = cell.get("source", "")
            if isinstance(source, list):
                source = "".join(source)
            
            # Skip empty cells or cells with only comments/spaces
            if not source.strip():
                cell["source"] = []
                continue
            
            # Remove any previous load_env and import os injections to start clean
            source = re.sub(r'import os\n\ndef load_env\(\):.*?# Load env variables\s+load_env\(\)\s*', '', source, flags=re.DOTALL)
            source = re.sub(r'def load_env\(\):.*?# Load env variables\s+load_env\(\)\s*', '', source, flags=re.DOTALL)
            
            # Remove extra import os statements at the start that might be duplicated
            source = re.sub(r'^import os\s*', '', source)
            
            # Modify source variables to use env
            updated_source = source
            
            # Do replacements for customerReviews_API
            if "customerReviews_API" in file_path:
                updated_source = updated_source.replace(
                    'API_URL = "https://6a4281337602860e652190ee.mockapi.io/retailer/reviews"',
                    'API_URL = os.getenv("RETAILER_API_URL", "https://6a4281337602860e652190ee.mockapi.io/retailer/reviews")'
                )
                updated_source = updated_source.replace(
                    'GCS_BUCKET = "retailer-datalake-project-cheikh"',
                    'GCS_BUCKET = os.getenv("RETAILER_GCS_BUCKET", "retailer-datalake-project-cheikh")'
                )

            # Do replacements for retailerMysqlToLanding
            if "retailerMysqlToLanding" in file_path:
                updated_source = updated_source.replace(
                    'GCS_BUCKET = "retailer-datalake-project-cheikh"',
                    'GCS_BUCKET = os.getenv("RETAILER_GCS_BUCKET", "retailer-datalake-project-cheikh")'
                )
                updated_source = updated_source.replace(
                    'BQ_PROJECT = "data-analysis-303816"',
                    'BQ_PROJECT = os.getenv("BQ_PROJECT", "data-analysis-303816")'
                )
                updated_source = updated_source.replace(
                    'BQ_DATASET = "temp_dataset"',
                    'BQ_DATASET = os.getenv("BQ_DATASET", "temp_dataset")'
                )
                # Replace MYSQL_CONFIG
                old_mysql = 'MYSQL_CONFIG = {\n    "url": "jdbc:mysql://34.61.30.20:3306/retailerDB?useSSL=true&requireSSL=true&verifyServerCertificate=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=30000",\n    "driver": "com.mysql.cj.jdbc.Driver",\n    "user": "cheikh",\n    "password": "c12081987B@"\n}'
                new_mysql = 'MYSQL_CONFIG = {\n    "url": os.getenv("RETAILER_MYSQL_URL", "jdbc:mysql://34.61.30.20:3306/retailerDB?useSSL=true&requireSSL=true&verifyServerCertificate=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=30000"),\n    "driver": os.getenv("RETAILER_MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver"),\n    "user": os.getenv("RETAILER_MYSQL_USER", "cheikh"),\n    "password": os.getenv("RETAILER_MYSQL_PASSWORD", "c12081987B@")\n}'
                updated_source = updated_source.replace(old_mysql, new_mysql)

            # Do replacements for supplierMysqlToLanding
            if "supplierMysqlToLanding" in file_path:
                updated_source = updated_source.replace(
                    'GCS_BUCKET = "retailer-datalake-project-cheikh"',
                    'GCS_BUCKET = os.getenv("RETAILER_GCS_BUCKET", "retailer-datalake-project-cheikh")'
                )
                updated_source = updated_source.replace(
                    'BQ_PROJECT = "data-analysis-303816"',
                    'BQ_PROJECT = os.getenv("BQ_PROJECT", "data-analysis-303816")'
                )
                updated_source = updated_source.replace(
                    'BQ_DATASET = "temp_dataset"',
                    'BQ_DATASET = os.getenv("BQ_DATASET", "temp_dataset")'
                )
                # Replace MYSQL_CONFIG
                old_mysql = 'MYSQL_CONFIG = {\n    "url": "jdbc:mysql://34.172.69.91:3306/supplierDB?useSSL=true&requireSSL=true&verifyServerCertificate=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=30000",\n    "driver": "com.mysql.cj.jdbc.Driver",\n    "user": "cheikh",\n    "password": "c12081987B@"\n}'
                new_mysql = 'MYSQL_CONFIG = {\n    "url": os.getenv("SUPPLIER_MYSQL_URL", "jdbc:mysql://34.172.69.91:3306/supplierDB?useSSL=true&requireSSL=true&verifyServerCertificate=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=30000"),\n    "driver": os.getenv("SUPPLIER_MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver"),\n    "user": os.getenv("SUPPLIER_MYSQL_USER", "cheikh"),\n    "password": os.getenv("SUPPLIER_MYSQL_PASSWORD", "c12081987B@")\n}'
                updated_source = updated_source.replace(old_mysql, new_mysql)

            # Prepend env loading logic to the first cell
            if prepend_env:
                updated_source = LOAD_ENV_CODE + "\n" + updated_source
                prepend_env = False

            # Update cell
            cell["source"] = updated_source.splitlines(keepends=True)
            py_code.append(updated_source)

    # Save modified .ipynb back
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1)

    # Write as .py file
    py_file_path = file_path.replace(".ipynb", ".py")
    with open(py_file_path, "w", encoding="utf-8") as f:
        f.write("\n\n# " + "-"*80 + "\n# NEW CELL\n# " + "-"*80 + "\n\n".join(py_code))
    print(f"Generated python script: {py_file_path}")

if __name__ == "__main__":
    ingestion_dir = "data/INGESTION"
    for file in os.listdir(ingestion_dir):
        if file.endswith(".ipynb"):
            update_notebook(os.path.join(ingestion_dir, file))
