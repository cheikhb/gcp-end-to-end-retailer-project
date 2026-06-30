Composer CI/CD

This guide outlines an approach to implementing CI/CD in Cloud Composer using Cloud Build and GitHub.

File Layout

.
├── dags
│   ├── bq_dag.py
│   ├── pyspark_dag.py
├── data
│   ├── BQ
│   │   ├── bronzeTable.sql
│   │   ├── silverTable.sql
│   │   ├── goldTable.sql
│   ├── DBs
│   │   ├── retailerdb.sql
│   │   ├── supplierdb.sql
│   ├── INGESTION
│   │   ├── customerReviews_API.py
│   │   ├── retailerMysqlToLanding.py
│   │   ├── supplierMysqlToLanding.py
├── utils
│   ├── add_dags_to_composer.py
│   ├── requirements.txt
├── cloudbuild.yaml
├── README.md

📂 Directory Structure Explanation

📁 dags/

Contains DAGs that orchestrate workflows in Cloud Composer:

📝 bq_dag.py - DAG for BigQuery workflows.

📝 pyspark_dag.py - DAG for PySpark-based workflows.

📁 data/

Contains SQL scripts and ingestion logic:

BQ/ - SQL scripts for creating BigQuery tables:

📄 bronzeTable.sql, silverTable.sql, goldTable.sql

DBs/ - SQL scripts for initializing databases:

📄 retailerdb.sql, supplierdb.sql

INGESTION/ - Python scripts for data ingestion:

🖥️ customerReviews_API.py - Fetches customer reviews from API.

🖥️ retailerMysqlToLanding.py - Extracts data from retailer MySQL database.

🖥️ supplierMysqlToLanding.py - Extracts data from supplier MySQL database.

📁 utils/

Utility scripts to assist with deployment:

🛠️ add_dags_to_composer.py - Syncs DAGs with Cloud Composer after merging changes.

📜 requirements.txt - Contains dependencies required for utilities.

📁 cloudbuild.yaml

Cloud Build configuration file to automate CI/CD for Cloud Composer.

📁 README.md

This documentation file explaining the project structure and workflow.

🚀 Automated CI/CD Workflow

Develop and Push Changes

Modify or create a DAG or ingestion script.

Push the changes to a development branch.

Create a Pull Request (PR)

Open a PR against the main branch in GitHub.

Run Validation Tests with Cloud Build

Cloud Build triggers validation tests.

Checks DAG syntax and integrity.

Approval and Merge

Once reviewed, approve and merge the PR into main.

Sync Changes to Cloud Composer

Cloud Build syncs DAGs and dependencies with Cloud Composer.

Verify DAG Execution

Check if the new DAGs and updates behave as expected in Cloud Composer.

This setup ensures a seamless CI/CD pipeline for managing workflows in Cloud Composer with Cloud Build integration. 🚀

