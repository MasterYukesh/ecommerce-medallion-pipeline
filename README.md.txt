📌 Ecommerce Medallion Data Pipeline
🚀 Project Overview

This project implements an end-to-end Data Engineering pipeline using the Medallion Architecture (Bronze → Silver → Gold) on an E-commerce dataset. The pipeline ingests raw data, applies transformations, enforces data quality, and produces analytics-ready tables.

Orchestration is handled using Apache Airflow, while transformations run on Databricks using Spark and Delta Lake.

🏗 Architecture
Source Files (CSV)
      ↓
Bronze Layer (Raw Ingestion + Metadata)
      ↓
Silver Layer (Cleaning + Dedup + SCD Type 2)
      ↓
Gold Layer (Aggregations + Analytics Tables)
      ↓
Airflow Orchestration + Monitoring


(Add architecture diagram in /diagrams)

🧰 Tech Stack

Apache Spark (PySpark)

Databricks

Delta Lake

Apache Airflow (Dockerized)

Python

SQL

GitHub

✅ Key Features

✔ Medallion architecture implementation (Bronze / Silver / Gold)

✔ Incremental data loads using Delta MERGE

✔ SCD Type 2 implementation for customer dimension

✔ Workflow orchestration using Airflow DAG

✔ Task dependencies with retries and SLA monitoring

✔ Failure alerts and monitoring

✔ Data quality validation gates

✔ Production-style folder structure and configuration

📂 Repository Structure
airflow/      → DAGs and Docker setup
databricks/   → Spark transformation notebooks
data/         → Sample dataset (optional)
diagrams/     → Architecture diagrams

▶ How to Run (Local)
1. Start Airflow
cd airflow
docker compose up airflow-init
docker compose up


Open:

http://localhost:8080


Login:

airflow / airflow

2. Configure Databricks Connection

In Airflow UI:

Admin → Connections → Databricks


Add host and token.

3. Trigger DAG

Turn ON the DAG and trigger manually.

📈 Future Enhancements

Slack alerts integration

Automated data validation framework

Performance optimization (Z-ORDER, OPTIMIZE)

Parameterized pipelines

👨‍💻 Author

Yukeswaran
Data Engineer | Spark | Databricks | Airflow | SQL

LinkedIn: in/yukeswaran-master-5630791b9
GitHub: https://github.com/MasterYukesh