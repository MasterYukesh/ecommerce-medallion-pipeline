# Ecommerce Medallion Data Pipeline

### 🚀 Overview

This repository contains an end-to-end **Data Engineering pipeline** built using **Apache Airflow**, **Databricks**, **PySpark**, and **Delta Lake** based on the **Medallion Architecture** (Bronze → Silver → Gold).  
It demonstrates production-grade practices like incremental loads, SCD Type 2 support, SLA monitoring, failure alerts, and data quality checks.

---

## 📦 Folder Structure

ecommerce-medallion-pipeline/
├── airflow/
│ ├── dags/
│ │ └── ecommerce_medallion_dag.py # Airflow DAG
│ ├── docker-compose.yaml # Airflow setup
│ └── README.md # Airflow folder info
│
├── databricks/
│ ├── bronze_ingestion.py # Bronze layer logic
│ ├── silver_transform.py # Silver layer logic
│ └── gold_aggregation.py # Gold layer logic
│
├── data/
│ └── sample_data.csv # Optional sample dataset
│
├── diagrams/
│ └── architecture.png # Architecture diagram
│
├── .gitignore
└── README.md # Project overview



## 🧰 Tech Stack

- **Apache Spark (PySpark)**
- **Databricks**
- **Delta Lake**
- **Apache Airflow (Docker)**
- **Docker Compose**
- **Python**
- **SQL**

---

## 🚦 Features

✔ Bronze → Silver → Gold layered pipeline  
✔ Incremental ingest with **Delta MERGE**  
✔ **SCD Type 2** implementation (Customer dimension)  
✔ Workflow orchestration with **Airflow DAG**  
✔ Task dependencies and SLA  
✔ Failure alerts via email  
✔ Data quality validations in notebooks

---

## 📌 Getting Started — Airflow

### 1. Start Airflow
```bash
cd airflow
docker compose up airflow-init
docker compose up
Open UI:

arduino
Copy code
http://localhost:8080
Login:

nginx
Copy code
airflow / airflow
2. Configure Databricks Connection
In Airflow UI:

nginx
Copy code
Admin → Connections → Databricks
Add connection with your workspace URL and token.

3. Trigger Pipeline
Turn on the DAG → Trigger manually → Observe sequential run.

🧠 How to Use Databricks Notebooks
Each Python file under /databricks represents stage logic:

bronze_ingestion.py: Ingest raw data & add metadata

silver_transform.py: Clean, deduplicate, apply SCD2

gold_aggregation.py: Create analytics-ready tables

You can import these as notebooks or jobs in Databricks.

📈 Future Enhancements
Slack alerts / webhook notifications

Runtime metrics logging

Parameterized pipelines (e.g., date partitions)

Data quality framework (dbt, Great Expectations)

👨‍💻 Author
Yukeswaran
Data Engineer | Spark | Databricks | Airflow | SQL
LinkedIn: https://www.linkedin.com/in/MasterYukesh/
GitHub: https://github.com/MasterYukesh
