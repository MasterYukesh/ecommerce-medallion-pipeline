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

graph TD
    %% Define Nodes
    A[/CSV Files/]
    B[(Bronze Layer: Raw Data)]
    C[(Silver Layer: Cleaned + SCD2)]
    D[(Gold Layer: Analytics Ready)]
    E[Airflow Dashboard]

    %% Define Flow
    A -->|Ingest| B
    B -->|Transform & Historize| C
    C -->|Aggregate & Model| D
    D -->|Monitor & Visualize| E

    %% Styling
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#cd7f32,stroke:#333,stroke-width:2px,color:#fff
    style C fill:#c0c0c0,stroke:#333,stroke-width:2px
    style D fill:#ffd700,stroke:#333,stroke-width:2px
    style E fill:#6495ed,stroke:#333,stroke-width:2px,color:#fff
---

## 📌 Getting Started — Airflow


### 1. Start Airflow
```bash
cd airflow
docker compose up airflow-init
docker compose up
```
Open UI:
    a. http://localhost:8080
    b. airflow / airflow

### 2. Configure Databricks Connection

In Airflow UI:
Admin → Connections → Databricks
Add connection with your workspace URL and token.

### 3. Trigger Pipeline
Turn on the DAG → Trigger manually → Observe sequential run.

🧠 How to Use Databricks Notebooks
Each Python file under /databricks represents stage logic:

- *bronze_ingestion.py*: Ingest raw data & add metadata

- *silver_transform.py*: Clean, deduplicate, apply SCD2

- *gold_aggregation.py*: Create analytics-ready tables

You can import these as notebooks or jobs in Databricks.

### 📈 Future Enhancements
✔ Slack alerts / webhook notifications

✔ Runtime metrics logging

✔ Parameterized pipelines (e.g., date partitions)

✔ Data quality framework (dbt, Great Expectations)

👨‍💻 Author
Yukeshwaran 
Data Engineer | Spark | Databricks | Airflow | SQL

LinkedIn: https://www.linkedin.com/in/MasterYukesh/
GitHub: https://github.com/MasterYukesh
