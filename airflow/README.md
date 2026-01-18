# Airflow Orchestration

This folder contains:

- `dags/`: Defines pipeline workflow
- `docker-compose.yaml`: Airflow local setup

### How to run

1. Start Airflow with Docker  
2. Set up Databricks connection under Airflow Admin → Connections  
3. Trigger `ecommerce_medallion_pipeline` DAG
