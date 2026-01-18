# Databricks Notebooks

This folder contains the Python scripts that power each stage of the medallion pipeline:

- `bronze_ingestion.py`: Bronze layer ingestion logic  
- `silver_transform.py`: Silver transformation & SCD2 logic  
- `gold_aggregation.py`: Gold layer aggregations

These scripts are designed to be used as Databricks jobs or notebooks.
