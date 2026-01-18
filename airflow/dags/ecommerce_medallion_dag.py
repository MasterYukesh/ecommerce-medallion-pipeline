from airflow import DAG
#from airflow.utils.dates import days_ago
from pendulum import datetime
from datetime import timedelta
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

def task_failure_alert(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exec_date = context['execution_date']
    log_url = context['task_instance'].log_url

    print(f'DAG \t: {dag_id} \n Task \t: {task_id} \n Execution Time \t: {exec_date} \n Log URL \t: {log_url}')

default_args = {
    'owner' : 'yukesh',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # On Failure Alert 
    'on_failure_callback':task_failure_alert,
    # Email notification on failure
    'email':['masteryukesh@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':  False
}

dag = DAG(
    dag_id= 'ecommerce_medallion_pipeline' ,
    default_args= default_args,
    schedule='*/20 * * * *',
    start_date=datetime(year=2026,month=1,day=15),
    catchup = False
)


bronze_task = DatabricksRunNowOperator(
    task_id = "bronze_task",
    databricks_conn_id= "Yuke_Databricks",
    job_id = 1006296377795555,
    sla = timedelta(minutes=5),
    #notebook_task= {
     #   "notebook_path" :"/Workspace/BatchProcessing_DataEngineering_Project/01_Bronze_Layer"
    #},
    dag = dag
)

silver_task = DatabricksRunNowOperator(
    task_id = "silver_task",
    databricks_conn_id= "Yuke_Databricks",
    job_id=947521183532661,
    sla = timedelta(minutes=10),
    # notebook_task= {
    #     "notebook_path" :"/Workspace/BatchProcessing_DataEngineering_Project/02_Silver_Layer"
    # },
    dag = dag
)

gold_task = DatabricksRunNowOperator(
    task_id = "gold_task",
    databricks_conn_id= "Yuke_Databricks",
    job_id= 1028506407474723,
    sla = timedelta(minutes=5),
    # notebook_task= {
    #     "notebook_path" :"/Workspace/BatchProcessing_DataEngineering_Project/03_Gold_Layer"
    # },
    dag = dag
) 

bronze_task >> silver_task >> gold_task