from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
    
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="DAG3",
    start_date=datetime(2023,1,7),
):
     task1=EmptyOperator(task_id="empty")