from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

# Define your DAG
dag = DAG(
    'my_dag_LatestOnlyOperator',
    schedule_interval='@daily',
    start_date=datetime(2023, 7, 1),
    catchup=False  # Only run the latest DAG run, no backfills
)

# Define tasks
latest_only_task = LatestOnlyOperator(task_id='latest_only_task', dag=dag)

task1 = DummyOperator(task_id='task1', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)

# Define the workflow
latest_only_task >> task1 >> task2
