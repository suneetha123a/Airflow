from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

dag = DAG(
    'action_upstream_failed',
    schedule_interval=None,
    start_date=datetime(2023, 7, 1),
    catchup=False
)

def task_a_function():
    # Simulate a failure in task_a by raising an exception
    raise Exception("Task A failed!")

task_a = PythonOperator(
    task_id='task_a',
    python_callable=task_a_function,
    dag=dag
)

task_b = DummyOperator(task_id='task_b', dag=dag)
task_c = DummyOperator(task_id='task_c', dag=dag)

task_a >> task_b
task_a >> task_c
