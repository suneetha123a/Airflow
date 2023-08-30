from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime

dag = DAG(
    dag_id="branch",
    schedule_interval=None,
    start_date=datetime(2023, 7, 1),
    catchup=False
)

def branchfunction(**kwargs):
    val = kwargs['op_args'][0]  # Get the value of 'val' from op_args
    if val == 7:
        return 'taska'
    else:
        return 'taskb'

def pyfunctiona():
    print("hey i am a")

def pyfunctionb():
    print("hey i am b")

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=branchfunction,
    op_args=[7],
    provide_context=True,
    dag=dag
)

taska = PythonOperator(
    task_id="taska",
    python_callable=pyfunctiona,
    dag=dag
)

taskb = PythonOperator(
    task_id="taskb",
    python_callable=pyfunctionb,
    dag=dag
)

branch >> taska
branch >> taskb
