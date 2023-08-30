from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime
from datetime import timedelta

def check_weekday():
    current_day = datetime.now().weekday()
    if current_day < 5:  # Monday to Friday (0 to 4)
        return 'task_a'
    else:  # Saturday or Sunday (5 or 6)
        return 'task_b'

def task_a_function():
    print("It's a weekday. Task A executed.")

def task_b_function():
    print("It's a weekend. Task B executed.")

# Define the DAG
dag = DAG(
    'weekday_weekend_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 1),
    catchup=False
)

# Define tasks
check_weekday_task = BranchPythonOperator(
    task_id='check_weekday',
    python_callable=check_weekday,
    dag=dag
)

task_a = PythonOperator(
    task_id='task_a',
    python_callable=task_a_function,
    dag=dag
)

task_b = PythonOperator(
    task_id='task_b',
    python_callable=task_b_function,
    dag=dag
)

end_task = DummyOperator(task_id='end_task', dag=dag,trigger_rule='one_done')

# Set up task dependencies based on the condition
check_weekday_task >> [task_a, task_b]
task_a >> end_task
task_b >> end_task
