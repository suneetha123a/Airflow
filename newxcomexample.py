from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def first():
    return "Abhi"

def second():
    val= context['task_instance'].xcom_pull(task_ids='first')
    print(f'hello {val} i am second task you know' )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hey_xcom',
    default_args=default_args,
    schedule_interval='@once',
)

task1=PythonOperator(task_id="task1",python_callable=first,dag=dag)
task2=PythonOperator(task_id="task2",python_callable=second,dag=dag)


task1>>task2
