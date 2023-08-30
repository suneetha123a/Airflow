from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def get_person_info():
    # Simulating a function that returns name and age
    return {'name': 'John Doe', 'age': 35}

def print_person_info(**context):
    # Retrieve name and age from XCom
    person_info = context['task_instance'].xcom_pull(task_ids='get_person_info')
    name = person_info['name']
    age = person_info['age']

    # Print the information
    print(f"Hey, I am {name} and my age is {age}.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xcom_example_testii',
    default_args=default_args,
    schedule_interval='@daily',
)

get_person_info_task = PythonOperator(
    task_id='get_person_info',
    python_callable=get_person_info,
    dag=dag,
)

print_person_info_task = PythonOperator(
    task_id='print_person_info',
    python_callable=print_person_info,
    provide_context=True,
    dag=dag,
)

get_person_info_task >> print_person_info_task
