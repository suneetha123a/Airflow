from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def test(age,ti):
    name = ti.xcom_pull(task_ids='get_name')

    print(f"hello  world! my name {name},"
          f" and i am {age} years old!")

def get_name():
    return 'Jon'

dag=DAG(dag_id="xcom_testing_v4",
        start_date=datetime(2023,7,7),
        schedule_interval='@once')


task1=PythonOperator(task_id="task1",
                     python_callable=test,
                     op_kwargs={'age':20},
                     dag=dag)

task2=PythonOperator(task_id="task2",
                     python_callable=get_name,
                     dag=dag)


task2>>task1

