from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def test(name):
    print("hello  "+name+" !")

def greet():
    return "returning xcom value"

dag=DAG(dag_id="xcom_testing_v3",
        start_date=datetime(2023,7,7),
        schedule_interval=None)


task1=PythonOperator(task_id="task1",
                     python_callable=test,
                     op_kwargs={"name":"hari"},
                     dag=dag)

task2=PythonOperator(task_id="task2",
                     python_callable=greet,
                     dag=dag)


task3=BashOperator(task_id="task3",bash_command="echo hello, this is bash command", dag=dag)

task1>>task2
task1>>task3
