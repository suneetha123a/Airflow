from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def test(name):
    print("hello  "+name+" !")

dag=DAG(dag_id="xcom_testing_v1",
        start_date=datetime(2023,7,7),
        schedule_interval=None)


task1=PythonOperator(task_id="task1",
                     python_callable=test,
                     op_kwargs={"name":"hari"},
                     dag=dag)


task2=BashOperator(task_id="task2",bash_command="echo hello, this is bash command", dag=dag)

task2.set_upstream(task1)