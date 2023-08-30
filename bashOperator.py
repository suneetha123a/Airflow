from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

  
#step-2
default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2020,4,13),
    'retries':0


}

def greet():
    print("hello")

#step-3
dag=DAG(dag_id='py2_bash',default_args=default_args,catchup=False,schedule_interval='@once')



run_this = BashOperator(
    task_id="run_after_loop",
    bash_command="echo 1",
    dag=dag
)


task1 = PythonOperator(
    task_id="task1",
  
    python_callable=greet,
      dag=dag,
    )

run_this>>task1