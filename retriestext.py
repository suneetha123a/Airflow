# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# def test(name):
#     print("hello  "+name+" !")

# def greet():
#     return "returning xcom value"

# dag=DAG(dag_id="retry",
#         start_date=datetime(2023,7,7),
#         schedule_interval=None)


# task1=PythonOperator(task_id="task1",
#                      python_callable=test,
#                      dag=dag,
#                      retries=3,
#                      retry_delay=60)

# task2=PythonOperator(task_id="task2",
#                      python_callable=greet,
#                      dag=dag)


# task3=BashOperator(task_id="task3",bash_command="echo hello, this is bash command", dag=dag)

# task1>>task2
# task1>>task3


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from random import randint

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 3,                 # Maximum number of retries for the task
    'retry_delay': timedelta(seconds=30),  # Delay between retry attempts (30 seconds)
}

# Define the DAG
dag = DAG(
    'retry',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
    catchup=False,                       # Skip past DAG runs on startup
)

# Define a simple Python function that generates a random number and checks if it's less than the threshold
def my_task_function():
    threshold = 70
    number = randint(1, 100)
    print(f"Generated number: {number}")
    
    if number < threshold:
        raise ValueError(f"Number is less than the threshold ({threshold})!")

# Define the task using PythonOperator, which executes the Python function
my_task = PythonOperator(
    task_id='my_task',
    python_callable=my_task_function,
    dag=dag,
)

# Set the task dependencies (no dependencies in this simple example)
my_task
