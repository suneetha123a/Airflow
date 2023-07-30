def print_args(*args):
    for arg in args:
        print(arg)

print_args('Hello', 'world', '!', 123)
# Output: Hello
#         world
#         !
#         123

############################## using ars #######################################
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the Python function to be called
def add_numbers(a, b):
    return a + b

# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

# Define the PythonOperator to call the function
add_numbers_task = PythonOperator(
    task_id='add_numbers_task',
    python_callable=add_numbers,
    op_args=[5, 3],  # Pass the arguments as a list
    dag=dag,
)
##################################using keyword operator######################################
def print_kwargs(**kwargs):
    for key, value in kwargs.items():
        print(f"{key}: {value}")

print_kwargs(name='Alice', age=30, city='New York')
# Output: name: Alice
#         age: 30
#         city: New York




##########################################################################
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_person_info(**kwargs):
    print("Person Information:")
    for key, value in kwargs.items():
        print(f"{key}: {value}")

# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag_with_kwargs',
    default_args=default_args,
    schedule_interval='@daily',
)

# Define the PythonOperator to call the function with kwargs
print_info_task = PythonOperator(
    task_id='print_info_task',
    python_callable=print_person_info,
    op_kwargs={
        'name': 'John Doe',
        'age': 35,
        'city': 'New York',
        'occupation': 'Engineer'
    },
    dag=dag,
)

