from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta

def generate_subdag(parent_dag_name, child_dag_name, args):
    subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        schedule_interval="@daily",
    )

    with subdag:
        # Define tasks within the SubDAG
        task1 = DummyOperator(task_id='task1')
        task2 = DummyOperator(task_id='task2')
        task3 = DummyOperator(task_id='task3')

        # Define task dependencies
        task1 >> task2 >> task3

    return subdag

# Define your main (outer) DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

main_dag = DAG(
    'my_main_dag',
    default_args=default_args,
    schedule_interval="@daily",
)

# Create the SubDAG as a single task in the main DAG
subdag_task = SubDagOperator(
    task_id='subdag_task',
    subdag=generate_subdag('my_main_dag', 'subdag_task', default_args),  # Corrected dag_id
    dag=main_dag,
)

# Define other standalone tasks
start_task = DummyOperator(task_id='start_task', dag=main_dag)
end_task = DummyOperator(task_id='end_task', dag=main_dag)

# Define the workflow
start_task >> subdag_task >> end_task
