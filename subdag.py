from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

# Define the main DAG
dag = DAG(
    'main_dag',
    schedule_interval='@daily',
    start_date=datetime(2023, 7, 1),
    catchup=False
)

# Define a SubDAG
def create_subdag(parent_dag_name, child_dag_name, args):
    subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        schedule_interval=args['schedule_interval'],
        start_date=args['start_date'],
    )

    with subdag:
        # Define tasks for the SubDAG
        task1 = DummyOperator(task_id='task1')
        task2 = DummyOperator(task_id='task2')
        task1 >> task2  # Set the task dependency within the SubDAG

    return subdag

# Create a SubDagOperator and associate it with the main DAG
subdag_task = SubDagOperator(
    task_id='subdag_task',
    subdag=create_subdag('main_dag', 'subdag_task', {'schedule_interval': '@daily', 'start_date': datetime(2023, 7, 1)}),
    dag=dag
)

# Define other tasks in the main DAG
start_task = DummyOperator(task_id='start_task')
end_task = DummyOperator(task_id='end_task')

# Set up task dependencies within the main DAG
start_task >> subdag_task >> end_task
