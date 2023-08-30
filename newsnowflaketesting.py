from airflow import DAG
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 8),
    
}

dag = DAG(
    'create_snowflake_table',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
)

# Define the task to create the table
create_table_task = SnowflakeOperator(
    task_id='create_table',
    sql="""
    CREATE OR REPLACE TABLE your_schema.your_table (
        column1 VARCHAR,
        column2 INT,
        column3 DATE
    )
    COMMENT = 'Your table comment'
    """,
    snowflake_conn_id='testing',
    autocommit=True,
    dag=dag,
)

# Set task dependencies if needed
# task_a >> create_table_task >> task_b
