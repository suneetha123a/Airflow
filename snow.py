from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python_operator import  PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
#from airflow.providers.operators.snowflake import SnowflakeQueryOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
   
}


# Snowflake connection details
# snowflake_conn = {
#     'account': 'tl69466',
#     'host':'https://tl69466.AWS_AP_SOUTH_1.snowflake.com',
#     'warehouse': 'WAREHOUSE',
#     'database': 'WHDATABASE',
#     'schema': 'SCHEMA',
#     'user': 'ACCOUNTADMIN',
#     'password': 'Hyderabad@001'
# }

dag = DAG(
    'snow',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

create_cost_table = """
    CREATE OR REPLACE TRANSIENT TABLE costtable1
        (
            id INT,
            land_damage_cost INT,
            property_damage_cost INT,
            lost_profits_cost INT
        );
"""
# insert_query="""
#      INSER INTO TABLE costtable1 VALUES
#      (
#         1,100000,4000000,200000
#      );
#      """
load_cost_data = """
    INSERT INTO costtable1 VALUES
        (1,150000,32000,10000),
        (2,200000,50000,50000),
        (3,90000,120000,300000),
        (4,230000,14000,7000),
        (5,98000,27000,48000),
        (6,72000,800000,0),
        (7,50000,2500000,0),
        (8,8000000,33000000,0),
        (9,6325000,450000,76000);
"""

with dag:
    # Define your Snowflake SQL query here

    # Use the SnowflakeQueryOperator to execute the SQL query
    snowflake_task1 = SnowflakeOperator(
        task_id='creating_a_table',
        sql=create_cost_table,
        snowflake_conn_id="snowflakecon"
    )
    
    snowflake_task2 = SnowflakeOperator(
        task_id="Insert_a_table",
        sql=load_cost_data,
        snowflake_conn_id="snowflakecon"
    )


# Set the order of the tasks
snowflake_task1>>snowflake_task2