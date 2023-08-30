# from datetime import timedelta
# from airflow import DAG
# from airflow.providers.microsoft.azure.operators.azure_blob_list import AzureBlobStorageListOperator
# from airflow.utils.dates import days_ago

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     }

# with DAG(
#     'blob_example',
#     default_args=default_args,
#     description='An example DAG to list blobs in Azure Blob storage',
#     schedule_interval=timedelta(days=1),
# ) as dag:

#     list_blobs = AzureBlobStorageListOperator(
#         task_id='list_blobs',
#         container_name='your-container-name',
#         conn_id='blobcon',
#     )

# list_blob
