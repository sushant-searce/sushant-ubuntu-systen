from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import sys
from google.cloud import storage
from celery import Celery

app = Celery('list_blobs_with_prefix', broker='redis://10.181.13.81:6379/0')

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_python_operator',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


@app.task
def list_blobs_with_prefix(**kwargs):

    storage_client = storage.Client()
    bucket_name="sushant-fyusiob-dataset"
    prefix="IBUG"
    list_files = []

    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix
    )

    print("Blobs:")
    for blob in blobs:
    #    print(blob.name)
        list_files.append(blob.name)
    return list_files   

run_this = PythonOperator(
    task_id='list_the_files',
    provide_context=True,
    python_callable=list_blobs_with_prefix,
    dag=dag,
)

