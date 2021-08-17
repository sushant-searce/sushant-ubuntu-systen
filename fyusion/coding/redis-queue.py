from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import redis
from google.cloud import storage
from celery import Celery


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example-redis-queue',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


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


def ABC(**kwargs):

    x = list_blobs_with_prefix(**kwargs)

    r = redis.StrictRedis(host='10.181.13.81', port=6379, db=0)
    # r.set('large_tasks','foo')

    # y = r.get('large_tasks')
    # print(y)    

    for i in x:
        # print(i)
        r.lpush('large_tasks', i)

    
    # while(r.llen('_kombu.binding.large_tasks')!=0):
    #     print(r.lpop('large_tasks'))


    # y = r.get('test_result_v1')
    # print("hello worlddddd")
    # # print(y)
    # for j in y:
    #     print(j)

run_this = PythonOperator(
    task_id='redis_files',
    provide_context=True,
    python_callable=ABC,
    dag=dag,
)