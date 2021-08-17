import pandas as pd
import sys
import boto3
import glob
import os, time
import requests
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.redshift_load_csv_plugin_production import S3ToRedshiftCsvOperatorProd

os.environ['TZ'] = 'Asia/Jakarta'
time.tzset()

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 6),
    'email': ['taufiq.ibrahim@bizzy.co.id'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('ghseet_truncate_dag', default_args=default_args, schedule_interval=None)

s3_client = boto3.client('s3')

def gsheet_to_csv(bucketName,csvPath,**kwargs):
    response = requests.get('https://docs.google.com/spreadsheets/d/e/2PACX-1vQjUfgjqHbEZscGVomzz6Fzwx2CsFpAHLiQr8CsaZ9AntfISkTDoqJXQ6Wx7dFhCB4E5SbHbzfPJvPM/pub?gid=0&single=true&output=csv')
    
    assert response.status_code == 200, 'Wrong status code'

    with open('gsheet_output.csv', 'w') as f:
        writer = csv.writer(f)
        for line in response.iter_lines():
            writer.writerow(line.decode('utf-8').split(','))
    upload_csv_to_s3(bucketName,csvPath)

#upload_csv_to_s3(bucketName,csvPath)

def upload_csv_to_s3(bucketName,csvPath):
    print("Uploading {0} csv to the {1} s3 path".format(csvPath+"/gsheet_output.csv",bucketName))
    s3_client.upload_file('gsheet_output.csv',bucketName,csvPath+"/gsheet_output.csv")
    print('Csv file uploaded successfully!')



test_task = PythonOperator(
    task_id='gsheet_to_csv',
    python_callable=gsheet_to_csv,
    execution_timeout=timedelta(hours=2),
    dag=dag,
    provide_context=True,
    op_kwargs={
        'bucketName': 'prod-datalake-raw-data',
        'csvPath':'excel-datasource/gsheet-truncate-dag/csv-file'
    })

response_csv = s3_client.list_objects_v2(Bucket='prod-datalake-raw-data',Prefix='excel-datasource/gsheet-truncate-dag/csv-file')
all = response_csv['Contents']
latest_csv = max(all, key=lambda x: x['LastModified'])
res_csv = latest_csv['Key'].split('/',3)
result_csv = res_csv[3]

csv_file_name = "excel-datasource/gsheet-truncate-dag/csv-file/"+result_csv
[os.remove(file) for file in glob.glob("*.csv")]

def load(number,columns, **kwargs):
    return S3ToRedshiftCsvOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=csv_file_name,
        region="ap-southeast-1",
        op_kwargs={ "columns":columns,"cdc_prefix":"excel_testing_cdc","temp_prefix":"excel_testing_temp","main_prefix":"excel_testing","iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
        dag=dag)

schema_details_query = """
select * from schema_details where db_name='excel_testing' and table_name='employee_gsheet'
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
#    load(record[2]) >>  upsert(record[2], record[3], record[4])
    test_task >> load(record[2],record[3])

""" for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) """
