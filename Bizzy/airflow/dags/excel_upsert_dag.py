import pandas as pd
import sys
import boto3
import glob
import os, time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.redshift_load_csv_plugin_production import S3ToRedshiftCsvOperatorProd
from airflow.operators.redshift_csv_upsert_plugin_production import RedshiftCsvUpsertOperatorProd

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

dag = DAG('excel_upsert_dag', default_args=default_args, schedule_interval=None)

s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket='prod-datalake-raw-data',Prefix='excel-datasource/upsert-dag/excel-file')
all = response['Contents']
latest = max(all, key=lambda x: x['LastModified'])
res = latest['Key'].split('/',3)
result = res[3]

def get_xl_from_s3(bucketName,xlPath,csvPath,xlName,**kwargs):
    print("Getting {0} excel from the {1} bucket".format(xlPath+"/"+xlName,bucketName))
    # Download the xlsx file from S3 to the local
    s3_client.download_file(bucketName, xlPath+"/"+xlName,xlName)	
    # s3_client.upload_file('hello.txt', 'MyBucket', 'hello-remote.txt')
    upload_csv_to_s3(bucketName,csvPath,xl_to_csv_parse(xlName))

def xl_to_csv_parse(xlWithPath):
    print("xl to csv parsing program started")
    print("inputXlFilePath: "+xlWithPath)
    xls = pd.ExcelFile(xlWithPath)
    csvFileList=[]
	
    # listing all sheets in xlsx file
    for sheets in xls.sheet_names:
        print("Sheet Name:"+sheets)
        out_csv_file= xlWithPath.split('.')[0]+"_"+sheets+".csv"
        print("outputCsv: "+out_csv_file)
        read_file=pd.read_excel(xlWithPath, sheet_name=sheets)
        read_file.to_csv (out_csv_file, index = None, header=True)
        csvFileList.append(out_csv_file)
    return csvFileList

def upload_csv_to_s3(bucketName,csvPath,csvFileList):
    s3_client = boto3.client('s3')
    # s3_client.upload_file('hello.txt', 'MyBucket', 'hello-remote.txt')
    for csv in csvFileList:
        print("Uploading {0} csv to the {1} s3 path".format(csvPath+"/"+csv,bucketName))
        s3_client.upload_file(csv,bucketName,csvPath+"/"+csv)
        print(csv)

[os.remove(file) for file in glob.glob("*.csv")]
[os.remove(file) for file in glob.glob("*.xlsx")]

test_task = PythonOperator(
    task_id='convert_xls_to_csv',
    python_callable=get_xl_from_s3,
    execution_timeout=timedelta(hours=2),
    dag=dag,
    provide_context=True,
    op_kwargs={
        'bucketName': 'prod-datalake-raw-data',
        'xlPath':'excel-datasource/upsert-dag/excel-file',
        'xlName':result,
        'csvPath':'excel-datasource/upsert-dag/csv-file'
    })

response_csv = s3_client.list_objects_v2(Bucket='prod-datalake-raw-data',Prefix='excel-datasource/upsert-dag/csv-file')
all = response_csv['Contents']
latest_csv = max(all, key=lambda x: x['LastModified'])
res_csv = latest_csv['Key'].split('/',3)
result_csv = res_csv[3]

csv_file_name = "excel-datasource/upsert-dag/csv-file/"+result_csv

def load(number,columns, **kwargs):
    return S3ToRedshiftCsvOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=csv_file_name,
        region="ap-southeast-1",
        op_kwargs={ "columns":columns,"cdc_prefix":"excel_testing_cdc","temp_prefix":"excel_testing_temp","main_prefix":"excel_testing_cdc","iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
        dag=dag)

def upsert(tablename,columnname,primary_key_column, **kwargs):
    return RedshiftCsvUpsertOperatorProd(
        task_id='upsert_{}'.format(tablename),
        src_redshift_conn_id="prod_redshift_connection",
        dest_redshift_conn_id="prod_redshift_connection",
        src_table=tablename,
        dest_table=tablename,
        column_name=columnname,
        src_keys=primary_key_column,
        dest_keys=primary_key_column,
        op_kwargs={"cdc_prefix":"excel_testing_cdc","temp_prefix":"excel_testing_temp","main_prefix":"excel_testing"},
        dag = dag)

schema_details_query = """
select * from schema_details where db_name='excel_testing' and table_name='employee_details'
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
#    load(record[2]) >>  upsert(record[2], record[3], record[4])
    test_task >> load(record[2],record[3]) >>  upsert(record[2], record[3], record[4])

""" for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) """
