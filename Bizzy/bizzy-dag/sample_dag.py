import os, time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.redshift_upsert_plugin import RedshiftUpsertOperator
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator


os.environ['TZ'] = 'Asia/Jakarta'
time.tzset()

lastHourDateTime = datetime.now() - timedelta(hours = 1)
lastHourS3Path = lastHourDateTime.strftime('year=%Y/month=%m/day=%d')
# lastHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('CrowdSysProdAirflow', default_args=default_args, schedule_interval="*/300 * * * *")


def load(number, **kwargs):
    dyn_value = "{{ task_instance.xcom_pull(task_ids='push_func') }}"
    return S3ToRedshiftOperator(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table='crowdsysprod_cdc.'+number,
        s3_bucket="prod-datalake-raw-data",
        s3_path="debezium-connector/prod-aurora-cluster/prod-aurora-cluster.crowdSysProd."+number+"/"+lastHourS3Path,
        region="ap-southeast-1",
        dag=dag)

def upsert(tablename,columnname,primary_key_column, **kwargs):
    dyn_value = "{{ task_instance.xcom_pull(task_ids='push_func_two') }}"
    return RedshiftUpsertOperator(
        task_id='upsert_{}'.format(tablename),
        src_redshift_conn_id="prod_redshift_connection",
        dest_redshift_conn_id="prod_redshift_connection",
        src_table=tablename,
        dest_table=tablename,
        column_name=[columnname],
        src_keys=primary_key_column,
        dest_keys=primary_key_column,
        dag = dag)

push_func = DummyOperator(
    task_id='push_func',
    dag=dag)

push_func_two = DummyOperator(
    task_id='push_func_two',
    dag=dag)

complete = DummyOperator(
    task_id='All_jobs_completed',
    dag=dag)

schema_details_query = """
select * from schema_details where table_name in ('notification', 'push', 'customer_deposit','customer_order')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}
for record in myDatabaseHook.get_records(schema_details_query):
    push_func >> load(record[2]) >> push_func_two >> upsert(record[2], record[3], record[4]) >> complete