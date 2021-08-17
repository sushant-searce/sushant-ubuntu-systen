import os, time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.redshift_load_plugin_production import S3ToRedshiftOperatorProd
from airflow.operators.redshift_upsert_plugin_production import RedshiftUpsertOperatorProd

os.environ['TZ'] = 'Asia/Jakarta'
time.tzset()

lastHourDateTime = datetime.now() - timedelta(hours = 1)
lastHourS3Path = lastHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

lastTwoHourDateTime = datetime.now() - timedelta(hours = 2)

lastTwoHourS3Path = lastTwoHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

# "debezium-connector/prod-aurora-cluster/prod-aurora-cluster.crowdSysProd."+number+"/"+lastHourS3Path
prefix_s3_path = "debezium-connector/prod-bizzy-distribution-crows/prod-bizzy-distribution-crows.so_mgt_portal_prod."

s3_past_two_hour_path=lastTwoHourS3Path
UNTIL = "{{ ds }}"
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 7),
    'email': ['taufiq.ibrahim@bizzy.co.id'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('SalesOrderMgnDBAirflow', default_args=default_args, schedule_interval='@hourly')

# op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"so_mgt_portal_prod_cdc","temp_prefix":"so_mgt_portal_prod_temp","main_prefix":"so_mgt_portal_prod", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role", "until":"""{{ execution_date }}"""},

def load(number, **kwargs):
    # print(kwargs)
    return S3ToRedshiftOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=lastHourS3Path,
        region="ap-southeast-1",
        provide_context=True,
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"so_mgt_portal_prod_cdc","temp_prefix":"so_mgt_portal_prod_temp","main_prefix":"so_mgt_portal_prod", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role", "until":"""{{ execution_date }}"""},
        dag=dag)

def upsert(tablename,columnname,primary_key_column, **kwargs):
    return RedshiftUpsertOperatorProd(
        task_id='upsert_{}'.format(tablename),
        src_redshift_conn_id="prod_redshift_connection",
        dest_redshift_conn_id="prod_redshift_connection",
        src_table=tablename,
        dest_table=tablename,
        column_name=columnname,
        src_keys=primary_key_column,
        dest_keys=primary_key_column,
        provide_context=True,
        op_kwargs={"cdc_prefix":"so_mgt_portal_prod_cdc","temp_prefix":"so_mgt_portal_prod_temp","main_prefix":"so_mgt_portal_prod"},
        dag = dag)

schema_details_query = """
select * from schema_details where db_name='so_mgt_portal_prod' and table_name in ('bdp_so_custorder','bdp_so_custorder_detail1','bdp_so_custorder_detail2','bdp_so_custorder_detail3','bdp_so_custorder_log','bdp_so_master_config','bdp_so_master_config_detail','bdp_so_po_group','bdp_so_uploadpo_config','bdp_so_uploadpo_config_detail')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) >>  upsert(record[2], record[3], record[4])

""" for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) """
