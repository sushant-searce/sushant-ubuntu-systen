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

prefix_s3_path = "debezium-connector/prod-crowdsys-apps-farmer-cluster/prod-crowdsys-apps-farmer-cluster.farmerdb_prod."
                  
s3_past_two_hour_path=lastTwoHourS3Path

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 13),
    'email': ['taufiq.ibrahim@bizzy.co.id'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('BFFfarmerdbAirflow', default_args=default_args, schedule_interval='@hourly')


def load(number, **kwargs):
    return S3ToRedshiftOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=lastHourS3Path,
        region="ap-southeast-1",
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"farmerdb_prod_cdc","temp_prefix":"farmerdb_prod_temp","main_prefix":"farmerdb_prod", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
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
        op_kwargs={"cdc_prefix":"farmerdb_prod_cdc","temp_prefix":"farmerdb_prod_temp","main_prefix":"farmerdb_prod"},
        dag = dag)

schema_details_query = """
select * from schema_details where db_name='farmerdb_prod' and table_name in ('answer_type','cash_in_transit','customer_order','noo_address','noo_image','noo_profile','order_detail','order_process','prod_uom_detail','stock_taking','stock_taking_detail','survey','survey_answer','survey_question')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) >>  upsert(record[2], record[3], record[4])
