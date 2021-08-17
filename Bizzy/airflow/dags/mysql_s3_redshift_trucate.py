import os, time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.redshift_load_plugin_production_trunc import S3ToRedshiftOperatorProdTrunc
from airflow.operators.mysql_plugin import MySQLToS3Operator
from airflow.operators.dummy_operator import DummyOperator

conn = None

os.environ['TZ'] = 'Asia/Jakarta'
time.tzset()

lastHourDateTime = datetime.now() - timedelta(hours = 1)
lastHourS3Path = lastHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

lastTwoHourDateTime = datetime.now() - timedelta(hours = 2)

#lastHourS3Path = lastHourDateTime.strftime('year=%Y')

lastTwoHourS3Path = lastTwoHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

# "debezium-connector/prod-aurora-cluster/prod-aurora-cluster.crowdSysProd."+number+"/"+lastHourS3Path
prefix_s3_path = "mysql_S3_truncate-tables/"

s3_past_two_hour_path=lastHourS3Path


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 27),
 # 'email': ['taufiq.ibrahim@bizzy.co.id'],
 # 'email_on_failure': True,
 # 'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('mysql_s3_redshift-v1', default_args=default_args, schedule_interval=None)

def Mysql_to_s3(tname, conn, **kwargs):
    return MySQLToS3Operator(
        task_id='mysql_to_s3_{}'.format(tname),
        mysql_conn_id=conn,
        mysql_table=tname,
        s3_conn_id='s3_connection',
        s3_bucket='prod-datalake-raw-data',
        s3_key=prefix_s3_path+tname+"/"+lastHourS3Path+"/"+tname+".json",
        dag=dag
    )



def load(number,db_name,**kwargs):
    return S3ToRedshiftOperatorProdTrunc(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=prefix_s3_path,
        region="ap-southeast-1",
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"crowdsysprod_cdc","temp_prefix":"crowdsysprod_temp","main_prefix":db_name, "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
        dag=dag)

#schema_details_query = """
#select * from schema_details where db_name='crowdSysProd' and table_name in ('product_price')
#"""

schema_details_query = """
select * from schema_details where primary_key_column='truncate_tables'
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}


for record in myDatabaseHook.get_records(schema_details_query):
        if record[1]=='crowdSysProd':
           conn='tokosmart_mysql_conn'
        elif record[1]=='masterdatadb_prod':  
           conn='masterdatadb_prod_mysql_conn'
        elif record[1]=='so_mgt_portal_prod':
           conn='so_mgt_portal_prod_mysql_conn'
        Mysql_to_s3(record[2],conn) >> load(record[2],record[1])


#for record in myDatabaseHook.get_records(schema_details_query):
#    upsert(record[2], record[3], record[4])
