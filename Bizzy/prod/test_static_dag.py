import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.redshift_upsert_plugin import RedshiftUpsertOperator
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('airflow', default_args=default_args, schedule_interval=timedelta(days=1))

upsert = RedshiftUpsertOperator(
  task_id='upsert',
  src_redshift_conn_id="my_redshift",
  dest_redshift_conn_id="my_redshift",
  src_table="customer_balance",
  dest_table="prod_customer_balance",
  src_keys=["id"],
  dest_keys=["id"],
  dag = dag
)

load = S3ToRedshiftOperator(
  task_id="load",
  redshift_conn_id="my_redshift",
  table="customer_balance",
  s3_bucket="sushant-bizzy-testing",
  s3_path="debezium/",
  region="us-east-1",
  dag=dag
)

load >> upsert
