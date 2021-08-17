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

prefix_s3_path = "debezium-connector/prod-crowdsys-tms-cluster/prod-crowdsys-TMS-cluster.tmsdb_prod."

s3_past_two_hour_path=lastTwoHourS3Path

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

dag = DAG('TMS-dbAirflow', default_args=default_args, schedule_interval='@hourly')


def load(number, **kwargs):
    return S3ToRedshiftOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=lastHourS3Path,
        region="ap-southeast-1",
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"tmsdb_prod_cdc","temp_prefix":"tmsdb_prod_temp","main_prefix":"tmsdb_prod", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
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
        op_kwargs={"cdc_prefix":"tmsdb_prod_cdc","temp_prefix":"tmsdb_prod_temp","main_prefix":"tmsdb_prod"},
        dag = dag)

schema_details_query = """
select * from schema_details where db_name='tmsdb_prod' and table_name in ('bdp_tms_do_header','tms_do','bdp_tms_do_items','bdp_tms_order_receiver_header','bdp_tms_order_receiver_items','bdp_tms_order_receiver_item_prices','bdp_tms_order_receiver_item_stocks','tms_branch_matrix','tms_config','tms_data_sync','tms_do_bill_ref','tms_do_redelivery_status','tms_do_request_approval','tms_do_sales_order','tms_do_shipment_head','tms_do_shipment_item','tms_do_shipping_sequence','tms_driver','tms_driver_carton','tms_driver_drop_point','tms_driver_get_shipment_log','tms_driver_incentive_summary','tms_driver_location','tms_driver_rit','tms_driver_sms_log','tms_driver_submit_do','tms_driver_submit_item','tms_driver_submit_shipment','tms_email_notification_config','tms_master_rate_droppoint','tms_master_rate_qtyuom','tms_master_rate_rit','tms_ref_division','tms_ref_do_sequence','tms_ref_do_type','tms_ref_do_type_flow','tms_ref_master_status_granular','tms_ref_master_status_sap','tms_ref_payment_method','tms_ref_reason','tms_ref_shipment_cost_preset','tms_ref_so_type','tms_ref_submit_status','tms_rule_so_do_detail','tms_rule_so_do_header','tms_trans_dafin_submition','tms_trans_do_generated_ir','tms_trans_ir_save_do','tms_trans_ir_submit_sap','tms_trans_manual_routing','tms_trans_pod_check_head','tms_trans_pod_check_items','tms_trans_shipment_cost_head','tms_trans_shipment_cost_item')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) >>  upsert(record[2], record[3], record[4])
