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

prefix_s3_path = "debezium-connector/prod-idsp-controltower-cluster/prod-IDSP-controltower-cluster.IDSPcontroltowerdb_prod."
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

dag = DAG('IDSPcontroltowerAirflow', default_args=default_args, schedule_interval='@hourly')


def load(number, **kwargs):
    return S3ToRedshiftOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=lastHourS3Path,
        region="ap-southeast-1",
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"idspcontroltowerdb_prod_cdc","temp_prefix":"idspcontroltowerdb_prod_temp","main_prefix":"idspcontroltowerdb_prod", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
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
        op_kwargs={"cdc_prefix":"idspcontroltowerdb_prod_cdc","temp_prefix":"idspcontroltowerdb_prod_temp","main_prefix":"idspcontroltowerdb_prod"},
        dag = dag)

schema_details_query = """
select * from schema_details where db_name='IDSPcontroltowerdb_prod' and table_name in ('idspct_additional_module','idspct_deactivation_period_principal','idspct_discrepancy_qty','idspct_doc_num','idspct_doc_remarks','idspct_doi_limit','idspct_freeze_branches','idspct_gr_num','idspct_ims_expire_day','idspct_lead_time','idspct_master_contract_nt','idspct_master_gi','idspct_master_gi_products','idspct_master_gr','idspct_master_gr_products','idspct_master_po','idspct_master_pre_po_nt','idspct_master_qcf_nt','idspct_master_rfx_nt','idspct_master_sa','idspct_master_sc_nt','idspct_master_st','idspct_master_sto','idspct_master_sto_products','idspct_master_st_products','idspct_max_unloading','idspct_moq','idspct_movement_type','idspct_pareto_status','idspct_po_remarks','idspct_pre_po_nt_process_flow','idspct_pre_po_nt_process_flow_sequence','idspct_purchase_requisition','idspct_purchase_requisition_branch','idspct_purchase_requisition_detail','idspct_qcf_nt_products','idspct_reference_doc_type','idspct_rfx_nt_products','idspct_sa_products','idspct_sc_nt_products','idspct_stage','idspct_stock_allocation','idspct_stock_allocation_detail','idspct_stock_cogs','idspct_stock_level','idspct_stock_movement','idspct_stock_reservation','idspct_stock_reservation_detail','idspct_stock_status','idspct_sub_po','idspct_sub_po_products','idspct_trans','idspct_trans_type','idspct_unilever_po')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) >>  upsert(record[2], record[3], record[4])
