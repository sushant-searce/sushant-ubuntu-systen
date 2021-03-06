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

prefix_s3_path = "debezium-connector/prod-masterdata-mdm-cluster/prod-masterdata-mdm-cluster.masterdatadb_prod."

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

dag = DAG('MasterdataMDMAirflow', default_args=default_args, schedule_interval='@hourly')


def load(number, **kwargs):
    return S3ToRedshiftOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=lastHourS3Path,
        region="ap-southeast-1",
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"masterdatadb_prod_cdc","temp_prefix":"masterdatadb_prod_temp","main_prefix":"masterdatadb_prod", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
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
        op_kwargs={"cdc_prefix":"masterdatadb_prod_cdc","temp_prefix":"masterdatadb_prod_temp","main_prefix":"masterdatadb_prod"},
        dag = dag)

schema_details_query = """
select * from schema_details where db_name='masterdatadb_prod' and table_name in ('bdp_cl_custcreditlimit','bdp_md_bank','bdp_md_bankpaymentguide','bdp_md_city','bdp_md_company','bdp_md_country','bdp_md_custcl','bdp_md_custcldetail1','bdp_md_custcldetail2','bdp_md_custdistchannel','bdp_md_custgroup','bdp_md_custgroup1','bdp_md_custgroup1_detail','bdp_md_custgroup2','bdp_md_custgroup2_detail','bdp_md_custgroup3','bdp_md_custgroup3_detail','bdp_md_custgroup4','bdp_md_custgroup4_detail','bdp_md_custgroup5','bdp_md_custgroup5_detail','bdp_md_custgroupmatrix','bdp_md_custimage','bdp_md_customer','bdp_md_customer_info','bdp_md_customer_vehicle_type','bdp_md_custsales','bdp_md_custshipto','bdp_md_custsubdistchannel','bdp_md_custtop','bdp_md_custtopdetail1','bdp_md_custtopdetail2','bdp_md_distributor','bdp_md_district','bdp_md_docnumber','bdp_md_docnumber_detail','bdp_md_docnumber_format','bdp_md_docobject','bdp_md_docsource','bdp_md_doctype','bdp_md_drivers','bdp_md_employee','bdp_md_empprincipalmatrix','bdp_md_emptype','bdp_md_emptypedetail','bdp_md_images','bdp_md_incoterm','bdp_md_paymentgateway','bdp_md_paymentmethod','bdp_md_paymentmethoddetail','bdp_md_postalcode','bdp_md_prodavailability','bdp_md_prodcategory1','bdp_md_prodcategory2','bdp_md_prodcategory3','bdp_md_prodcategory4','bdp_md_prodcategory5','bdp_md_prodcategory6','bdp_md_prodcategory7','bdp_md_prodcombocomp','bdp_md_prodcomboparent','bdp_md_proddiscount','bdp_md_prodhierarchy','bdp_md_prodhierarchy1','bdp_md_prodhierarchy2','bdp_md_prodhierarchy3','bdp_md_prodhierarchy4','bdp_md_prodhierarchy5','bdp_md_prodhierarchymatrix','bdp_md_prodimage','bdp_md_prodprice','bdp_md_prodpurchase','bdp_md_prodspecification','bdp_md_prodspecification_copy','bdp_md_produom','bdp_md_produomcomposite','bdp_md_produomcomposite_detail','bdp_md_produomconv','bdp_md_province','bdp_md_purchase_group','bdp_md_purchase_organization','bdp_md_reason','bdp_md_reasondetail','bdp_md_sales_organization','bdp_md_salesoffice','bdp_md_soffregion','bdp_md_sotype','bdp_md_storage_location','bdp_md_top','bdp_md_vehicle','bdp_md_vehicletype','bdp_md_village','bdp_md_zdbb_matrix','branches','customer_attributes','customers','digital_products_additional_details','digital_response_codes','digital_telco_operators','digital_telco_prefix','digital_transactions_ack','digital_transactions_request','driver_attributes','driver_branches','drivers','vehicle_attributes','vehicles')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) >>  upsert(record[2], record[3], record[4])
