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


lastTwoHourDateTime = datetime.now() - timedelta(hours = 2)
# lastTwoHourS3Path = lastTwoHourDateTime.strftime('year=%Y/month=%m/day=%d')
lastTwoHourS3Path = lastTwoHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

lastHourDateTime = datetime.now() - timedelta(hours = 1)
lastHourS3Path = lastHourDateTime.strftime('year=%Y/month=%m/day=%d')
#lastHourS3Path = lastHourDateTime.strftime('year=%Y')

# "debezium-connector/prod-aurora-cluster/prod-aurora-cluster.crowdSysProd."+number+"/"+lastHourS3Path
prefix_s3_path = "debezium-connector/prod-aurora-cluster/prod-aurora-cluster.crowdSysProd."

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

dag = DAG('CrowdSysProdAirflow', default_args=default_args, schedule_interval='@hourly')


def load(number, **kwargs):
    return S3ToRedshiftOperatorProd(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=lastHourS3Path,
        region="ap-southeast-1",
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "cdc_prefix":"crowdsysprod_cdc","temp_prefix":"crowdsysprod_temp","main_prefix":"crowdsysprod", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
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
        op_kwargs={"cdc_prefix":"crowdsysprod_cdc","temp_prefix":"crowdsysprod_temp","main_prefix":"crowdsysprod"},
        dag = dag)

schema_details_query = """
select * from schema_details where db_name='crowdSysProd' and table_name in ('tokosmart_history','log_customer','notification', 'customer_deposit','customer_order','push','customer_balance','customer_deposit_history','customer_device','customer_distributor','customer_ext_info','customer_image','customer_sales_noo','customer_whitelist','distributor','electronic_wallet','electronic_wallet_payment','electronic_wallet_payment_detail','extdist_product','extdist_product_availability','extdist_product_component_combo','extdist_product_discount','extdist_product_discount_structure','extdist_product_parent_combo','extdist_product_price','extdist_product_uom_detail','images','login','noo_mapping','noo_profile','noo_region_sd_matrix','noo_registration','noo_salesman_matrix','noo_sales_branch_matrix','oauth_access_tokens','oauth_authorization_codes','oauth_clients','oauth_refresh_tokens','oauth_scopes','order_detail','order_detail_history','order_payment_method','order_payment_methoddetail','order_status_history','order_status_master','otp','popular_brand','popular_product','principal_matrix','product','product_availability','product_blacklist','product_brand','product_brand_image','product_category','product_category_image','product_component_combo','product_geography','product_image','product_parent_combo','product_price','product_subcategory','product_subcategory_image','product_temp','referral','referral_setting','reset_pin','salesman','system_settings','tokosmart_config','tokosmart_config_schedule','tokosmart_points','va_discount','va_payment','va_payment_detail','va_payment_retry')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}

for record in myDatabaseHook.get_records(schema_details_query):
    load(record[2]) >>  upsert(record[2], record[3], record[4])

#for record in myDatabaseHook.get_records(schema_details_query):
#    upsert(record[2], record[3], record[4])
