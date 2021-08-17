import airflow
import os
import os, time
import pprint
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mssql_plugin import MsSQLToS3Operator
from airflow.operators.redshift_load_plugin_mssql import S3ToRedshiftOperatorProduction
from airflow.operators.redshift_upsert_plugin_mssql import RedshiftUpsertOperatorMssql
from datetime import datetime, timedelta
from textwrap import dedent


os.environ['TZ'] = 'Asia/Jakarta'
time.tzset()


lastHourDateTime = datetime.now() - timedelta(hours = 1)
lastHourS3Path = lastHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')


lastTwoHourDateTime = datetime.now() - timedelta(hours = 2)

lastTwoHourS3Path = lastTwoHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

# "debezium-connector/prod-aurora-cluster/prod-aurora-cluster.crowdSysProd."+number+"/"+lastHourS3Path
prefix_s3_path = "cargowise_data/"

s3_past_two_hour_path=lastHourS3Path


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 28),
    }
    
    
dag = DAG('cargowise-batch', default_args=default_args,schedule_interval=None)

def Mssql_to_s3(tname,pk_column,**kwargs):
    return MsSQLToS3Operator(
        task_id='mssql_to_s3_{}'.format(tname),
        mssql_conn_id='mssql-connection',
        mssql_table=tname,
        s3_conn_id='s3_connection',
        s3_bucket='prod-datalake-raw-data',
        s3_key=prefix_s3_path+tname+"/"+lastHourS3Path+"/"+tname+".json",
        primary_key=pk_column,
        batchsize=50000,
        package_schema=False,
       # incremental_key=incremental_key_column,
       # start='YYYY-MM-DD HH:MI:SS',
       # end='YYYY-MM-DD HH:MI:SS',
        #start='2020-02-15 00:00:00',
        #end='2020-02-17 00:00:00',
        dag=dag)
         

def load(number, **kwargs):
    return S3ToRedshiftOperatorProduction(
        task_id='load_{}'.format(number),
        redshift_conn_id="prod_redshift_connection",
        table=number,
        s3_bucket="prod-datalake-raw-data",
        s3_path=lastHourS3Path,
        region="ap-southeast-1",
        op_kwargs={"s3_past_two_hour_path":s3_past_two_hour_path, "main_prefix":"odysseyptbprd", "prefix_s3_path":prefix_s3_path, "iam_role":"arn:aws:iam::593305529235:role/s3-redshift-role"},
        dag=dag)


def upsert(tablename,**kwargs):
    return RedshiftUpsertOperatorMssql(
        task_id='upsert_{}'.format(tablename),
        src_redshift_conn_id="prod_redshift_connection",
        dest_redshift_conn_id="prod_redshift_connection",
        src_table=tablename,
        op_kwargs={"main_prefix":"odysseyptbprd"},
        dag = dag)


schema_details_query = """
select * from cargowise_table where db_name='OdysseyPTBPRD' and table_name in ('AccChargeCode','AccGLAggregate','AccGLHeader','AccGroups','AccTransactionHeader','AccTransactionLines','AccTransactionMatchLink','DtbBooking','DtbBookingConfirmation','DtbBookingConsolidation','DtbBookingInstruction','DtbConsignmentRunSheetInstruction','GenCustomAddOnValue','GlbBranch','GlbCompany','GlbDepartment','GlbStaff','JobBookedCtgMove','JobCartage','JobChargeRevRecognition','JobConShipLink','JobConsol','JobConsolTransport','JobContainer','JobContainerDetention','JobContainerPackPivot','JobDocAddress','JobHeader','JobMawb','JobPackLines','JobSailing','JobShipment','JobStorage','JobSundryCharges','JobVoyAccount','JobVoyage','JobVoyDestination','JobVoyOrigin','OrgAddress','OrgHeader','OrgMiscServ','OrgPartCategory','OrgPartRelation','OrgPartUnit','OrgRelatedParty','OrgSupplierPart','ProcessTasks','RefCommodityCode','RefContainer','RefCurrency','StmALog','StmNote','vw_JobDocAddress','vw_Report_ProductInfo','WhsDocket','WhsDocketCustomAttributes','WhsDocketLine','WhsDocketLineCustomAttributes','WhsDocketReference','WhsLocation','WhsLocationView','WhsPick','WhsPickLine','WhsProductStyle','WhsProductStyleColour','WhsReleaseCapturedAttributes','WhsRow','WhsWarehouse')
"""

myDatabaseHook = PostgresHook(postgres_conn_id='prod_postgres_connection')
resultCounter = {}


for record in myDatabaseHook.get_records(schema_details_query):
    Mssql_to_s3(record[2],record[3]) >> upsert(record[2]) >> load(record[2])

