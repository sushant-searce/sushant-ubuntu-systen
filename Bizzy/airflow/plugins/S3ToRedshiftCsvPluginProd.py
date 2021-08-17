import logging
import boto3
from botocore.errorfactory import ClientError
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os, time
from datetime import datetime, timedelta
from pytz import timezone

s3Client = boto3.client('s3')

log = logging.getLogger(__name__)

class S3ToRedshiftCsvOperatorProd(BaseOperator):
    # template_fields = ('imtest')
    """
    Executes a LOAD command on a s3 CSV file into a Redshift table
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param table: reference to a specific table in redshift database
    :type table: string
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: string
    :param s3_access_key_id: reference to a specific S3 key
    :type s3_key: string
    :param s3_secret_access_key: reference to a specific S3 key
    :type s3_key: string
    :param delimiter: delimiter for CSV data
    :type s3_key: string
    :param region: location of the s3 bucket (eg. 'eu-central-1' or 'us-east-1')
    :type s3_key: string
    """
    @apply_defaults

    def __init__(self, redshift_conn_id,table,s3_bucket,s3_path,region,*args, **kwargs):

        # print("hello")
        # print(args)
        # print(kwargs)
        # print(self.imtest)
        # print(self.parameters)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.columns=kwargs['op_kwargs']['columns']
        self.cdc_prefix=kwargs['op_kwargs']['cdc_prefix']
        self.temp_prefix=kwargs['op_kwargs']['temp_prefix']
        self.main_prefix=kwargs['op_kwargs']['main_prefix']
        self.iam_role = kwargs['op_kwargs']['iam_role']

        super(S3ToRedshiftCsvOperatorProd, self).__init__(*args, **kwargs)

    def execute(self, context):

        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        log.info("Connected with " + self.redshift_conn_id)
        load_statement = "truncate "+self.main_prefix+'.'+self.table+" ; ;"
#        load_statement = "truncate "+self.cdc_prefix+'.'+self.table+" ; ;"
#        load_statement = "truncate "+self.temp_prefix+'.'+self.table+" ; ;"
        cursor.execute(load_statement)
        cursor.close()
        conn.commit()
        s3_path = self.s3_path

        try:
            response = s3Client.list_objects_v2(
                Bucket=self.s3_bucket,
                MaxKeys=1,
                Prefix=s3_path
            )
            mystring = self.columns
            mystringArr = mystring.split(",")
            myjoinString = ""
            for key in mystringArr:
                myjoinString += "\""+key+"\", "

            if(response['Contents']):
                conn = self.hook.get_conn()
                cursor = conn.cursor()
                load_statement = """
                    copy
                    {0}({1})
                    from 's3://{2}/{3}'
                    iam_role '{4}'
                    csv ignoreheader 1 timeformat 'YYYY-MM-DDTHH:MI:SS' """.format(
                self.main_prefix+"."+self.table,myjoinString[:-2], self.s3_bucket, s3_path, self.iam_role)
#                print(load_statement)
                cursor.execute(load_statement)
                cursor.close()
                conn.commit()
                log.info("Load command completed for last two hour "+s3_path)

        except Exception as ex:
            print(ex)
            log.info(self.s3_bucket+"/"+s3_path+" does not exits")

        return True

class S3ToRedshiftCsvOperatorPluginProd(AirflowPlugin):
    name = "redshift_load_csv_plugin_production"
    operators = [S3ToRedshiftCsvOperatorProd]
