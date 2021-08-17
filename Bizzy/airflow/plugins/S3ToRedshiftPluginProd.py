import logging

import boto3

from botocore.errorfactory import ClientError

from airflow.hooks.postgres_hook import PostgresHook

from airflow.plugins_manager import AirflowPlugin

from airflow.models import BaseOperator

from airflow.utils.decorators import apply_defaults

import os, time

import re

from datetime import datetime, timedelta

from pytz import timezone







s3Client = boto3.client('s3')



log = logging.getLogger(__name__)



class S3ToRedshiftOperatorProd(BaseOperator):

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

        self.s3_past_two_hour_path=kwargs['op_kwargs']['s3_past_two_hour_path']

        self.cdc_prefix=kwargs['op_kwargs']['cdc_prefix']

        self.temp_prefix=kwargs['op_kwargs']['temp_prefix']

        self.main_prefix=kwargs['op_kwargs']['main_prefix']

        self.prefix_s3_path=kwargs['op_kwargs']['prefix_s3_path']

        self.iam_role = kwargs['op_kwargs']['iam_role']

        super(S3ToRedshiftOperatorProd, self).__init__(*args, **kwargs)



    def execute(self, context):

        

        org_ts = context['ts']

        temp_ts = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2}[A-Z]{1}[0-9]{2}:[0-9]{2}:[0-9]{2})", org_ts)

        ts = temp_ts[0]

        date_str = ts

        datetime_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")

        datetime_obj_utc = datetime_obj.replace(tzinfo=timezone('UTC'))

        

        # Convert to Asia/Jakarta time zone

        now_pacific = datetime_obj_utc.astimezone(timezone('Asia/Jakarta'))

        lastHourDateTime = now_pacific - timedelta(hours = 1)

        lastHourS3Path = lastHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

        #print(lastHourS3Path)

        

        lastTwoHourDateTime = now_pacific - timedelta(hours = 2)

        lastTwoHourS3Path = lastTwoHourDateTime.strftime('year=%Y/month=%m/day=%d/hour=%H')

        #print(lastTwoHourS3Path)

        

        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        conn = self.hook.get_conn() 

        cursor = conn.cursor()

        log.info("Connected with " + self.redshift_conn_id)

        load_statement = "truncate "+self.cdc_prefix+'.'+self.table+" ; ;"

        load_statement = "truncate "+self.temp_prefix+'.'+self.table+" ; ;"

        cursor.execute(load_statement)

        cursor.close()

        conn.commit()



        s3_past_two_hour_path = self.prefix_s3_path+self.table+"/"+lastTwoHourS3Path



        s3_path = self.prefix_s3_path+self.table+"/"+lastHourS3Path



        print(s3_past_two_hour_path)

        print(s3_path)



        try:



            response = s3Client.list_objects_v2(

                Bucket=self.s3_bucket,

                MaxKeys=1,

                Prefix=s3_past_two_hour_path

            )



            if 'Contents' in response:

                conn = self.hook.get_conn()

                cursor = conn.cursor()      

                s3_copy_format = 'auto'



                if self.table == "idspct_trans":

                    s3_copy_format = 's3://prod-datalake-raw-data/json-path-files/prod-IDSP-controltower-cluster-IDSPcontroltowerdb_prod-idspct_trans.json'                

                load_statement = """

                    copy

                    {0}

                    from 's3://{1}/{2}'

                    iam_role '{3}'

                    json '{4}' GZIP timeformat 'YYYY-MM-DDTHH:MI:SS' """.format(

                self.cdc_prefix+"."+self.table, self.s3_bucket, s3_past_two_hour_path, self.iam_role, s3_copy_format)



                print(load_statement)

        

                cursor.execute(load_statement)

                cursor.close()

                conn.commit()

                log.info("Load command completed for last two hour "+s3_past_two_hour_path)



        except Exception as ex:

            print(ex)

            log.info(self.s3_bucket+"/"+s3_past_two_hour_path+" does not exits")





        try:

            responseTwo = s3Client.list_objects_v2(

                Bucket=self.s3_bucket,

                MaxKeys=1,

                Prefix=s3_path

            )



            if 'Contents' in responseTwo:

                conn = self.hook.get_conn()

                cursor = conn.cursor()

                s3_copy_format = 'auto'



                if self.table == "idspct_trans":

                    s3_copy_format = 's3://prod-datalake-raw-data/json-path-files/prod-IDSP-controltower-cluster-IDSPcontroltowerdb_prod-idspct_trans.json'

                

                load_statement = """

                    copy

                    {0}

                    from 's3://{1}/{2}'

                    iam_role '{3}'

                    json '{4}' GZIP timeformat 'YYYY-MM-DDTHH:MI:SS' """.format(

                self.cdc_prefix+"."+self.table, self.s3_bucket, s3_path, self.iam_role, s3_copy_format)

                print(load_statement)

                cursor.execute(load_statement)

                cursor.close()

                conn.commit()

                log.info("Load command completed for last hour")



        except Exception as ex:

            print(ex)

            log.info(self.s3_bucket+"/"+s3_path+" does not exits")



        return True



class S3ToRedshiftOperatorPluginProd(AirflowPlugin):

    name = "redshift_load_plugin_production"

    operators = [S3ToRedshiftOperatorProd]
