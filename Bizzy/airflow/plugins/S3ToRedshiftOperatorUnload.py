import logging



import boto3



from botocore.errorfactory import ClientError



from airflow.hooks.postgres_hook import PostgresHook



from airflow.plugins_manager import AirflowPlugin



from airflow.models import BaseOperator



from airflow.utils.decorators import apply_defaults



import os, time



from datetime import datetime, timedelta



s3Client = boto3.client('s3')



log = logging.getLogger(__name__)



class S3ToRedshiftOperatorUnload(BaseOperator):
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

    
        self.redshift_conn_id = redshift_conn_id



        self.table = table



        self.s3_bucket = s3_bucket



        self.s3_path = s3_path



        



        super(S3ToRedshiftOperatorUnload, self).__init__(*args, **kwargs)



    def execute(self, context):


        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # conn = self.hook.get_conn()
        # cursor = conn.cursor()
        
        log.info("Connected with " + self.redshift_conn_id)
        sql_statement = """



                    unload



                    ('select * from {0}')



                    to 's3://{1}/{2}'



                    iam_role '{3}'


                    allowoverwrite
                    
                    format as csv HEADER GZIP """.format(



                "crowdsysprod."+self.table, self.s3_bucket, self.s3_path+self.table, "arn:aws:iam::593305529235:role/s3-redshift-role")

            
        
        
        print(sql_statement)
        # cursor.execute(sql_statement)
        # cursor.close()
        # conn.commit()
        log.info("Unload command completed")

        return True



class S3ToRedshiftOperatorPluginUnload(AirflowPlugin):



    name = "redshift_unload_plugin_production"



    operators = [S3ToRedshiftOperatorUnload]
