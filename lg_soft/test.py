import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args['JOB_NAME'], args)
mysql_source_endpoint = "glue-database.c5gzadejgevo.us-east-1.rds.amazonaws.com"
mysql_source_port = "3306"
source_database = "source"
source_table = "student"
mysql_source_host_username = "admin"
mysql_source_host_password = "password"
s3_path_to_store_data = "s3://surya-test-bucket-2/rishabh/"
format_ = "csv"

connection_mysql5_options = {
    "url": "jdbc:mysql://{}:{}/{}".format(mysql_source_endpoint,mysql_source_port,source_database),
    "dbtable": "{}".format(source_table),
    "user": "{}".format(mysql_source_host_username),
    "password": "{}".format(mysql_source_host_password)}

datasource = glueContext.create_dynamic_frame.from_options(connection_type="mysql", connection_options=connection_mysql5_options)
dynamic_frame_with_less_partitions=datasource.coalesce(1)
datasink = glueContext.write_dynamic_frame.from_options(frame = dynamic_frame_with_less_partitions, connection_type ="s3", connection_options = {"path": s3_path_to_store_data}, format = format_, transformation_ctx = "datasink")
job.commit()