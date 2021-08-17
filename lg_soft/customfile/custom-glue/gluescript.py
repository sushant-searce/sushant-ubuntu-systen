import sys
import sys
from datetime import datetime
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from io import StringIO
import pandas as pd
from awsglue.transforms import *
from support import support_testing
from module.searce import searce
import flask
from bs4 import BeautifulSoup

args = getResolvedOptions(sys.argv, ['inputfile', 'outputfile', 'somevariable'])

ip = args['inputfile']
op = args['outputfile']
print(ip)
print(op)

glueContext = GlueContext(SparkContext.getOrCreate())

medicare = glueContext.read.format(
   "com.databricks.spark.csv").option(
   "header", "true").option(
   "inferSchema", "true").load(
   's3://testing-input-output-bucket/input/tripdata.csv')

support_testing()
searce()
medicare.printSchema()

nyTaxi = glueContext.read.option("inferSchema", "true").option("header", "true").csv(ip)

updatedNYTaxi = nyTaxi.withColumn("current_date", lit(datetime.now()))

updatedNYTaxi.printSchema()

print(updatedNYTaxi.show())

print("Total number of records: " + str(updatedNYTaxi.count()))

updatedNYTaxi.write.csv(op)
s3_location = op

name_bucket = s3_location.split('/')[2]
name_key = '/'.join(s3_location.split('/')[3:])

file_loc_name = '/'.join(s3_location.split('/')[3:])

client = boto3.client('s3')
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(name_bucket)
s3_key = '/'.join(name_key)

for object_summary in my_bucket.objects.filter(Prefix=name_key):
    a = object_summary.key
    result = a.startswith(name_key+"/part")#(name_key+'/part')
    if result==True:
        file_name = a
        print(file_name)
        print(file_name)

copy_source = {
    'Bucket': name_bucket,
    'Key': file_name
}

s3.meta.client.copy(copy_source, name_bucket , file_loc_name)
