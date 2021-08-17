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
import keras
import tensorflow as tf

glueContext = GlueContext(SparkContext.getOrCreate())

medicare = glueContext.read.format(
   "com.databricks.spark.csv").option(
   "header", "true").option(
   "inferSchema", "true").load(
   's3://testing-input-output-bucket/input/tripdata.csv')

medicare.printSchema()

support_testing()

nyTaxi = glueContext.read.option("inferSchema", "true").option("header", "true").csv("s3://testing-input-output-bucket/input/tripdata.csv")


updatedNYTaxi = nyTaxi.withColumn("current_date", lit(datetime.now()))

updatedNYTaxi.printSchema()

print(updatedNYTaxi.show())

print("Total number of records: " + str(updatedNYTaxi.count()))

updatedNYTaxi.write.csv("s3://testing-input-output-bucket/input/optripdata.csv")
