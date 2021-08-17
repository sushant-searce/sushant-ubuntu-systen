import sys
from datetime import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .appName("SparkETL")\
    .getOrCreate()

inpDF = spark.read.parquet("gs://glanceaztogcspoc/analytics/searce-parquet-hourly/glance_shared")
#inpDF = spark.read.orc("gs://glanceaztogcspoc/analytics/searcetest")
#ipDF = spark.read.json("gs://glanceaztogcspoc/analytics/bigquery-data/hour_glance_shared/myfile-000000000000.json")


inpDF.printSchema()
inpDF.show(5)