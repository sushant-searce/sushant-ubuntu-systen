import sys
from datetime import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .appName("SparkETL")\
    .getOrCreate()

inpDF = spark.read.json("gs://glanceaztogcspoc/analytics/json-hourly/glance_shared/")

inpDF.printSchema()

inpDF.write.orc("gs://glanceaztogcspoc/analytics/searce-orc-hourly/glance_shared")

