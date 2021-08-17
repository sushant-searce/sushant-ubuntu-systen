import sys
from datetime import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession\
    .builder\
    .appName("SparkETL")\
    .getOrCreate()

Data = spark.read.format("orc").load("gs://glanceaztogcspoc/analytics/searce-orc-final-hour/peek_started/")

Data.write.partitionBy("apikey","process_date","hour").orc("gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/peek_started")
