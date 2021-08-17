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

# Data = spark.read.format("json").load("gs://glanceaztogcspoc/analytics/json-hourly/glance_started/")
# cols=["year","month","day"]
# Data = Data.withColumn("process_date",concat_ws("-",*cols).cast("date"))
Data = spark.read.format("json").load("gs://glanceaztogcspoc/analytics/json-hourly/glance_started/year=2021/month=06/day=23")
Data.printSchema()
Data.show(2)
# Data.write.partitionBy("process_date").parquet("gs://glanceaztogcspoc/analytics/searce-parquet-hourly/glance_started")