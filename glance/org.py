import sys
from datetime import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from pyspark.sql.types import StructType

spark = SparkSession\
    .builder\
    .appName("SparkETL")\
    .getOrCreate()



Data = spark.read.format("json").load("gs://glanceaztogcspoc/analytics/bigquery-data/glance_shared/")
Data.printSchema()
# Data.show()

df2= Data.withColumn("stateId",col("stateId").cast(IntegerType))

# .withColumn("sdkVersion",col("sdkVersion").cast(IntegerType)).withColumn("clientTime",col("clientTime").cast(IntegerType)).withColumn("requestTime",col("requestTime").cast(IntegerType)).withColumn("cityId",col("cityId").cast(IntegerType)).withColumn("time",col("time").cast(IntegerType)).withColumn("hour",col("hour").cast(IntegerType))

df2.printSchema()
df2.show()
#Data.write.partitionBy("process_date").orc("gs://glanceaztogcspoc/analytics/searce-orc-hourly/hour_glance_shared")

    # StructField("stateId", IntegerType(), True),
    # StructField("sdkVersion", IntegerType(), True),
    # StructField("clientTime", IntegerType(), True),
    # StructField("requestTime", IntegerType(), True),
    # StructField("cityId", IntegerType(), True),
    # StructField("time", IntegerType(), True),
    # StructField("hour", IntegerType(), True),