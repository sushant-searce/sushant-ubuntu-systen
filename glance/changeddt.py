import sys
from datetime import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from pyspark.sql.types import StructType
from pyspark.sql.functions import col

spark = SparkSession\
    .builder\
    .appName("SparkETL")\
    .getOrCreate()

temp_schema = StructType([
    StructField("impressionType", StringType(), True),
    StructField("region", StringType(), True),
    StructField("stateId", StringType(), True),
    StructField("sdkVersion", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("clientTime", StringType(), True),
    StructField("eventName", StringType(), True),
    StructField("impressionId", StringType(), True),
    StructField("partnerId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("apiKey", StringType(), True),
    StructField("glanceId", StringType(), True),
    StructField("sessionId", StringType(), True),
    StructField("sessionMode", StringType(), True),
    StructField("requestTime", StringType(), True),
    StructField("cityId", StringType(), True),
    StructField("time", StringType(), True),
    StructField("source", StringType(), True),
    StructField("hour", StringType(), True),
    StructField("process_date", DateType(), True)
])

Data = spark.read.schema(temp_schema).format("json").load("gs://glanceaztogcspoc/analytics/bigquery-data/glance_shared/")
#Data = spark.read.format("json").load("gs://glanceaztogcspoc/analytics/bigquery-data/glance_shared/")
#Data = Data.withColumn("stateId", Data['stateId'].cast(IntegerType()))
#Data = Data.withColumn("sdkVersion", Data['sdkVersion'].cast(IntegerType()))
#Data = Data.withColumn("clientTime", Data['clientTime'].cast(LongType()))
#Data = Data.withColumn("requestTime", Data['requestTime'].cast(LongType()))
#Data = Data.withColumn("cityId", Data['cityId'].cast(IntegerType()))
#Data = Data.withColumn("time", Data['time'].cast(LongType()))
#Data = Data.withColumn("hour", Data['hour'].cast(IntegerType()))
#Data = Data.withColumn("process_date", Data['process_date'].cast(DateType()))

#ddf = df.withcolumn("stateId", df["stateId"].cast(IntegerType()))
#Data.printSchema()
#Data.show(1)

Data.printSchema()
Data.show(2)


Data.write.partitionBy("process_date").orc("gs://glanceaztogcspoc/analytics/searce-orc-hourly/hour_glance_sharedv1")
