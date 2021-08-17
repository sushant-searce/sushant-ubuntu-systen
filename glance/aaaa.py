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

# Data = spark.read.schema(temp_schema).format("json").load("gs://glanceaztogcspoc/analytics/bigquery-data/glance_started/")
Data = spark.read.format("json").load("gs://glanceaztogcspoc/analytics/bigquery-data/glance_ended/")

Data = Data.withColumn("stateId", Data['stateId'].cast(IntegerType()))
Data = Data.withColumn("clientTime", Data['clientTime'].cast(LongType()))
Data = Data.withColumn("requestTime", Data['requestTime'].cast(LongType()))
Data = Data.withColumn("holdDuration", Data['holdDuration'].cast(LongType()))
Data = Data.withColumn("time", Data['time'].cast(LongType()))
Data = Data.withColumn("sdkVersion", Data['sdkVersion'].cast(IntegerType()))
Data = Data.withColumn("cityId", Data['cityId'].cast(IntegerType()))
Data = Data.withColumn("year", Data['year'].cast(IntegerType()))
Data = Data.withColumn("month", Data['month'].cast(IntegerType()))
Data = Data.withColumn("day", Data['day'].cast(IntegerType()))
Data = Data.withColumn("hour", Data['hour'].cast(IntegerType()))
Data = Data.withColumn("duration", Data['duration'].cast(LongType()))
Data = Data.withColumn("process_date", Data['process_date'].cast(DateType()))

#ddf = df.withcolumn("stateId", df["stateId"].cast(IntegerType()))
#Data.printSchema()
#Data.show(1)

Data.printSchema()
Data.show(2)


Data.write.partitionBy("process_date").parquet("gs://glanceaztogcspoc/analytics/searce-parquet-hourly/glance_ended")