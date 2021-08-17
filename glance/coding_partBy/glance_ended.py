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

Data = spark.read.format("json").load("gs://glanceaztogcspoc/analytics/json-hourly/glance_ended/")
cols=["year","month","day"]
Data = Data.withColumn("process_date",concat_ws("-",*cols).cast("date"))

DF = Data.withColumnRenamed("apiKey", "apikey").withColumnRenamed("cityId", "cityid").withColumnRenamed("clientTime", "clienttime").withColumnRenamed("duration", "duration").withColumnRenamed("eventName", "eventname").withColumnRenamed("glanceId", "glanceid").withColumnRenamed("holdDuration", "holdduration").withColumnRenamed("impressionId", "impressionid").withColumnRenamed("impressionType", "impressiontype").withColumnRenamed("locale", "locale").withColumnRenamed("partnerId", "partnerid").withColumnRenamed("region", "region").withColumnRenamed("requestTime", "requesttime").withColumnRenamed("sdkVersion", "sdkversion").withColumnRenamed("sessionId", "sessionid").withColumnRenamed("sessionMode", "sessionmode").withColumnRenamed("source", "source").withColumnRenamed("stateId", "stateid").withColumnRenamed("time", "time").withColumnRenamed("userId", "userid").withColumnRenamed("year", "year").withColumnRenamed("month", "month").withColumnRenamed("day", "day").withColumnRenamed("hour", "hour").withColumnRenamed("process_date", "process_date")

# DF.printSchema()
#Data.show(2)
DF.write.partitionBy("apikey","process_date","hour").orc("gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/glance_endedv3")


# .withColumnRenamed("duration", "duration")
# .withColumnRenamed("holdDuration", "holdduration")
# .withColumnRenamed("failed","failed")
# .withColumnRenamed("loadDuration", "loadduration")
# .withColumnRenamed("playCallDuration", "playcallduration")
# .withColumnRenamed("playStartDuration", "playstartduration")

# .withColumnRenamed("isLiked", "isliked")
# .withColumnRenamed("source", "source")

# .withColumnRenamed("glancePosition", "glanceposition")
# .withColumnRenamed("bubbleImpressionId", "bubbleimpressionid")
# .withColumnRenamed("isFeatureBank", "isfeaturebank").withColumnRenamed("liveStoriesCount", "livestoriescount")
# .withColumnRenamed("bubbleImpressionId", "bubbleimpressionid")

# .withColumnRenamed("networkType", "networktype").withColumnRenamed("notificationCount", "notificationcount")
