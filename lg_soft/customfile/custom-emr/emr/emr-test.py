import sys
from datetime import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from module.searce import searce


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputfile', help="shows inputfile")
    parser.add_argument('--outputfile', help="shows outputfile")
    parser.add_argument('--somevariable', help="shows variable we want to use for Emr")
    support_testing()
    searce()

    args = parser.parse_args()
    print(args.inputfile)
    print(args.outputfile)
    print(args.somevariable)

    spark = SparkSession\
        .builder\
        .appName("SparkETL")\
        .getOrCreate()

    nyTaxi = spark.read.option("inferSchema", "true").option("header", "true").csv(args.inputfile)
    print(nyTaxi.show())

    updatedNYTaxi = nyTaxi.withColumn("current_date", lit(datetime.now()))

    updatedNYTaxi.write.csv(args.outputfile)
