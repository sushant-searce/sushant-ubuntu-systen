package org.demo_rbl;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;

public class Parquet {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("hello lets start with Spark parquet/avro example");
		
		SparkSession spark = SparkSession
			  .builder().master("local[*]")
			  .appName("Java Spark pivot unpivot example")
			  .getOrCreate();
		
//		Dataset<Row> source = spark.read().format("json")
//				.load("/home/sushantnigudkar/Desktop/spark-testing-prep/pivot.json");
		
//		source.show();
		
//		source.write().parquet("/home/sushantnigudkar/Desktop/spark-testing-prep/pivot.parquet");
		
//		Dataset<Row> sourceDF = spark.read().parquet("/home/sushantnigudkar/Desktop/spark-testing-prep/userdata1.parquet");
//		sourceDF.show();
		
		Dataset<Row> usersDF = spark.read().format("avro")
				.load("/home/sushantnigudkar/Desktop/spark-testing-prep/userdata1.avro");
		
		usersDF.show();
		
		
		
//		source.write().format("avro").save("/home/sushantnigudkar/Desktop/spark-testing-prep/pivot.avro");
		
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);		

	}

}
