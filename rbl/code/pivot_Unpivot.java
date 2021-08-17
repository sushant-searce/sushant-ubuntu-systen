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

public class pivot_Unpivot {

public static void main(String[] args) {
		
		System.out.println("hello lets start with Spark pivot unpivot example");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark pivot unpivot example")
				  .getOrCreate();

		Dataset<Row> source = spark.read().format("json")
				.load("/home/sushantnigudkar/Desktop/spark-testing-prep/pivot.json");
		
//		source.show();
		Dataset<Row> sourceDf = source
			    .groupBy("salesperson")
			    .pivot("device")
			    .sum("amount_sold");
		
//		sourceDf.show();
		
		Dataset<Row> unpivot = sourceDf
				.selectExpr("salesperson","stack(4,'camera','camera','large phone','large phone','notebook','notebook','small phone','small phone') as ")
			    .withColumnRenamed("col0","device") // default name of this column is col0
			    .withColumnRenamed("col1","amount_sold")// default name of this column is col1
			    .filter("amount_sold".isNotNull); // must explicitly remove nulls	
		
		unpivot.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);		

}
}