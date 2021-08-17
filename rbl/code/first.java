package org.demo_rbl;

import org.apache.spark.sql.SparkSession;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class first {

	public static void main(String[] args) {
		
		System.out.println("hello");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark SQL basic example")
				  .getOrCreate();
		
		Dataset<Row> csv = spark.read().format("csv")
                .option("delimiter", ";")
                .option("header", "true")
                .load("/home/sushantnigudkar/Desktop/spark-testing-prep/access-code.csv");

				csv.show();
				
//		Dataset<Row> text = spark.read()
//                .option("header", "false")
//                .csv("/home/sushantnigudkar/Desktop/spark-testing-prep/test.txt");
//
//				text.show();	
				
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}

}
