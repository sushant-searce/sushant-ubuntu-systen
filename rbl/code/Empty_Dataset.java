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

public class Empty_Dataset {
public static void main(String[] args) {
		
		System.out.println("hello");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark empty dataset example")
				  .getOrCreate();
		

		Dataset<Row> dataset = spark.emptyDataFrame();
		Dataset<Row> employee = spark.read().format("json").load("/home/sushantnigudkar/Desktop/spark-testing-prep/employee.json");
//		employee.show();
		StructType structure = employee.select("Employee_Name").schema();
		List<Row> rows = new ArrayList<Row>();
		Dataset<Row> schema = spark.createDataFrame(rows, structure);
		schema.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);		

}
}
