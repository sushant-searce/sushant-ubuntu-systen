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

public class Array_Map_Type {

	public static void main(String[] args) {
		
		System.out.println("hello lets start with Spark pivot unpivot example");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark pivot unpivot example")
				  .getOrCreate();
		
		Dataset<Row> singers = spark.createDF(
				  List(("bieber", Array("baby", "sorry")),
				    ("ozuna", Array("criminal"))
				  ), List(
				    ("name", StringType, true),
				    ("hit_songs", ArrayType(StringType, true), true)
				  )
				);

				singers.show();

	}

}
