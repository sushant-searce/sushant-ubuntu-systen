package org.demo3.rbl;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

public class testing {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		

		
//		Dataset<Row> adf_client = spark.read().format("csv")
////	              .option("delimiter", "|")
//	              .option("header", "true")
//	              .load("/home/sushantnigudkar/Downloads/part.csv");
////	              .select("ACCOUNT_HOLDER_NAME");
//		
//		
//		System.out.println(" No of rows in adf_client= " + adf_client.count() + "  & no of colums in adf_client  = " + adf_client.columns().length);
//		adf_client.show(10);
		
		
		
		Dataset<Row> adf = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/Downloads/Validation.csv");
//	              .filter("ACCOUNT_HOLDER_NAME = %GANAPATI%");
//		
//		System.out.println(" No of rows in adf= " + adf.count() + "  & no of colums in adf  = " + adf.columns().length);
//		adf.show();
		
		adf.createOrReplaceTempView("demo");
		
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM demo where ACCOUNT_HOLDER_NAME like '%GANAPATI%'");
		sqlDF.show();

		
		
		
//		adf_client.coalesce(20).write().option("header", "true").csv("/home/sushantnigudkar/Downloads/final_test.csv");

	}

}
