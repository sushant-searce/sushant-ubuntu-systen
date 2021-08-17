package org.demo3.rbl;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;

public class adf_cdr {

	public static void main(String[] args) {
		
		System.out.println("testing");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> adf_cdr = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("resources/adf_cdr_lrs.txt");
		

//		adf_cdr1.show();
		
//		adf_cdr.createOrReplaceTempView("demo");
//		
//		Dataset<Row> sqlDF = spark.sql("SELECT row_number() over(partition by acid order by acid,next_emi_date,lr_freq_type ) srno,* FROM demo");
//		sqlDF.show();
//		
//		
//		sqlDF.createOrReplaceTempView("dem");
//		Dataset<Row> sqlDF1 = spark.sql("SELECT * FROM dem WHERE srno=1");
//		sqlDF1.show();
//		
		
		
//		adf_cdr.show();
		
		Dataset<Row> a = adf_cdr.withColumn("SRNO", functions.row_number().over(Window.partitionBy("ACID").orderBy("ACID")
					.orderBy(functions.col("next_emi_date"))
					.orderBy(functions.col("lr_freq_type"))).alias("SRNO")).filter(functions.col("SRNO").equalTo("1"));
		
		a.show(10);
		
		//.filter(functions.col("SRNO").equalTo("1"))
		
		Dataset<Row> b = adf_cdr.withColumn("SRNO", functions.row_number().over(Window.partitionBy("ACID").orderBy("ACID")
				.orderBy(functions.col("next_emi_date").desc())
				.orderBy(functions.col("lr_freq_type").desc())).alias("SRNO")).filter(functions.col("SRNO").equalTo("1"));
	
	b.show(10);
		
		
//		


		

	}

}
