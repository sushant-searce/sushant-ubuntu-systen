package org.demo_rbl;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;

import java.util.ArrayList;
import java.util.Iterator;

public class test2 {
	public static void main(String[] args) {
		System.out.println("HelloLetsGetStarted");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark SQL basic example")
				  .getOrCreate();
		
		Dataset<Row> sush = spark.read().format("csv")
                .option("delimiter", ",")
                .option("header", "true")
                .load("/home/sushantnigudkar/Desktop/testingofrbl/people.csv");
		
//		
//		a.createOrReplaceTempView("demo");
//		
//		Dataset<Row> sqlDF = spark.sql("SELECT * FROM demo where ACCOUNT_HOLDER_NAME like '%GANAPATI%'");
//		sqlDF.show();
		
		sush.show();
		sush.printSchema();
		
		Dataset<Row> sush1 = sush.select("Name", "Age", "Job", "Marks", "Aggr")
				.withColumn("Marks", col("Marks").cast("double"))
				.withColumn("Age", col("Age").cast("double"))
				.withColumn("Aggr", col("Aggr").cast("double"));
		
		sush1.show();
		sush1.printSchema();
                
//		sush.show();
//		sush3.show();
//		sush3.printSchema();	
		
		StructField[] schemaFields = {
              new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
              new StructField("Age", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("Job", DataTypes.StringType, true, Metadata.empty()),
              new StructField("Marks", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("Marksceil", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("Marksfloor", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("roundmarks", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("Aggr", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("power", DataTypes.DoubleType, true, Metadata.empty())
              
		};
		
		StructType Schema = new StructType(schemaFields);
		ExpressionEncoder<Row> encoder = RowEncoder.apply(Schema);
		
		Dataset<Row> test  = sush1.flatMap(new FlatMapFunction<Row, Row>() {
          @Override
          public Iterator<Row> call(Row record) throws Exception {
              String Name = record.getAs("Name");
              Double Age = record.getAs("Age");
              String Job = record.getAs("Job");
              Double Marks = record.getAs("Marks");	
              Double Aggr = record.getAs("Aggr");
              Double Marksceil = Math.ceil(Marks);
              Double Marksfloor = Math.floor(Marks);           
              Double power = Math.pow(Marksceil, Aggr);
              
              double roundmarks = Math.round(Marks);
              
              BigDecimal b = new BigDecimal(Marks);
              System.out.println(b);            
              
//              BigDecimal c = Math.round(b);           
//            System.out.println(b.toString()); 
//            System.out.println(roundmarks);         
//            System.out.print(Math.ceil(Marks)); 
//            System.out.print(Math.floor(Marks)); 
//            System.out.print(Math.pow(Marks, Aggr));            
                
              ArrayList<Row> newRows = new ArrayList<>();
              Row recordOut;
              recordOut = RowFactory.create(Name, Age, Job, Marks, Marksceil, Marksfloor,roundmarks, Aggr,power);
              newRows.add(recordOut);
              return newRows.iterator();
          }
		}, encoder);
		
		test.show();
		test.printSchema();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
	}
}