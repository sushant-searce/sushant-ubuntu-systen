package org.demo2.rbl;

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
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;

public class SOL_DF {

	public static void main(String[] args) {
		
		System.out.println("SOL");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> sol = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/sol.csv");

		
		
		Dataset<Row> SOL_DF_filter = sol.select(sol.col("SOL_ID"), sol.col("SOL_DESC"))
				.distinct()
				.filter(sol.col("DEL_FLG").equalTo("N"))
				.filter(sol.col("BANK_CODE").equalTo("176"))
				.withColumnRenamed("SOL_DESC","BRANCH_NAME");
				


		StructField[] SOL_DFschemaFields = {
                new StructField("SOL_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("BRANCH_NAME", DataTypes.StringType, true, Metadata.empty())
		};
		
		StructType SOL_DFSchema = new StructType(SOL_DFschemaFields);
        ExpressionEncoder<Row> SOL_DFencoder = RowEncoder.apply(SOL_DFSchema);
		
		Dataset<Row> SOL_DF  = SOL_DF_filter.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
            	String SOL_ID = record.getAs("SOL_ID");
                String BRANCH_NAME = record.getAs("BRANCH_NAME");

                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(SOL_ID, BRANCH_NAME);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, SOL_DFencoder);
		
		SOL_DF.show();
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);

	}
	
}

