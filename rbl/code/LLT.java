package org.demo_rbl;

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

public class LLT {

	public static void main(String[] args) {
		System.out.println("LLT");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> LLT = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/llt.csv");
		
		Dataset<Row> llt = LLT.select(LLT.col("LIMIT_B2KID"), LLT.col("LIM_EXP_DATE"), LLT.col("LIMIT_REVIEW_DATE"));

		
		StructField[] LLT_DFschemaFields = {
                new StructField("LIMIT_B2KID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("LIM_EXP_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("LIMIT_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty())             
		};
		
		
		StructType LLT_DFSchema = new StructType(LLT_DFschemaFields);
        ExpressionEncoder<Row> LLT_DFencoder = RowEncoder.apply(LLT_DFSchema);
		
		Dataset<Row> LLT_DF  = llt.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String LIMIT_B2KID = record.getAs("LIMIT_B2KID");
                String LIM_EXP_DATE = record.getAs("LIM_EXP_DATE");
                String LIMIT_REVIEW_DATE = record.getAs("LIMIT_REVIEW_DATE");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(LIMIT_B2KID, LIM_EXP_DATE, LIMIT_REVIEW_DATE);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, LLT_DFencoder);
		
		LLT_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}

