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

public class RCT_AF_DF {

	public static void main(String[] args) {
		
		System.out.println("RCT_AF_DF  => REF_REC_TYPE = 'AF'");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> MRCT = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/rct.csv");
		
		
		Dataset<Row> RCT_AF = MRCT.select(MRCT.col("REF_CODE"), MRCT.col("REF_DESC"))
				.filter(MRCT.col("REF_REC_TYPE").equalTo("AF"))
				.withColumnRenamed("REF_CODE","RCT_AF_REF_CODE")
				.withColumnRenamed("REF_DESC","RCT_AF_REF_DESC");


		StructField[] RCT_AF_DFschemaFields = {
                new StructField("RCT_AF_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_AF_REF_DESC", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType RCT_AF_DFSchema = new StructType(RCT_AF_DFschemaFields);
        ExpressionEncoder<Row> RCT_AF_DFencoder = RowEncoder.apply(RCT_AF_DFSchema);
		
		Dataset<Row> RCT_AF_DF  = RCT_AF.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_AF_REF_CODE = record.getAs("RCT_AF_REF_CODE");
                String RCT_AF_REF_DESC = record.getAs("RCT_AF_REF_DESC");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_AF_REF_CODE, RCT_AF_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_AF_DFencoder);
		
		RCT_AF_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}
