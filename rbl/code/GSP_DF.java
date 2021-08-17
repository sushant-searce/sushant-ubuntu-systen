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

public class GSP_DF {

	public static void main(String[] args) {
		
		System.out.println("DEL_FLG = 'N'");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> MRCT = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/gsp.csv");
		
		Dataset<Row> GSP_DF_filter = MRCT.select(MRCT.col("SCHM_CODE"), MRCT.col("ACCT_PREFIX"), MRCT.col("SCHM_DESC"), MRCT.col("PRODUCT_CONCEPT"))
				.filter(MRCT.col("DEL_FLG").equalTo("N"));

		
		StructField[] GSP_DFschemaFields = {
                new StructField("SCHM_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACCT_PREFIX", DataTypes.StringType, true, Metadata.empty()),
                new StructField("SCHM_DESC", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PRODUCT_CONCEPT", DataTypes.StringType, true, Metadata.empty())
		};
		
		StructType GSP_DFSchema = new StructType(GSP_DFschemaFields);
        ExpressionEncoder<Row> GSP_DFencoder = RowEncoder.apply(GSP_DFSchema);
		
		Dataset<Row> GSP_DF  = GSP_DF_filter.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
            	String SCHM_CODE = record.getAs("SCHM_CODE");
                String ACCT_PREFIX = record.getAs("ACCT_PREFIX");
                String SCHM_DESC = record.getAs("SCHM_DESC");
                String PRODUCT_CONCEPT = record.getAs("PRODUCT_CONCEPT");

                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(SCHM_CODE, ACCT_PREFIX, SCHM_DESC, PRODUCT_CONCEPT);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, GSP_DFencoder);
		
		GSP_DF.show();
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);

	}
	
}
