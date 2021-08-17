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

public class RCT_CAM_GX_DF {

	public static void main(String[] args) {
		
		System.out.println("RCT CAM REF_REC_TYPE == 'GX'");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> MRCT = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/rct.csv");
		
		
		Dataset<Row> RCT_CAM = MRCT.select(MRCT.col("REF_CODE"), MRCT.col("REF_DESC"))
				.distinct()
				.filter(MRCT.col("REF_REC_TYPE").equalTo("GX"))
				.withColumnRenamed("REF_DESC","ACCT_CLS_REASON_DESC");
		



		StructField[] RCT_CAM_DFschemaFields = {
                new StructField("REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACCT_CLS_REASON_DESC", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType RCT_CAM_DFSchema = new StructType(RCT_CAM_DFschemaFields);
        ExpressionEncoder<Row> RCT_CAM_DFencoder = RowEncoder.apply(RCT_CAM_DFSchema);
		
		Dataset<Row> RCT_CAM_DF  = RCT_CAM.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String REF_CODE = record.getAs("REF_CODE");
                String ACCT_CLS_REASON_DESC = record.getAs("ACCT_CLS_REASON_DESC");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(REF_CODE, ACCT_CLS_REASON_DESC );
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_CAM_DFencoder);
		
		RCT_CAM_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}


