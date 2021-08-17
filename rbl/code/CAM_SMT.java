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

public class CAM_SMT {

	public static void main(String[] args) {
		
		System.out.println("CAM_SMT");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> CAMs = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/CAM.csv");
		
		Dataset<Row> SMT = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/smt.csv");

		Dataset<Row> CAM_DFs = CAMs.select(CAMs.col("ACID"), CAMs.col("ACCT_STATUS"), CAMs.col("ACCT_STATUS_DATE")
				, CAMs.col("ACCT_CLS_REASON_CODE"))
				.filter(CAMs.col("DEL_FLG").equalTo("N"))
				.filter(CAMs.col("ENTITY_CRE_FLG").equalTo("y"))
				.union(SMT.select(SMT.col("ACID"), SMT.col("ACCT_STATUS"), SMT.col("ACCT_STATUS_DATE")
						, SMT.col("ACCT_CLS_REASON_CODE")));
		
		CAM_DFs.show();
		
		System.out.println(" No of rows i= " + CAM_DFs.count() + "  & no of colums  = " + CAM_DFs.columns().length);
	
//		StructField[] CAM_DFschemaFields = {
//                new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("ACCT_STATUS", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("ACCT_STATUS_DATE", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("ACCT_CLS_REASON_CODE", DataTypes.StringType, true, Metadata.empty())
//		};
//		
//		
//		StructType CAM_DFSchema = new StructType(CAM_DFschemaFields);
//        ExpressionEncoder<Row> CAM_DFencoder = RowEncoder.apply(CAM_DFSchema);
//		
//		Dataset<Row> CAM_DF  = CAM_DFs.flatMap(new FlatMapFunction<Row, Row>() {
//            @Override
//            public Iterator<Row> call(Row record) throws Exception {
//                String ACID = record.getAs("ACID");
//                String ACCT_STATUS = record.getAs("ACCT_STATUS");
//                String ACCT_STATUS_DATE = record.getAs("ACCT_STATUS_DATE");
//                String ACCT_CLS_REASON_CODE = record.getAs("ACCT_CLS_REASON_CODE");
////                String REF_REC_TYPE = record.getAs("REF_REC_TYPE");
//          
//                  
//                ArrayList<Row> newRows = new ArrayList<>();
//                Row recordOut;
//                recordOut = RowFactory.create(ACID, ACCT_STATUS, ACCT_STATUS_DATE, ACCT_CLS_REASON_CODE );
//                newRows.add(recordOut);
//                return newRows.iterator();
//            }
//		}, CAM_DFencoder);
//		
//		CAM_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}





