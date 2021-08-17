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

public class LAM {

	public static void main(String[] args) {
		
		System.out.println("LAM");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> LAM = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/LAM.csv");
		
		StructField[] LAM_DFschemaFields = {
                new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("REP_PERD_MTHS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("REP_PERD_DAYS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("OP_ACID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PAYOFF_FLG", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACCT_STATUS_FLG", DataTypes.StringType, true, Metadata.empty()),
                new StructField("EI_PERD_START_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PAYOFF_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("EI_PERD_END_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PAYOFF_REASON_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("CRFILE_REF_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("EMI_TYPE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PAYMENT_METHOD", DataTypes.StringType, true, Metadata.empty())
		};
		
		StructType LAM_DFSchema = new StructType(LAM_DFschemaFields);
        ExpressionEncoder<Row> LAM_DFencoder = RowEncoder.apply(LAM_DFSchema);
		
		Dataset<Row> LAM_DF  = LAM.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String ACID = record.getAs("ACID");
                String REP_PERD_MTHS = record.getAs("REP_PERD_MTHS");
                String REP_PERD_DAYS = record.getAs("REP_PERD_DAYS");
                String OP_ACID = record.getAs("OP_ACID");
                String PAYOFF_FLG = record.getAs("PAYOFF_FLG");
                String ACCT_STATUS_FLG = record.getAs("ACCT_STATUS_FLG");
                String EI_PERD_START_DATE = record.getAs("EI_PERD_START_DATE");
                String PAYOFF_DATE = record.getAs("PAYOFF_DATE");
                String EI_PERD_END_DATE = record.getAs("EI_PERD_END_DATE");
                String PAYOFF_REASON_CODE = record.getAs("PAYOFF_REASON_CODE");
                String CRFILE_REF_ID = record.getAs("CRFILE_REF_ID");               
                String EMI_TYPE = record.getAs("EI_SCHM_FLG");
                String PAYMENT_METHOD = record.getAs("DMD_SATISFY_MTHD");
                
                if (EMI_TYPE.contentEquals("Y")) {
                	EMI_TYPE = "EI";
               }
                else {
            	   EMI_TYPE = "NON EI";
               }
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ACID, REP_PERD_MTHS, REP_PERD_DAYS, OP_ACID, PAYOFF_FLG , ACCT_STATUS_FLG, EI_PERD_START_DATE, PAYOFF_DATE
                		, EI_PERD_END_DATE, PAYOFF_REASON_CODE, CRFILE_REF_ID, EMI_TYPE, PAYMENT_METHOD);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, LAM_DFencoder);
		
		LAM_DF.show();
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);

	}
	
}
