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

public class TAM_DF {

	public static void main(String[] args) {
		
		System.out.println("TAM");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> T = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/TAM.csv").select("ACID", "CLOSE_ON_MATURITY_FLG","AUTO_RENEWAL_FLG","DEPOSIT_PERIOD_MTHS","DEPOSIT_PERIOD_DAYS"
      				,"ACCT_STATUS","LINK_OPER_ACCOUNT","OPEN_EFFECTIVE_DATE","MATURITY_DATE", "MATURITY_AMOUNT"
    				,"DEPOSIT_AMOUNT", "DEPOSIT_STATUS","REPAYMENT_ACID");
		
		T.show();
		

		
		Dataset<Row> TAM = T.select(T.col("ACID"), T.col("CLOSE_ON_MATURITY_FLG"), T.col("AUTO_RENEWAL_FLG"), T.col("DEPOSIT_PERIOD_MTHS")
				,T.col("DEPOSIT_PERIOD_DAYS"), T.col("ACCT_STATUS"), T.col("LINK_OPER_ACCOUNT")
				, T.col("OPEN_EFFECTIVE_DATE"), T.col("MATURITY_DATE"), T.col("MATURITY_AMOUNT")
				,T.col("DEPOSIT_AMOUNT"), T.col("DEPOSIT_STATUS"), T.col("REPAYMENT_ACID"));
		
		TAM.show();
		

		
//		StructField[] TAM_DFschemaFields = {
//                new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("CLOSE_ON_MATURITY_FLG", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("AUTO_RENEWAL_FLG", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("DEPOSIT_PERIOD_MTHS", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("DEPOSIT_PERIOD_DAYS", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("ACCT_STATUS", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("LINK_OPER_ACCOUNT", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("OPEN_EFFECTIVE_DATE", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("MATURITY_DATE", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("MATURITY_AMOUNT", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("DEPOSIT_AMOUNT", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("DEPOSIT_STATUS", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("REPAYMENT_ACID", DataTypes.StringType, true, Metadata.empty())
//		};
//		
//		StructType TAM_DFSchema = new StructType(TAM_DFschemaFields);
//        ExpressionEncoder<Row> TAM_DFencoder = RowEncoder.apply(TAM_DFSchema);
//		
//		Dataset<Row> TAM_DF  = TAM.flatMap(new FlatMapFunction<Row, Row>() {
//            @Override
//            public Iterator<Row> call(Row record) throws Exception {
//            	String ACID = record.getAs("ACID");
//                String CLOSE_ON_MATURITY_FLG = record.getAs("CLOSE_ON_MATURITY_FLG");
//                String AUTO_RENEWAL_FLG = record.getAs("AUTO_RENEWAL_FLG");
//                String DEPOSIT_PERIOD_MTHS = record.getAs("DEPOSIT_PERIOD_MTHS");
//                String DEPOSIT_PERIOD_DAYS = record.getAs("DEPOSIT_PERIOD_DAYS");
//                String ACCT_STATUS = record.getAs("ACCT_STATUS");
//                String LINK_OPER_ACCOUNT = record.getAs("LINK_OPER_ACCOUNT");
//                String OPEN_EFFECTIVE_DATE = record.getAs("OPEN_EFFECTIVE_DATE");
//                String MATURITY_DATE = record.getAs("MATURITY_DATE");
//                String MATURITY_AMOUNT = record.getAs("MATURITY_AMOUNT");
//                String DEPOSIT_AMOUNT = record.getAs("DEPOSIT_AMOUNT");
//                String DEPOSIT_STATUS = record.getAs("DEPOSIT_STATUS");
//                String REPAYMENT_ACID = record.getAs("REPAYMENT_ACID");
//
//                ArrayList<Row> newRows = new ArrayList<>();
//                Row recordOut;
//                recordOut = RowFactory.create(ACID, CLOSE_ON_MATURITY_FLG, AUTO_RENEWAL_FLG, DEPOSIT_PERIOD_MTHS, DEPOSIT_PERIOD_DAYS
//                		, ACCT_STATUS, LINK_OPER_ACCOUNT,  OPEN_EFFECTIVE_DATE, MATURITY_DATE,MATURITY_AMOUNT, DEPOSIT_AMOUNT , DEPOSIT_STATUS, REPAYMENT_ACID);
//                newRows.add(recordOut);
//                return newRows.iterator();
//            }
//		}, TAM_DFencoder);
//		
//		TAM_DF.show();
//		
//		Logger.getLogger("org").setLevel(Level.OFF);
//		Logger.getLogger("INFO").setLevel(Level.OFF);

	}
	
}
