package org.demo3.rbl;

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
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;

public class ITC_DF {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();

		
		Dataset<Row> A = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/ITC.csv");
		
		Dataset<Row> B = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/ITC.csv");
		
		Dataset <Row> ITC_B = B.select(B.col("ENTITY_ID"), B.col("INT_TBL_CODE_SRL_NUM"), B.col("ENTITY_TYPE")
				, B.col("DEL_FLG"), B.col("ENTITY_CRE_FLG"))
				.filter(B.col("ENTITY_TYPE").equalTo("ACCNT"))
				.filter(B.col("DEL_FLG").equalTo("N"))
				.filter(B.col("ENTITY_CRE_FLG").equalTo("Y"))
				.groupBy(B.col("ENTITY_ID"))
				.agg(max(B.col("INT_TBL_CODE_SRL_NUM")))
				.withColumnRenamed("max(INT_TBL_CODE_SRL_NUM)","INT_TBL_CODE_SRL_NUM");
		
//		ITC_B.show();
				
				
		Dataset<Row> ITC_A = A.select(A.col("ENTITY_ID"), A.col("INT_TBL_CODE_SRL_NUM"), A.col("PEGGED_FLG")
				, A.col("INT_TBL_CODE"), A.col("NRML_PCNT_CR"), A.col("REASON_CODE"), A.col("PEG_REVIEW_DATE")
				, A.col("END_DATE"), A.col("PEG_FREQUENCY_IN_MONTHS"), A.col("PEG_FREQUENCY_IN_DAYS"));
		
//		ITC_A.show();

		Dataset<Row> A_B = ITC_B.join(ITC_A, ITC_A.col("ENTITY_ID").equalTo(ITC_B.col("ENTITY_ID"))
				.and(ITC_A.col("INT_TBL_CODE_SRL_NUM").equalTo(ITC_B.col("INT_TBL_CODE_SRL_NUM"))));

//		A_B.show();
		
		StructField[] ITC_DFschemaFields = {
                new StructField("ENTITY_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("INT_TBL_CODE_SRL_NUM", DataTypes.StringType, true, Metadata.empty()),          
                new StructField("PEGGED_FLG", DataTypes.StringType, true, Metadata.empty()),
                new StructField("INT_TBL_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("NRML_PCNT_CR", DataTypes.StringType, true, Metadata.empty()),
                new StructField("REASON_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PEG_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("END_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PEG_FREQUENCY_IN_MONTHS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PEG_FREQUENCY_IN_DAYS", DataTypes.StringType, true, Metadata.empty()) 
                };
		
		
		StructType ITC_DFSchema = new StructType(ITC_DFschemaFields);
        ExpressionEncoder<Row> ITC_DFencoder = RowEncoder.apply(ITC_DFSchema);
		
		Dataset<Row> ITC_DF  = A_B.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String ENTITY_ID = record.getAs("ENTITY_ID");
                String INT_TBL_CODE_SRL_NUM = record.getAs("INT_TBL_CODE_SRL_NUM");
                String PEGGED_FLG = record.getAs("PEGGED_FLG");
                String INT_TBL_CODE = record.getAs("INT_TBL_CODE");
                String NRML_PCNT_CR = record.getAs("NRML_PCNT_CR");
                String REASON_CODE = record.getAs("REASON_CODE");
                String PEG_REVIEW_DATE = record.getAs("PEG_REVIEW_DATE");
                String END_DATE = record.getAs("END_DATE");
                String PEG_FREQUENCY_IN_MONTHS = record.getAs("PEG_FREQUENCY_IN_MONTHS");
                String PEG_FREQUENCY_IN_DAYS = record.getAs("PEG_FREQUENCY_IN_DAYS");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ENTITY_ID, INT_TBL_CODE_SRL_NUM, PEGGED_FLG , INT_TBL_CODE
                		, NRML_PCNT_CR, REASON_CODE, PEG_REVIEW_DATE, END_DATE, PEG_FREQUENCY_IN_MONTHS, PEG_FREQUENCY_IN_DAYS );
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, ITC_DFencoder);
		
		ITC_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
		
		
	}

}
