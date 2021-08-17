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

public class CFCM {

	public static void main(String[] args) {
		
		System.out.println("CFCM");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
				
		Dataset<Row> cfcm = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/cfcm.csv").select("ENTITY_ID","FREE_CODE_20","FREE_CODE_22","FREE_CODE_21"
            		  , "FREE_CODE_5","FREE_CODE_6","FREE_CODE_1","FREE_CODE_43","ENTITY_TYPE")
    				.filter("ENTITY_TYPE = 'A'")
    				.withColumnRenamed("FREE_CODE_5","CFCM_FREE_CODE_5")
    				.withColumnRenamed("FREE_CODE_6","CFCM_FREE_CODE_6");
		
		cfcm.show();
		
//		Dataset<Row> cfcm_filter = cfcm.select(cfcm.col("ENTITY_ID"), cfcm.col("FREE_CODE_20"), cfcm.col("FREE_CODE_22")
//				, cfcm.col("FREE_CODE_21"), cfcm.col("FREE_CODE_5"), cfcm.col("FREE_CODE_6")
//				, cfcm.col("FREE_CODE_1"), cfcm.col("FREE_CODE_43"), cfcm.col("ENTITY_TYPE"))
//				.filter(cfcm.col("ENTITY_TYPE").equalTo("A"))
//				.withColumnRenamed("FREE_CODE_5","CFCM_FREE_CODE_5")
//				.withColumnRenamed("FREE_CODE_6","CFCM_FREE_CODE_6");	
//				

//		StructField[] CFCM_DFschemaFields = {
//                new StructField("ENTITY_ID", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("FREE_CODE_20", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("FREE_CODE_22", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("FREE_CODE_21", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("CFCM_FREE_CODE_5", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("CFCM_FREE_CODE_6", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("FREE_CODE_1", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("FREE_CODE_43", DataTypes.StringType, true, Metadata.empty())
//		};
//		
//		StructType CFCM_DFSchema = new StructType(CFCM_DFschemaFields);
//        ExpressionEncoder<Row> CFCM_DFencoder = RowEncoder.apply(CFCM_DFSchema);
//		
//		Dataset<Row> CFCM_DF  = cfcm_filter.flatMap(new FlatMapFunction<Row, Row>() {
//            @Override
//            public Iterator<Row> call(Row record) throws Exception {
//            	String ENTITY_ID = record.getAs("ENTITY_ID");
//                String FREE_CODE_20 = record.getAs("FREE_CODE_20");
//                String FREE_CODE_22 = record.getAs("FREE_CODE_22");
//                String FREE_CODE_21 = record.getAs("FREE_CODE_21");
//                String CFCM_FREE_CODE_5 = record.getAs("CFCM_FREE_CODE_5");
//                String CFCM_FREE_CODE_6 = record.getAs("CFCM_FREE_CODE_6");
//                String FREE_CODE_1 = record.getAs("FREE_CODE_1");
//                String FREE_CODE_43 = record.getAs("FREE_CODE_43");
//
//                ArrayList<Row> newRows = new ArrayList<>();
//                Row recordOut;
//                recordOut = RowFactory.create(ENTITY_ID, FREE_CODE_20, FREE_CODE_22, FREE_CODE_21, CFCM_FREE_CODE_5, CFCM_FREE_CODE_6
//                		, FREE_CODE_1, FREE_CODE_43);
//                newRows.add(recordOut);
//                return newRows.iterator();
//            }
//		}, CFCM_DFencoder);
//		
//		CFCM_DF.show();
//		
//		Logger.getLogger("org").setLevel(Level.OFF);
//		Logger.getLogger("INFO").setLevel(Level.OFF);

	}
	
}
