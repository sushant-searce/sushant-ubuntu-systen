package org.demo2.rbl;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ACD {

	public static void main(String[] args) {

System.out.println("ACD");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> acd = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("resources/gem.txt")
				.select("RCRE_TIME");
		
//		acd.createOrReplaceTempView("demo");
//		
//		Dataset<Row> sqlDF = spark.sql("SELECT * FROM demo where FORACID like '1005510010002142'");
//		Dataset<Row> sqlDF = spark.sql("SELECT ACCOUNT_NO FROM demo");
		
//		1005510010002142	
		
//		sqlDF.show(1000);
//              .select("B2K_ID","MAIN_CLASSIFICATION_USER");
		System.out.println("acc master cols = " + acd.columns().length);
		
		acd.show(10);
		
//		Dataset<Row> ACD1 = acd.select(acd.col("B2K_ID"), acd.col("MAIN_CLASSIFICATION_USER"));

		
//		StructField[] ACD_DFschemaFields = {
//                new StructField("B2K_ID", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("MAIN_CLASSIFICATION_USER", DataTypes.StringType, true, Metadata.empty())            
//		};
//		
//		
//		StructType ACD_DFSchema = new StructType(ACD_DFschemaFields);
//        ExpressionEncoder<Row> ACD_DFencoder = RowEncoder.apply(ACD_DFSchema);
//		
//		Dataset<Row> ACD_DF  = ACD1.flatMap(new FlatMapFunction<Row, Row>() {
//            @Override
//            public Iterator<Row> call(Row record) throws Exception {
//                String B2K_ID = record.getAs("B2K_ID");
//                String MAIN_CLASSIFICATION_USER = record.getAs("MAIN_CLASSIFICATION_USER");
//                
//                ArrayList<Row> newRows = new ArrayList<>();
//                Row recordOut;
//                recordOut = RowFactory.create(B2K_ID, MAIN_CLASSIFICATION_USER);
//                newRows.add(recordOut);
//                return newRows.iterator();
//            }
//		}, ACD_DFencoder);
//		
//		ACD_DF.show();
//
//		Logger.getLogger("org").setLevel(Level.OFF);
//		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}