package org.demo_rbl;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class C_DBC {

	public static void main(String[] args) {

		System.out.println("GAM");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> adt = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("resources/ADT.txt");
		
		Dataset<Row> gct = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("resources/GCT.txt");
		
		Dataset<Row> gem = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("resources/GEM.txt");
		
		Dataset<Row> lrp = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("resources/LRP.txt");
		
		Dataset<Row> c_gam = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("resources/C_GAM.txt");
		
		Dataset<Row> GAM_DF = funcFetchGAMDF(spark, adt, gct, gem, lrp, c_gam);
		
		
		printMessage(" No of rows = " + adt.count() + "  & no of colums in adt  = " + adt.columns().length);

		printMessage(" No of rows = " + c_gam.count() + "  & no of colums in c_gam  = " + c_gam.columns().length);
	
		printMessage(" No of rows = " + gct.count() + "  & no of colums in gct  = " + gct.columns().length);

		printMessage(" No of rows = " + gem.count() + "  & no of colums in gem  = " + gem.columns().length);
		
		printMessage(" No of rows = " + lrp.count() + "  & no of colums in lrp  = " + lrp.columns().length);		
}


	private static Dataset<Row> funcFetchGAMDF(SparkSession spark, Dataset<Row> adt, Dataset<Row> gct, Dataset<Row> gem,
			Dataset<Row> lrp, Dataset<Row> c_gam) {
		
		Dataset<Row> gam1DF = adt.select("ACID")
					.filter(functions.col("ACID").notEqual("!"));
		
		Dataset<Row> gam1UnionDF = c_gam.select("ACID")
					.filter(functions.col("ACCT_OWNERSHIP").notEqual("O"))
					.filter(functions.col("DEL_FLG").equalTo("N"))
					.filter(functions.col("ENTITY_CRE_FLG").equalTo("Y")).union(gam1DF).distinct();
		
		gam1UnionDF.show();

		
		Dataset<Row> gamOPDF = c_gam.join(gam1UnionDF, c_gam.col("ACID").equalTo(gam1UnionDF.col("ACID")), "inner")
				   .join(gem, c_gam.col("ACID").equalTo(gem.col("ACID")), "left")
				   .join(lrp, c_gam.col("ACID").equalTo(lrp.col("ACID"))
						   .and(lrp.col("entity_cre_flg").equalTo(lrp.col("entity_cre_flg")))
						   .and(lrp.col("del_flg").equalTo(lrp.col("del_flg"))), "left")
				   .crossJoin(gct)
				   .filter(c_gam.col("ACCT_OWNERSHIP").notEqual("O"))
				   .filter(c_gam.col("DEL_FLG").equalTo("N"))
				   .filter(c_gam.col("ENTITY_CRE_FLG").equalTo("Y"))
				   .withColumn("repricing_plan", lrp.col("repricing_plan"))
				   .withColumn("ACCT_OPN_BOD_DATE", gem.col("ACCT_OPN_BOD_DATE"));
		
		gamOPDF.show();
		System.out.print(" No of rows = " + gamOPDF.count() + "  & no of colums in gamOPDF  = " + gamOPDF.columns().length);

		return gamOPDF;
	}
	
	private static void printMessage(String string) {
		// TODO Auto-generated method stub		
	}
}