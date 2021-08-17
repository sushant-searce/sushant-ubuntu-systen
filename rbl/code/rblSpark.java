package sparkEtl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.collection.JavaConversions;
import org.apache.commons.lang.StringUtils;
import java.util.*; 
import scala.collection.Seq;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class RblSparkEtl {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().master("local[*]").appName("RBL Spark ETL Job")
				.config("spark.sql.crossJoin.enabled", "true").getOrCreate();
		

		String outPath = "resources/outDF.csv";
		// 1. ACT - Starts
		printMessage("ACT----------------Starts");
		Dataset<Row> acd = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/acd.txt");
		// acd.show();
		acd.createOrReplaceTempView("ACD");

		//Dataset<Row> acdDF = spark.sql("SELECT count(*) as cnt FROM ACD");
		//acdDF.show();
		//printMessage("ACT----------------Ends");
		// 1. ACT - Ends

		// 2. ADT - Starts
		printMessage("ADT----------------Starts");
		Dataset<Row> adt = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/ADT.txt");
		// adt.show();
		adt.createOrReplaceTempView("ADT");

		//Dataset<Row> adtDF = spark.sql("SELECT count(*) as cnt FROM ADT");
		//adtDF.show();
		printMessage("ADT----------------Ends");
		// 2. ADT - Ends

		// 3. ALR - Starts
		printMessage("ALR----------------Starts");
		Dataset<Row> alr = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/ALR.txt");
		// alr.show();
		alr.createOrReplaceTempView("ALR");

		//Dataset<Row> alrDF = spark.sql("SELECT count(*) as cnt FROM ALR");
		//alrDF.show();
		printMessage("ALR----------------Ends");
		// 3. ALR - Ends

		// 4. C_DBC - Starts
		printMessage("C_DBC----------------Starts");
		Dataset<Row> c_dbc = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/C_DBC.txt");
		// c_dbc.show();
		c_dbc.createOrReplaceTempView("C_DBC");

		//Dataset<Row> c_dbcDF = spark.sql("SELECT count(*) as cnt FROM C_DBC");
		//c_dbcDF.show();
		printMessage("C_DBC----------------Ends");
		// 4. C_DBC - Ends

		// 5. C_GAM - Starts
		printMessage("C_GAM----------------Starts");
		Dataset<Row> c_gam = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/C_GAM.txt");
		// c_gam.show();
		c_gam.createOrReplaceTempView("C_GAM");

		//Dataset<Row> c_gamDF = spark.sql("SELECT count(*) as cnt FROM C_GAM");
		//c_gamDF.show();
		printMessage("C_GAM----------------Ends");
		// 5. C_GAM - Ends

		// 6. CAM - Starts
		printMessage("CAM----------------Starts");
		Dataset<Row> cam = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/CAM.txt");
		// cam.show();
		cam.createOrReplaceTempView("CAM");

		//Dataset<Row> camDF = spark.sql("SELECT count(*) as cnt FROM CAM");
		//camDF.show();
		printMessage("CAM----------------Ends");
		// 6. CAM - Ends

		// 7. CFCM - Starts
		printMessage("CFCM----------------Starts");
		Dataset<Row> cfcm = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/cfcm.txt");
		// cfcm.show();
		cfcm.createOrReplaceTempView("CFCM");

		//Dataset<Row> cfcmDF = spark.sql("SELECT count(*) as cnt FROM CFCM");
		//cfcmDF.show();
		printMessage("CFCM----------------Ends");
		// 7. CFCM - Ends

		// 8. COT - Starts
		printMessage("COT----------------Starts");
		Dataset<Row> cot = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/cot.txt");
		// cot.show();
		cot.createOrReplaceTempView("COT");

		//Dataset<Row> cotDF = spark.sql("SELECT count(*) as cnt FROM COT");
		//cotDF.show();
		printMessage("COT----------------Ends");
		// 8. COT - Ends

		// 9. EIT - Starts
		printMessage("EIT----------------Starts");
		Dataset<Row> eit = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/EIT.txt");
		// eit.show();
		eit.createOrReplaceTempView("EIT");

		//Dataset<Row> eitDF = spark.sql("SELECT count(*) as cnt FROM EIT");
		//eitDF.show();
		printMessage("EIT----------------Ends");
		// 9. EIT - Ends

		// 10. GAC - Starts
		printMessage("GAC----------------Starts");
		Dataset<Row> GAC_DF = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/GAC.txt");
		// gac.show();
		GAC_DF.createOrReplaceTempView("GAC");

		//Dataset<Row> gacDF = spark.sql("SELECT count(*) as cnt FROM GAC");
		//gacDF.show();
		printMessage("GAC----------------Ends");
		// 10. GAC - Ends

		// 11. GAM - Starts
		printMessage("GAM----------------Starts");
		Dataset<Row> gam = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/gam.txt");
		// gam.show();
		gam.createOrReplaceTempView("GAM");

		//Dataset<Row> gamDF = spark.sql("SELECT count(*) as cnt FROM GAM");
		//gamDF.show();
		printMessage("GAM----------------Ends");
		// 11. GAM - Ends

		// 12. GCT - Starts
		printMessage("GCT----------------Starts");
		Dataset<Row> gct = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/GCT.txt");
		// gct.show();
		gct.createOrReplaceTempView("GCT");

		//Dataset<Row> gctDF = spark.sql("SELECT count(*) as cnt FROM GCT");
		//gctDF.show();
		printMessage("GCT----------------Ends");
		// 12. GCT - Ends

		// 13. GEM - Starts
		printMessage("GEM----------------Starts");
		Dataset<Row> gem = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/GEM.txt");
		// gem.show();
		gem.createOrReplaceTempView("GEM");

		//Dataset<Row> gemDF = spark.sql("SELECT count(*) as cnt FROM GEM");
		//gemDF.show();
		printMessage("GEM----------------Ends");
		// 13. GEM - Ends

		// 14. GSH - Starts
		printMessage("GSH----------------Starts");
		Dataset<Row> gsh = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/GSH.txt");
		// gsh.show();
		gsh.createOrReplaceTempView("GSH");

		//Dataset<Row> gshDF = spark.sql("SELECT count(*) as cnt FROM GSH");
		//gshDF.show();
		printMessage("GSH----------------Ends");
		// 14. GSH - Ends

		// 15. GSP - Starts
		printMessage("GSP----------------Starts");
		Dataset<Row> gsp = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/gsp.txt");
		// gsp.show();
		gsp.createOrReplaceTempView("GSP");

		//Dataset<Row> gspDF = spark.sql("SELECT count(*) as cnt FROM GSP");
		//gspDF.show();
		printMessage("GSP----------------Ends");
		// 15. GSP - Ends

		// 16. HTD - Starts
		printMessage("HTD----------------Starts");
		Dataset<Row> htd = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/htd.txt");
		// htd.show();
		htd.createOrReplaceTempView("HTD");

		//Dataset<Row> htdDF = spark.sql("SELECT count(*) as cnt FROM HTD");
		//htdDF.show();
		printMessage("HTD----------------Ends");
		// 16. HTD - Ends

		// 17. ITC - Starts
		printMessage("ITC----------------Starts");
		Dataset<Row> itc = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/ITC.txt");
		// itc.show();
		itc.createOrReplaceTempView("ITC");

		//Dataset<Row> itcDF = spark.sql("SELECT count(*) as cnt FROM ITC");
		//itcDF.show();
		printMessage("ITC----------------Ends");
		// 17. ITC - Ends

		// 18. LHT - Starts
		printMessage("LHT----------------Starts");
		Dataset<Row> lht = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/lHT.txt");
		// lht.show();
		lht.createOrReplaceTempView("LHT");

		//Dataset<Row> lhtDF = spark.sql("SELECT count(*) as cnt FROM LHT");
		//lhtDF.show();
		printMessage("LHT----------------Ends");
		// 18. LHT - Ends

		// 19. LLT - Starts
		printMessage("LLT----------------Starts");
		Dataset<Row> llt = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/llt.txt");
		// llt.show();
		llt.createOrReplaceTempView("LLT");

		//Dataset<Row> lltDF = spark.sql("SELECT count(*) as cnt FROM LLT");
		//lltDF.show();
		printMessage("LLT----------------Ends");
		// 19. LLT - Ends

		// 20. LRP - Starts
		printMessage("LRP----------------Starts");
		Dataset<Row> lrp = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/LRP.txt");
		// lrp.show();
		lrp.createOrReplaceTempView("LRP");

		//Dataset<Row> lrpDF = spark.sql("SELECT count(*) as cnt FROM LRP");
		//lrpDF.show();
		printMessage("LRP----------------Ends");
		// 20. LRP - Ends

		// 21. RCT - Starts
		printMessage("RCT----------------Starts");
		Dataset<Row> rct = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/rct.txt");
		// rct.show();
		rct.createOrReplaceTempView("RCT");

	//	Dataset<Row> rctDF = spark.sql("SELECT count(*) as cnt FROM RCT");
		//rctDF.show();
		printMessage("RCT----------------Ends");
		// 21. RCT - Ends

		// 22. SCT - Starts
		printMessage("SCT----------------Starts");
		Dataset<Row> sct = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/SCT.txt");
		// sct.show();
		sct.createOrReplaceTempView("SCT");

		//Dataset<Row> sctDF = spark.sql("SELECT count(*) as cnt FROM SCT");
		//sctDF.show();
		printMessage("SCT----------------Ends");
		// 22. SCT - Ends

		// 23. SVSUSER_NSIGNCUSTINFO - Starts
		printMessage("SVSUSER_NSIGNCUSTINFO----------------Starts");
		Dataset<Row> svsuser_nsigncustinfo = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/SVSUSER_NSIGNCUSTINFO.txt");
		// svsuser_nsigncustinfo.show();
		svsuser_nsigncustinfo.createOrReplaceTempView("SVSUSER_NSIGNCUSTINFO");

		//Dataset<Row> svsuser_nsigncustinfoDF = spark.sql("SELECT count(*) as cnt FROM SVSUSER_NSIGNCUSTINFO");
		//svsuser_nsigncustinfoDF.show();
		printMessage("SVSUSER_NSIGNCUSTINFO----------------Ends");
		// 23. SVSUSER_NSIGNCUSTINFO - Ends

		// 24. SVSUSER_SIGNCUSTINFO - Starts
		printMessage("SVSUSER_SIGNCUSTINFO----------------Starts");
		Dataset<Row> svsuser_signcustinfo = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/SVSUSER_SIGNCUSTINFO.txt");
		// svsuser_signcustinfo.show();
		svsuser_signcustinfo.createOrReplaceTempView("SVSUSER_SIGNCUSTINFO");

		//Dataset<Row> svsuser_signcustinfoDF = spark.sql("SELECT count(*) as cnt FROM SVSUSER_SIGNCUSTINFO");
		//svsuser_signcustinfoDF.show();
		printMessage("SVSUSER_SIGNCUSTINFO----------------Ends");
		// 24. SVSUSER_SIGNCUSTINFO - Ends

		// 25. TA_COT - Starts
		printMessage("TA_COT----------------Starts");
		Dataset<Row> ta_cot = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/TA_COT.txt");
		// ta_cot.show();
		ta_cot.createOrReplaceTempView("TA_COT");

		//Dataset<Row> ta_cotDF = spark.sql("SELECT count(*) as cnt FROM TA_COT");
		//ta_cotDF.show();
		printMessage("TA_COT----------------Ends");
		// 25. TA_COT - Ends

		// 26. TAM - Starts
		printMessage("TAM----------------Starts");
		Dataset<Row> tam = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/TAM.txt");
		// tam.show();
		tam.createOrReplaceTempView("TAM");

		//Dataset<Row> tamDF = spark.sql("SELECT count(*) as cnt FROM TAM");
		//tamDF.show();
		printMessage("TAM----------------Ends");
		// 26. TAM - Ends
		
		// 27. LAM - Starts
		printMessage("LAM----------------Starts");
		Dataset<Row> lam = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/LAM.txt");
		// lam.show();
		lam.createOrReplaceTempView("LAM");

		//Dataset<Row> lamDF = spark.sql("SELECT count(*) as cnt FROM LAM");
		//lamDF.show();
		printMessage("LAM----------------Ends");
		// 27. LAM - Ends
		
		// 28. SOL - Starts
		printMessage("SOL----------------Starts");
		Dataset<Row> sol = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/sol.txt");
		// sol.show();
		sol.createOrReplaceTempView("SOL");

		//Dataset<Row> solDF = spark.sql("SELECT count(*) as cnt FROM SOL");
		//solDF.show();
		printMessage("SOL----------------Ends");
		// 28. SOL - Ends
		
		// 29. SMT - Starts
		printMessage("SMT----------------Starts");
		Dataset<Row> smt = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/smt.txt");
		// smt.show();
		smt.createOrReplaceTempView("SMT");

		//Dataset<Row> smtDF = spark.sql("SELECT count(*) as cnt FROM SMT");
		//smtDF.show();
		printMessage("SMT----------------Ends");
		// 29. SMT - Ends
		
		// 30. LA_SAM - Starts
		printMessage("LA_SAM----------------Starts");
		Dataset<Row> LA_SAM_DF = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("header", "true").load("resources/La_SAM.txt");
		// la_sam.show();
		LA_SAM_DF.createOrReplaceTempView("LA_SAM");

		//Dataset<Row> la_samDF = spark.sql("SELECT count(*) as cnt FROM LA_SAM");
		//la_samDF.show();
		printMessage("LA_SAM----------------Ends");
		// 30. LA_SAM - Ends
		
		
//Reverse parsing the individual query's used in joins
		// 1. S_DF - Starts
		printMessage("S_DF----------------Starts");
//		Dataset<Row> S_DF = spark.sql("SELECT CUSTID,\r\n" +
//				"CASE\r\n" +
//				"WHEN Z1.TAG = '1' THEN\r\n" +
//				"'APPROVED'\r\n" +
//				"WHEN Z1.TAG = '3' THEN\r\n" +
//				"'NOT APPROVED'\r\n" +
//				"ELSE\r\n" +
//				"'NOT LINKED'\r\n" +
//				"END SIGNATURE_STATUS\r\n" +
//				"FROM (SELECT ROW_NUMBER() OVER(PARTITION BY CUSTID ORDER BY CUSTID, MODIFIEDDATE DESC) SRNO,\r\n" +
//				"Z.*\r\n" +
//				"FROM (SELECT CUSTID,\r\n" +
//				"'1' AS TAG,\r\n" +
//				"MODIFIEDDATE\r\n" +
//				"FROM SVSUSER_SIGNCUSTINFO\r\n" +
//				"WHERE TRIM(CUSTID) IS NOT NULL\r\n" +
//				"UNION ALL\r\n" +
//				"SELECT CUSTID,\r\n" +
//				"'3' AS TAG,\r\n" +
//				"MODIFIEDDATE\r\n" +
//				"FROM SVSUSER_NSIGNCUSTINFO\r\n" +
//				"WHERE TRIM(CUSTID) IS NOT NULL) Z) Z1\r\n" +
//				"WHERE SRNO = 1"
//				+ "");
		
		//Fetch S DF

				Dataset<Row> S_DF = funcFetchSDF(spark);

				printMessage(" No of rows = " + S_DF.count() + "  & no of colums in S_DF  = " + S_DF.columns().length);

				

				// 1. S_DF - Ends
		
		S_DF.createOrReplaceTempView("S");
		//S_DF.show();
		printMessage("S_DF----------------Ends");
		// 1. S_DF - Ends

		// 2. PR_DF - Starts
		printMessage("PR_DF----------------Starts");
//		Dataset<Row> PR_DF = spark.sql("SELECT ENTITY_ID, NEXT_PEG_REVIEW_DATE\r\n" +
//				"FROM EIT\r\n" +
//				"WHERE NEXT_PEG_REVIEW_DATE IS NOT NULL"
//				+ "");
		
		Dataset<Row> EIT = eit.filter("NEXT_PEG_REVIEW_DATE != 'NULL'");
		StructField[] PR_DFOutschemaFields = {
                new StructField("ENTITY_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("NEXT_PEG_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty())
		};
				
		StructType PR_DFOutSchema = new StructType(PR_DFOutschemaFields);
        ExpressionEncoder<Row> PR_DFencoder = RowEncoder.apply(PR_DFOutSchema);
		
		Dataset<Row> PR_DF  = EIT.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String ENTITY_ID = record.getAs("ENTITY_ID");
                String NEXT_PEG_REVIEW_DATE = record.getAs("NEXT_PEG_REVIEW_DATE");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ENTITY_ID, NEXT_PEG_REVIEW_DATE);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, PR_DFencoder);
		PR_DF.createOrReplaceTempView("PR");
		//PR_DF.show();
		printMessage("SplitQ_506_508_DF----------------Ends");
		
	
		
		
		// 2. PR_DF - Ends

		// 3. V_DF - Starts
		printMessage("V_DF----------------Starts");
//		Dataset<Row> V_DF = spark.sql("SELECT ACID,\r\n" +
//				"date_format(MAX(CHRGE_OFF_DATE), 'YYYY-MM-DD') CHRGE_OFF_DATE\r\n" +
//				"FROM (SELECT ACID, CHRGE_OFF_PRINCIPAL, CHRGE_OFF_DATE\r\n" +
//				"FROM TA_COT\r\n" +
//				"WHERE ENTITY_CRE_FLG = 'Y'\r\n" +
//				"AND DEL_FLG = 'N'\r\n" +
//				"UNION ALL\r\n" +
//				"SELECT ACID, CHRGE_OFF_PRINCIPAL, CHRGE_OFF_DATE\r\n" +
//				"FROM COT\r\n" +
//				"WHERE ENTITY_CRE_FLG = 'Y'\r\n" +
//				"AND DEL_FLG = 'N') T\r\n" +
//				"WHERE T.CHRGE_OFF_DATE IS NOT NULL\r\n" +
//				"GROUP BY ACID"
//				+ "");
		
				Dataset<Row> T = ta_cot.select("ACID", "CHRGE_OFF_PRINCIPAL" ,"CHRGE_OFF_DATE").filter("ENTITY_CRE_FLG = 'Y' and DEL_FLG = 'N'")
				                 .union(cot
				                		 .select("ACID", "CHRGE_OFF_PRINCIPAL", "CHRGE_OFF_DATE").filter("ENTITY_CRE_FLG = 'Y' and DEL_FLG = 'N'"));
				
				Dataset<Row> V_DF = T.select("ACID", "CHRGE_OFF_DATE").filter("CHRGE_OFF_DATE != 'NULL'")
				.groupBy("ACID").agg(max("CHRGE_OFF_DATE").alias("CHRGE_OFF_DATE"));
						
		
		V_DF.createOrReplaceTempView("V");
		//V_DF.show();
		printMessage("V_DF----------------Ends");
		// 3. V_DF - Ends

		// 4. RCT_AI - Starts
		printMessage("RCT_AI----------------Starts");
		Dataset<Row> RCT_AI_DF = spark.sql("SELECT REF_CODE as RCT_AI_REF_CODE,REF_DESC as RCT_AI_REF_DESC\r\n" +
				"FROM RCT\r\n" +
				"WHERE REF_REC_TYPE = 'AI'"
				+ "");
		RCT_AI_DF.createOrReplaceTempView("RCT_AI");
		//RCT_AI_DF.show();
		printMessage("RCT_AI_DF----------------Ends");
		// 4. RCT_AI_DF - Ends

		// 4.1 LLT - Starts
		printMessage("LLT_DF----------------Starts");
//		Dataset<Row> LLT_DF = spark.sql("SELECT LIMIT_B2KID, LIM_EXP_DATE, LIMIT_REVIEW_DATE\r\n" +
//				"FROM LLT"
//				+ "");
//		
		StructField[] LLT_DFOutschemaFields = {
                new StructField("LIMIT_B2KID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("LIM_EXP_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("LIMIT_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty())             
		};
		
		
		StructType LLT_DFOutSchema = new StructType(LLT_DFOutschemaFields);
        ExpressionEncoder<Row> LLT_DFencoder = RowEncoder.apply(LLT_DFOutSchema);
		
		Dataset<Row> LLT_DF  = llt.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String LIMIT_B2KID = record.getAs("LIMIT_B2KID");
                String LIM_EXP_DATE = record.getAs("LIM_EXP_DATE");
                String LIMIT_REVIEW_DATE = record.getAs("LIMIT_REVIEW_DATE");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(LIMIT_B2KID, LIM_EXP_DATE, LIMIT_REVIEW_DATE);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, LLT_DFencoder);
		
		LLT_DF.createOrReplaceTempView("LLT");
		//LLT_DF.show();
		printMessage("LLT_DF----------------Ends");
		// 4.1 LLT_DF - Ends

		// 5. C_DBC - Starts
		printMessage("C_DBC----------------Starts");
//		Dataset<Row> C_DBC_DF = spark.sql("SELECT FORACID, DEBIT_CARD FROM C_DBC"
//				+ "");
		
		StructField[] C_DBCOutschemaFields = {
                new StructField("FORACID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DEBIT_CARD", DataTypes.StringType, true, Metadata.empty())            
		};
		
		
		StructType C_DBCOutSchema = new StructType(C_DBCOutschemaFields);
        ExpressionEncoder<Row> C_DBCencoder = RowEncoder.apply(C_DBCOutSchema);
		
		Dataset<Row> C_DBC_DF  = c_dbc.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String FORACID = record.getAs("FORACID");
                String DEBIT_CARD = record.getAs("DEBIT_CARD");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(FORACID, DEBIT_CARD);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, C_DBCencoder);
		
		C_DBC_DF.createOrReplaceTempView("C_DBC");
		//C_DBC.show();
		printMessage("C_DBC----------------Ends");
		// 5. C_DBC - Ends

		
		// 6. ACD_DF - Starts
		printMessage("ACD_DF----------------Starts");
//		Dataset<Row> ACD_DF = spark.sql("SELECT B2K_ID, MAIN_CLASSIFICATION_USER FROM ACD"
//				+ "");
		
		StructField[] ACD_DFschemaFields = {
                new StructField("B2K_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("MAIN_CLASSIFICATION_USER", DataTypes.StringType, true, Metadata.empty())            
		};
		
		
		StructType ACD_DFSchema = new StructType(ACD_DFschemaFields);
        ExpressionEncoder<Row> ACD_DFencoder = RowEncoder.apply(ACD_DFSchema);
		
		Dataset<Row> ACD_DF  = acd.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String B2K_ID = record.getAs("B2K_ID");
                String MAIN_CLASSIFICATION_USER = record.getAs("MAIN_CLASSIFICATION_USER");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(B2K_ID, MAIN_CLASSIFICATION_USER);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, ACD_DFencoder);

		
		ACD_DF.createOrReplaceTempView("ACD");
		//ACD_DF.show();
		printMessage("ACD_DF----------------Ends");
		// 6. ACD_DF - Ends

		// 7. RCT_EY - Starts
		printMessage("RCT_EY----------------Starts");
//		Dataset<Row> RCT_EY_DF = spark.sql("SELECT REF_CODE as RCT_EY_REF_CODE, REF_DESC as RCT_EY_REF_DESC\r\n" +
//				"FROM RCT\r\n" +
//				"WHERE REF_REC_TYPE = 'EY'"
//				+ "");
		
		Dataset<Row> RCT = rct.filter("REF_REC_TYPE == 'EY'");

		StructField[] RCT_EYschemaFields = {
                new StructField("RCT_EY_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_EY_REF_DESC", DataTypes.StringType, true, Metadata.empty())           
		};
		
		
		StructType RCT_EYSchema = new StructType(RCT_EYschemaFields);
        ExpressionEncoder<Row> RCT_EYencoder = RowEncoder.apply(RCT_EYSchema);
		
		Dataset<Row> RCT_EY_DF  = RCT.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_EY_REF_CODE = record.getAs("REF_CODE");
                String RCT_EY_REF_DESC = record.getAs("REF_DESC");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_EY_REF_CODE, RCT_EY_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_EYencoder);
		
		RCT_EY_DF.createOrReplaceTempView("RCT_EY");
		//RCT_EY_DF.show();
		printMessage("RCT_EY----------------Ends");
		// 7. RCT_EY - Ends

		// 8. LAM - Starts -- LAM Table not present
		printMessage("LAM----------------Starts");
//		Dataset<Row> LAM_DF = spark.sql("SELECT ACID,\r\n" +
//				"REP_PERD_MTHS,\r\n" +
//				"REP_PERD_DAYS,\r\n" +
//				"OP_ACID,\r\n" +
//				"PAYOFF_FLG,\r\n" +
//				"ACCT_STATUS_FLG,\r\n" +
//				"EI_PERD_START_DATE,\r\n" +
//				"PAYOFF_DATE,\r\n" +
//				"EI_PERD_END_DATE,\r\n" +
//				"PAYOFF_REASON_CODE,\r\n" +
//				"CRFILE_REF_ID,\r\n" +
//				"CASE\r\n" +
//				"WHEN EI_SCHM_FLG = 'Y' THEN\r\n" +
//				"'EI'\r\n" +
//				"ELSE\r\n" +
//				"'NON_EI'\r\n" +
//				"END AS EMI_TYPE,\r\n" +
//				"DMD_SATISFY_MTHD AS PAYMENT_METHOD\r\n" +
//				"FROM LAM"
//				+ "");
//		
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
		
		Dataset<Row> LAM_DF  = lam.flatMap(new FlatMapFunction<Row, Row>() {
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

		
		LAM_DF.createOrReplaceTempView("LAM");
		//LAM_DF.show();
		printMessage("LAM_DF----------------Ends");
		// 8. LAM_DF - Ends

		// 9. LHT_DF - Starts
		printMessage("LHT_DF----------------Starts");
//		Dataset<Row> LHT_DF = spark.sql("SELECT A.ACID,\r\n" +
//				"SUM(A.SANCT_LIM) SANCT_LIM,\r\n" +
//				"MIN(A.LIM_SANCT_DATE) LIM_SANCT_DATE,\r\n" +
//				"MIN(A.LIM_REVIEW_DATE) LIM_REVIEW_DATE,\r\n" +
//				"MIN(A.LIM_EXP_DATE) LIM_EXP_DATE,\r\n" +
//				"MIN(A.APPLICABLE_DATE) APPLICABLE_DATE\r\n" +
//				"FROM LHT A\r\n" +
//				"WHERE A.STATUS = 'A'\r\n" +
//				"AND ENTITY_CRE_FLG = 'Y'\r\n" +
//				"AND DEL_FLG = 'N'\r\n" +
//				"AND A.applicable_date <= trunc(date_add(current_date(), -1), 'DD')\r\n" +
//				"GROUP BY ACID"
//				+ "");
		
		Dataset<Row> LHT_select = lht.select("ACID", "SANCT_LIM", "LIM_SANCT_DATE", "LIM_REVIEW_DATE", "LIM_EXP_DATE", "APPLICABLE_DATE"
				, "STATUS", "ENTITY_CRE_FLG", "DEL_FLG")
				.filter("STATUS ='A' and ENTITY_CRE_FLG = 'y' and DEL_FLG = 'N'")
				.groupBy("ACID")
				.agg(sum("SANCT_LIM"), min("LIM_SANCT_DATE"), min("LIM_REVIEW_DATE"), min("LIM_EXP_DATE"), min("APPLICABLE_DATE"))
				.filter(col("min(APPLICABLE_DATE)").$less$eq("2017-05-22"));
		  
		
		StructField[] LHT_DFschemaFields = {
                new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("SANCT_LIM", DataTypes.StringType, true, Metadata.empty()),          
                new StructField("LIM_SANCT_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("LIM_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("LIM_EXP_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("APPLICABLE_DATE", DataTypes.StringType, true, Metadata.empty()) 
                };
		
		
		StructType LHT_DFSchema = new StructType(LHT_DFschemaFields);
        ExpressionEncoder<Row> LHT_DFencoder = RowEncoder.apply(LHT_DFSchema);
		
		Dataset<Row> LHT_DF  = LHT_select.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String ACID = record.getAs("ACID");
                String SANCT_LIM = record.getAs("sum(SANCT_LIM)");
                String LIM_SANCT_DATE = record.getAs("min(LIM_SANCT_DATE)");
                String LIM_REVIEW_DATE = record.getAs("min(LIM_REVIEW_DATE)");
                String LIM_EXP_DATE = record.getAs("min(LIM_EXP_DATE)");
                String APPLICABLE_DATE = record.getAs("min(APPLICABLE_DATE)");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ACID, SANCT_LIM,LIM_SANCT_DATE , LIM_REVIEW_DATE, LIM_EXP_DATE, APPLICABLE_DATE);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, LHT_DFencoder);
		
		LHT_DF.show();

		
		LHT_DF.createOrReplaceTempView("LHT");
		//LHT_DF.show();
		printMessage("LHT_DF----------------Ends");
		// 9. LHT_DF - Ends

		// 10. CFCM_DF - Starts
		printMessage("CFCM_DF----------------Starts");
//		Dataset<Row> CFCM_DF = spark.sql("SELECT ENTITY_ID,\r\n" +
//				"FREE_CODE_20,\r\n" +
//				"FREE_CODE_22,\r\n" +
//				"FREE_CODE_21,\r\n" +
//				"FREE_CODE_5 as CFCM_FREE_CODE_5,\r\n" +
//				"FREE_CODE_6 as CFCM_FREE_CODE_6,\r\n" +
//				"FREE_CODE_1,\r\n" +
//				"FREE_CODE_43\r\n" +
//				"FROM CFCM\r\n" +
//				"WHERE CFCM.ENTITY_TYPE = 'A'"
//				+ "");
		
		Dataset<Row> cfcm_filter = cfcm.filter("ENTITY_TYPE = 'A'");
		
//		cfcm_filter.show();	

		
		StructField[] CFCM_DFschemaFields = {
                new StructField("ENTITY_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("FREE_CODE_20", DataTypes.StringType, true, Metadata.empty()),
                new StructField("FREE_CODE_22", DataTypes.StringType, true, Metadata.empty()),
                new StructField("FREE_CODE_21", DataTypes.StringType, true, Metadata.empty()),
                new StructField("CFCM_FREE_CODE_5", DataTypes.StringType, true, Metadata.empty()),
                new StructField("CFCM_FREE_CODE_6", DataTypes.StringType, true, Metadata.empty()),
                new StructField("FREE_CODE_1", DataTypes.StringType, true, Metadata.empty()),
                new StructField("FREE_CODE_43", DataTypes.StringType, true, Metadata.empty())
		};
		
		StructType CFCM_DFSchema = new StructType(CFCM_DFschemaFields);
        ExpressionEncoder<Row> CFCM_DFencoder = RowEncoder.apply(CFCM_DFSchema);
		
		Dataset<Row> CFCM_DF  = cfcm_filter.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
            	String ENTITY_ID = record.getAs("ENTITY_ID");
                String FREE_CODE_20 = record.getAs("FREE_CODE_20");
                String FREE_CODE_22 = record.getAs("FREE_CODE_22");
                String FREE_CODE_21 = record.getAs("FREE_CODE_21");
                String CFCM_FREE_CODE_5 = record.getAs("FREE_CODE_5");
                String CFCM_FREE_CODE_6 = record.getAs("FREE_CODE_6");
                String FREE_CODE_1 = record.getAs("FREE_CODE_1");
                String FREE_CODE_43 = record.getAs("FREE_CODE_43");

                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ENTITY_ID, FREE_CODE_20, FREE_CODE_22, FREE_CODE_21, CFCM_FREE_CODE_5, CFCM_FREE_CODE_6
                		, FREE_CODE_1, FREE_CODE_43);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, CFCM_DFencoder);

		
		CFCM_DF.createOrReplaceTempView("CFCM");
		//CFCM_DF.show();
		printMessage("CFCM_DF----------------Ends");
		// 10. CFCM_DF - Ends

		// 11. RCT_31_DF - Starts
		printMessage("RCT_31_DF----------------Starts");
//		Dataset<Row> RCT_31_DF = spark.sql("SELECT REF_CODE as RCT_31_REF_CODE, REF_DESC as RCT_31_REF_DESC\r\n" +
//				"FROM RCT\r\n" +
//				"WHERE REF_REC_TYPE = '31'"
//				+ "");
		
		Dataset<Row> RCT31 = rct.filter("REF_REC_TYPE = '31'");
		
//		RCT32.show();

		StructField[] RCT_31_DFschemaFields = {
                new StructField("RCT_31_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_31_REF_DESC", DataTypes.StringType, true, Metadata.empty())          
		};
		
		
		StructType RCT_31_DFSchema = new StructType(RCT_31_DFschemaFields);
        ExpressionEncoder<Row> RCT_31_DFencoder = RowEncoder.apply(RCT_31_DFSchema);
		
		Dataset<Row> RCT_31_DF  = RCT31.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_31_REF_CODE = record.getAs("REF_CODE");
                String RCT_31_REF_DESC = record.getAs("REF_DESC");
                
                
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_31_REF_CODE, RCT_31_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_31_DFencoder);

		RCT_31_DF.createOrReplaceTempView("RCT_31");
		//RCT_31_DF.show();
		printMessage("RCT_31_DF----------------Ends");
		// 11. RCT_31_DF - Ends

		// 12. RCT_CN_DF - Starts
		printMessage("RCT_CN_DF----------------Starts");
//		Dataset<Row> RCT_CN_DF = spark.sql("SELECT REF_CODE as RCT_CN_REF_CODE, REF_DESC as RCT_CN_REF_DESC\r\n" +
//				"FROM RCT\r\n" +
//				"WHERE REF_REC_TYPE = 'CN'"
//				+ "");
		
		Dataset<Row> RCT_CN = rct.filter("REF_REC_TYPE = 'CN'");
		
		
		StructField[] RCT_CN_DFschemaFields = {
                new StructField("RCT_CN_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_CN_REF_DESC", DataTypes.StringType, true, Metadata.empty())          
		};
		
		
		StructType RCT_CN_DFSchema = new StructType(RCT_CN_DFschemaFields);
        ExpressionEncoder<Row> RCT_CN_DFencoder = RowEncoder.apply(RCT_CN_DFSchema);
		
		Dataset<Row> RCT_CN_DF  = RCT_CN.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_CN_REF_CODE = record.getAs("REF_CODE");
                String RCT_CN_REF_DESC = record.getAs("REF_DESC");
                

                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_CN_REF_CODE, RCT_CN_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_CN_DFencoder);
			
		
		RCT_CN_DF.createOrReplaceTempView("RCT_CN");
		//RCT_CN_DF.show();
		printMessage("RCT_CN_DF----------------Ends");
		// 12. RCT_CN_DF - Ends

		// 13. RCT_27_DF - Starts
		printMessage("RCT_27_DF----------------Starts");
//		Dataset<Row> RCT_27_DF = spark.sql("SELECT REF_CODE as RCT_27_REF_CODE, REF_DESC as RCT_27_REF_DESC\r\n" +
//				"FROM RCT\r\n" +
//				"WHERE REF_REC_TYPE = '27'"
//				+ "");
		
		Dataset<Row> RCT_27 = rct.filter("REF_REC_TYPE = '27'");

		StructField[] RCT_27_DFschemaFields = {
                new StructField("RCT_27_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_27_REF_DESC", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType RCT_27_DFSchema = new StructType(RCT_27_DFschemaFields);
        ExpressionEncoder<Row> RCT_27_DFencoder = RowEncoder.apply(RCT_27_DFSchema);
		
		Dataset<Row> RCT_27_DF  = RCT_27.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_27_REF_CODE = record.getAs("REF_CODE");
                String RCT_27_REF_DESC = record.getAs("REF_DESC");
              
             
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_27_REF_CODE, RCT_27_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_27_DFencoder);
			
		
		RCT_27_DF.createOrReplaceTempView("RCT_27");
		//RCT_27_DF.show();
		printMessage("RCT_27_DF----------------Ends");
		// 13. RCT_27_DF - Ends

		// 14. RCT_OC_DF - Starts
		printMessage("RCT_OC_DF----------------Starts");
//		Dataset<Row> RCT_OC_DF = spark.sql("SELECT REF_CODE as RCT_OC_REF_CODE, REF_DESC as RCT_OC_REF_DESC\r\n" +
//				"FROM RCT\r\n" +
//				"WHERE REF_REC_TYPE = '21'"
//				+ "");
		
		Dataset<Row> RCT_OC = rct.filter("REF_REC_TYPE = '21'");


		StructField[] RCT_OC_DFschemaFields = {
                new StructField("RCT_OC_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_OC_REF_DESC", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType RCT_OC_DFSchema = new StructType(RCT_OC_DFschemaFields);
        ExpressionEncoder<Row> RCT_OC_DFencoder = RowEncoder.apply(RCT_OC_DFSchema);
		
		Dataset<Row> RCT_OC_DF  = RCT_OC.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_OC_REF_CODE = record.getAs("REF_CODE");
                String RCT_OC_REF_DESC = record.getAs("REF_DESC");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_OC_REF_CODE, RCT_OC_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_OC_DFencoder);

		RCT_OC_DF.createOrReplaceTempView("RCT_OC");
		//RCT_OC_DF.show();
		printMessage("RCT_OC_DF----------------Ends");
		// 14. RCT_OC_DF - Ends

		// 15. RCT_AF_DF - Starts
		printMessage("RCT_AF_DF----------------Starts");
//		Dataset<Row> RCT_AF_DF = spark.sql("SELECT REF_CODE as RCT_AF_REF_CODE, REF_DESC as RCT_AF_REF_DESC\r\n" +
//				"FROM RCT\r\n" +
//				"WHERE REF_REC_TYPE = 'AF'"
//				+ "");
		
		Dataset<Row> RCT_AF = rct.filter("REF_REC_TYPE = 'AF'");


		StructField[] RCT_AF_DFschemaFields = {
                new StructField("RCT_AF_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_AF_REF_DESC", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType RCT_AF_DFSchema = new StructType(RCT_AF_DFschemaFields);
        ExpressionEncoder<Row> RCT_AF_DFencoder = RowEncoder.apply(RCT_AF_DFSchema);
		
		Dataset<Row> RCT_AF_DF  = RCT_AF.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_AF_REF_CODE = record.getAs("REF_CODE");
                String RCT_AF_REF_DESC = record.getAs("REF_DESC");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_AF_REF_CODE, RCT_AF_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_AF_DFencoder);

		
		RCT_AF_DF.createOrReplaceTempView("RCT_AF");
		//RCT_AF_DF.show();
		printMessage("RCT_AF_DF----------------Ends");
		// 15. RCT_AF_DF - Ends

		// 16. TAM_DF - Starts
		printMessage("TAM_DF----------------Starts");
//		Dataset<Row> TAM_DF = spark.sql("SELECT ACID,\r\n" + 
//				"CLOSE_ON_MATURITY_FLG,\r\n" + 
//				"AUTO_RENEWAL_FLG,\r\n" + 
//				"DEPOSIT_PERIOD_MTHS,\r\n" + 
//				"DEPOSIT_PERIOD_DAYS,\r\n" + 
//				"ACCT_STATUS,\r\n" + 
//				"LINK_OPER_ACCOUNT,\r\n" + 
//				"OPEN_EFFECTIVE_DATE,\r\n" + 
//				"MATURITY_DATE,\r\n" + 
//				"MATURITY_AMOUNT,\r\n" + 
//				"DEPOSIT_AMOUNT,\r\n" + 
//				"DEPOSIT_STATUS,\r\n" + 
//				"REPAYMENT_ACID\r\n" + 
//				"FROM TAM"
//				+ "");
		
		StructField[] TAM_DFschemaFields = {
                new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("CLOSE_ON_MATURITY_FLG", DataTypes.StringType, true, Metadata.empty()),
                new StructField("AUTO_RENEWAL_FLG", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DEPOSIT_PERIOD_MTHS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DEPOSIT_PERIOD_DAYS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACCT_STATUS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("LINK_OPER_ACCOUNT", DataTypes.StringType, true, Metadata.empty()),
                new StructField("OPEN_EFFECTIVE_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("MATURITY_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("MATURITY_AMOUNT", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DEPOSIT_AMOUNT", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DEPOSIT_STATUS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("REPAYMENT_ACID", DataTypes.StringType, true, Metadata.empty())
		};
		
		StructType TAM_DFSchema = new StructType(TAM_DFschemaFields);
        ExpressionEncoder<Row> TAM_DFencoder = RowEncoder.apply(TAM_DFSchema);
		
		Dataset<Row> TAM_DF  = tam.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
            	String ACID = record.getAs("ACID");
                String CLOSE_ON_MATURITY_FLG = record.getAs("CLOSE_ON_MATURITY_FLG");
                String AUTO_RENEWAL_FLG = record.getAs("AUTO_RENEWAL_FLG");
                String DEPOSIT_PERIOD_MTHS = record.getAs("DEPOSIT_PERIOD_MTHS");
                String DEPOSIT_PERIOD_DAYS = record.getAs("DEPOSIT_PERIOD_DAYS");
                String ACCT_STATUS = record.getAs("ACCT_STATUS");
                String LINK_OPER_ACCOUNT = record.getAs("LINK_OPER_ACCOUNT");
                String OPEN_EFFECTIVE_DATE = record.getAs("OPEN_EFFECTIVE_DATE");
                String MATURITY_DATE = record.getAs("MATURITY_DATE");
                String MATURITY_AMOUNT = record.getAs("MATURITY_AMOUNT");
                String DEPOSIT_AMOUNT = record.getAs("DEPOSIT_AMOUNT");
                String DEPOSIT_STATUS = record.getAs("DEPOSIT_STATUS");
                String REPAYMENT_ACID = record.getAs("REPAYMENT_ACID");

                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ACID, CLOSE_ON_MATURITY_FLG, AUTO_RENEWAL_FLG, DEPOSIT_PERIOD_MTHS, DEPOSIT_PERIOD_DAYS
                		, ACCT_STATUS, LINK_OPER_ACCOUNT,  OPEN_EFFECTIVE_DATE, MATURITY_DATE,MATURITY_AMOUNT, DEPOSIT_AMOUNT , DEPOSIT_STATUS, REPAYMENT_ACID);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, TAM_DFencoder);
		
		TAM_DF.createOrReplaceTempView("TAM");
		//TAM_DF.show();
		printMessage("TAM_DF----------------Ends");
		// 16. TAM_DF - Ends

		
		// 17. RCT_38_DF - Starts
		printMessage("RCT_38_DF----------------Starts");
//		Dataset<Row> RCT_38_DF = spark.sql("SELECT REF_CODE as RCT_38_REF_CODE, REF_DESC as RCT_38_REF_DESC\r\n" + 
//				"FROM RCT\r\n" + 
//				"WHERE REF_REC_TYPE = '38'"
//				+ "");
		
		Dataset<Row> RCT_38 = rct.filter("REF_REC_TYPE = '38'");


		StructField[] RCT_38_DFschemaFields = {
                new StructField("RCT_38_REF_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("RCT_38_REF_DESC", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType RCT_38_DFSchema = new StructType(RCT_38_DFschemaFields);
        ExpressionEncoder<Row> RCT_38_DFencoder = RowEncoder.apply(RCT_38_DFSchema);
		
		Dataset<Row> RCT_38_DF  = RCT_38.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String RCT_38_REF_CODE = record.getAs("REF_CODE");
                String RCT_38_REF_DESC = record.getAs("REF_DESC");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(RCT_38_REF_CODE, RCT_38_REF_DESC);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_38_DFencoder);

		
		RCT_38_DF.createOrReplaceTempView("RCT_38");
		//RCT_38_DF.show();
		printMessage("RCT_38_DF----------------Ends");
		// 17. RCT_38_DF - Ends

		// 18. SOL_DF - Starts -- SOL Table not present
		printMessage("SOL_DF----------------Starts");
//		Dataset<Row> SOL_DF = spark.sql("SELECT DISTINCT SOL_ID, SOL_DESC BRANCH_NAME\r\n" + 
//				"FROM SOL\r\n" + 
//				"WHERE DEL_FLG = 'N'\r\n" + 
//				"AND BANK_CODE = '176'"
//				+ "");
		
		Dataset<Row> SOL_DF_filter = sol.select("SOL_ID","SOL_DESC").distinct().filter("DEL_FLG == 'N' and BANK_CODE == '176'");


		StructField[] SOL_DFschemaFields = {
                new StructField("SOL_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("BRANCH_NAME", DataTypes.StringType, true, Metadata.empty())
		};
		
		StructType SOL_DFSchema = new StructType(SOL_DFschemaFields);
        ExpressionEncoder<Row> SOL_DFencoder = RowEncoder.apply(SOL_DFSchema);
		
		Dataset<Row> SOL_DF  = SOL_DF_filter.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
            	String SOL_ID = record.getAs("SOL_ID");
                String BRANCH_NAME = record.getAs("SOL_DESC");

                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(SOL_ID, BRANCH_NAME);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, SOL_DFencoder);

		
		SOL_DF.createOrReplaceTempView("SOL");
		//SOL_DF.show();
		printMessage("SOL_DF----------------Ends");
	
		// 18. SOL_DF - Ends

		// 19. RCT_CAM_DF - Starts
		printMessage("RCT_CAM_DF----------------Starts");
//		Dataset<Row> RCT_CAM_DF = spark.sql("SELECT DISTINCT REF_CODE, REF_DESC ACCT_CLS_REASON_DESC\r\n" + 
//				"FROM RCT\r\n" + 
//				"WHERE REF_REC_TYPE = 'GX'"
//				+ "");
//		
		Dataset<Row> RCT_CAM = rct.select("REF_CODE","REF_DESC").distinct().filter("REF_REC_TYPE == 'GX'");

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
                String ACCT_CLS_REASON_DESC = record.getAs("REF_DESC");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(REF_CODE, ACCT_CLS_REASON_DESC );
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, RCT_CAM_DFencoder);

		
		RCT_CAM_DF.createOrReplaceTempView("RCT_CAM");
		//RCT_CAM_DF.show();
		printMessage("RCT_CAM_DF----------------Ends");
		// 19. RCT_CAM_DF - Ends

		// 20. CAM_DF - Starts
		printMessage("CAM_DF----------------Starts");
//		Dataset<Row> CAM_DF = spark.sql("SELECT ACID,\r\n" + 
//				"ACCT_STATUS,\r\n" + 
//				"ACCT_STATUS_DATE,\r\n" + 
//				"ACCT_CLS_REASON_CODE\r\n" + 
//				"FROM CAM\r\n" + 
//				"WHERE DEL_FLG = 'N'\r\n" + 
//				"AND ENTITY_CRE_FLG = 'Y'\r\n" + 
//				"UNION ALL\r\n" + 
//				"SELECT ACID,\r\n" + 
//				"ACCT_STATUS,\r\n" + 
//				"ACCT_STATUS_DATE,\r\n" + 
//				"ACCT_CLS_REASON_CODE\r\n" + 
//				"FROM SMT"
//				+ "");
		
		Dataset<Row> CAM_DFs = cam.select("ACID", "ACCT_STATUS","ACCT_STATUS_DATE", "ACCT_CLS_REASON_CODE").filter("DEL_FLG == 'N' and ENTITY_CRE_FLG == 'y'")
				.union(smt.select("ACID", "ACCT_STATUS","ACCT_STATUS_DATE", "ACCT_CLS_REASON_CODE"));


		StructField[] CAM_DFschemaFields = {
                new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACCT_STATUS", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACCT_STATUS_DATE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACCT_CLS_REASON_CODE", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType CAM_DFSchema = new StructType(CAM_DFschemaFields);
        ExpressionEncoder<Row> CAM_DFencoder = RowEncoder.apply(CAM_DFSchema);
		
		Dataset<Row> CAM_DF  = CAM_DFs.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String ACID = record.getAs("ACID");
                String ACCT_STATUS = record.getAs("ACCT_STATUS");
                String ACCT_STATUS_DATE = record.getAs("ACCT_STATUS_DATE");
                String ACCT_CLS_REASON_CODE = record.getAs("ACCT_CLS_REASON_CODE");
//                String REF_REC_TYPE = record.getAs("REF_REC_TYPE");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ACID, ACCT_STATUS, ACCT_STATUS_DATE, ACCT_CLS_REASON_CODE );
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, CAM_DFencoder);

		CAM_DF.createOrReplaceTempView("CAM");
		//CAM_DF.show();
		printMessage("CAM_DF----------------Ends");
		// 20. CAM_DF - Ends

		
		// 21. ITC_DF - Starts
		printMessage("ITC_DF----------------Starts");
//		Dataset<Row> ITC_DF = spark.sql("SELECT A.ENTITY_ID,\r\n" + 
//				"A.INT_TBL_CODE_SRL_NUM,\r\n" + 
//				"PEGGED_FLG,\r\n" + 
//				"A.INT_TBL_CODE,\r\n" + 
//				"NRML_PCNT_CR,\r\n" + 
//				"REASON_CODE,\r\n" + 
//				"A.PEG_REVIEW_DATE,\r\n" + 
//				"A.END_DATE,\r\n" + 
//				"A.PEG_FREQUENCY_IN_MONTHS,\r\n" + 
//				"A.PEG_FREQUENCY_IN_DAYS\r\n" + 
//				"FROM ITC A,\r\n" + 
//				"(SELECT ENTITY_ID,\r\n" + 
//				"MAX(INT_TBL_CODE_SRL_NUM) INT_TBL_CODE_SRL_NUM\r\n" + 
//				"FROM ITC\r\n" + 
//				"WHERE ENTITY_TYPE = 'ACCNT'\r\n" + 
//				"AND DEL_FLG = 'N'\r\n" + 
//				"AND ENTITY_CRE_FLG = 'Y'\r\n" + 
//				"GROUP BY ENTITY_ID) B\r\n" + 
//				"WHERE A.ENTITY_ID = B.ENTITY_ID\r\n" + 
//				"AND A.INT_TBL_CODE_SRL_NUM = B.INT_TBL_CODE_SRL_NUM"
//				+ "");
		
		Dataset <Row> ITC_B = itc.select("ENTITY_ID", "INT_TBL_CODE_SRL_NUM", "ENTITY_TYPE", "DEL_FLG", "ENTITY_CRE_FLG")
				.filter("ENTITY_TYPE = 'ACCNT' and DEL_FLG = 'N' and ENTITY_CRE_FLG = 'Y'")
				.groupBy("ENTITY_ID")
				.agg(max("INT_TBL_CODE_SRL_NUM").alias("B_INT_TBL_CODE_SRL_NUM"));
		
//		ITC_B.show();
				
				
		Dataset<Row> ITC_A = itc.select("ENTITY_ID", "INT_TBL_CODE_SRL_NUM", "PEGGED_FLG", "INT_TBL_CODE", "NRML_PCNT_CR"
				, "REASON_CODE", "PEG_REVIEW_DATE", "END_DATE", "PEG_FREQUENCY_IN_MONTHS", "PEG_FREQUENCY_IN_DAYS")
				.withColumnRenamed("ENTITY_ID", "A_ENTITY_ID");
		
		
//		ITC_A.show();
		
		
		Dataset<Row> A_B = ITC_A.join(ITC_B, ITC_A.col("A_ENTITY_ID").equalTo(ITC_B.col("ENTITY_ID"))
				.and(ITC_A.col("INT_TBL_CODE_SRL_NUM").equalTo(ITC_B.col("B_INT_TBL_CODE_SRL_NUM"))));

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
                String ENTITY_ID = record.getAs("A_ENTITY_ID");
                String INT_TBL_CODE_SRL_NUM = record.getAs("B_INT_TBL_CODE_SRL_NUM");
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


		ITC_DF.createOrReplaceTempView("ITC");
		//ITC_DF.show();
		printMessage("ITC_DF----------------Ends");
		// 21. ITC_DF - Ends

		// 22. GSH_DF - Starts
		printMessage("GSH_DF----------------Starts");
//		Dataset<Row> GSH_DF = spark.sql("SELECT DISTINCT GL_CODE, GL_SUB_HEAD_CODE\r\n" + 
//				"FROM GSH\r\n" + 
//				"WHERE GSH.DEL_FLG = 'N'"
//				+ "");
		Dataset<Row> GSH_DFs = gsh.select("GL_CODE","GL_SUB_HEAD_CODE").distinct().filter("DEL_FLG == 'N'");
		
		StructField[] GSH_DFschemaFields = {
                new StructField("GL_CODE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("GL_SUB_HEAD_CODE", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType GSH_DFSchema = new StructType(GSH_DFschemaFields);
        ExpressionEncoder<Row> GSH_DFencoder = RowEncoder.apply(GSH_DFSchema);
		
		Dataset<Row> GSH_DF  = GSH_DFs.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String GL_CODE = record.getAs("GL_CODE");
                String GL_SUB_HEAD_CODE = record.getAs("GL_SUB_HEAD_CODE");
          
                  
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(GL_CODE, GL_SUB_HEAD_CODE);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, GSH_DFencoder);

		
		GSH_DF.createOrReplaceTempView("GSH");
		//GSH_DF.show();
		printMessage("GSH_DF----------------Ends");
		// 22. GSH_DF - Ends

		
		// 23. GSP_DF - Starts
		printMessage("GSP_DF----------------Starts");
//		Dataset<Row> GSP_DF = spark.sql("SELECT SCHM_CODE, ACCT_PREFIX, SCHM_DESC, PRODUCT_CONCEPT\r\n" + 
//				"FROM GSP\r\n" + 
//				"WHERE DEL_FLG = 'N'"
//				+ "");
		
		Dataset<Row> GSP_DF_filter = gsp.filter("DEL_FLG = 'N'");

		
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

		GSP_DF.createOrReplaceTempView("GSP");
		//GSP_DF.show();
		printMessage("GSP_DF----------------Ends");
		// 23. GSP_DF - Ends

		// 24. GAM_DF - Starts
		printMessage("GAM_DF----------------Starts");
	/*	Dataset<Row> GAM_DF = spark.sql("SELECT GAM.ACID,\r\n" + 
				"GAM.GL_SUB_HEAD_CODE,\r\n" + 
				"GAM.FORACID,\r\n" + 
				"GAM.ACCT_NAME,\r\n" + 
				"GAM.CIF_ID,\r\n" + 
				"GAM.ACCT_OPN_DATE,\r\n" + 
				"GAM.SCHM_CODE,\r\n" + 
				"GAM.ACCT_CRNCY_CODE,\r\n" + 
				"GAM.ACCT_CLS_DATE,\r\n" + 
				"GAM.LIMIT_B2KID,\r\n" + 
				"GAM.SOL_ID,\r\n" + 
				"GAM.SANCT_LIM,\r\n" + 
				"CASE\r\n" + 
				"WHEN (SELECT DC_CLS_DATE FROM GCT) >\r\n" + 
				"TO_DATE('03-05-2016', 'DD-MM-YYYY') THEN\r\n" + 
				"TO_DATE(date_format(GEM.ACCT_OPN_BOD_DATE, 'DD-MM-YYYY'),\r\n" + 
				"'DD-MM-YYYY')\r\n" + 
				"ELSE\r\n" + 
				"GAM.RCRE_TIME \r\n" + 
				"END GAM_RCRE_TIME,\r\n" + 
				"CASE\r\n" + 
				"WHEN GAM.ACCT_CLS_DATE IS NULL OR GAM.ACCT_CLS_DATE = '' THEN\r\n" + 
				"'Live'\r\n" + 
				"ELSE\r\n" + 
				"'Closed'\r\n" + 
				"END AS LIVE_CLOSED,\r\n" + 
				"GAM.SCHM_TYPE,\r\n" + 
				"GAM.ACCT_MGR_USER_ID,\r\n" + 
				"GAM.MODE_OF_OPER_CODE,\r\n" + 
				"GAM.ACCT_OWNERSHIP,\r\n" + 
				"NVL(GAM.CHQ_ALWD_FLG, 'N') CHQ_ALWD_FLG,\r\n" + 
				"GAM.FREZ_CODE,\r\n" + 
				"GAM.CHRG_LEVEL_CODE,\r\n" + 
				"NVL((CASE\r\n" + 
				"WHEN GAM.FREZ_CODE = 'D' THEN\r\n" + 
				"'DEBIT FREEZE'\r\n" + 
				"WHEN GAM.FREZ_CODE = 'C' THEN\r\n" + 
				"'CREDIT FREEZE'\r\n" + 
				"WHEN GAM.FREZ_CODE = 'T' THEN\r\n" + 
				"'TOTAL FREEZE'\r\n" + 
				"ELSE\r\n" + 
				"'NO FREEZE'\r\n" + 
				"END),\r\n" + 
				"'NA') FREEZE_DESC,\r\n" + 
				"FREZ_REASON_CODE,\r\n" + 
				"CASE\r\n" + 
				"WHEN GAM.FREZ_CODE IS NULL THEN\r\n" + 
				"NULL\r\n" + 
				"ELSE\r\n" + 
				"GAM.FREZ_REASON_CODE\r\n" + 
				"END FREEZE_REASON_CODE,\r\n" + 
				"GAM.RCRE_USER_ID,\r\n" + 
				"GAM.DRWNG_POWER,\r\n" + 
				"GAM.DRWNG_POWER_IND,\r\n" + 
				"GAM.LCHG_USER_ID,\r\n" + 
				"GAM.SOURCE_DEAL_CODE,\r\n" + 
				"GAM.DISBURSE_DEAL_CODE,\r\n" + 
				"GAM.LAST_TRAN_DATE,\r\n" + 
				"CASE\r\n" + 
				"WHEN GAM.FREZ_CODE IS NULL THEN\r\n" + 
				"NULL\r\n" + 
				"ELSE\r\n" + 
				"GAM.LAST_FREZ_DATE\r\n" + 
				"END LAST_FREZ_DATE,\r\n" + 
				"p.repricing_plan\r\n" + 
				"FROM C_GAM GAM\r\n" + 
//				"INNER JOIN (SELECT ACID\r\n" + 
//				"FROM C_GAM GAM\r\n" + 
//				"WHERE trunc(GAM.LCHG_TIME, 'DD') >=trunc(date_add(current_date(), -2), 'DD')\r\n" + 
//				"AND GAM.ACCT_OWNERSHIP <> 'O'\r\n" + 
//				"AND GAM.DEL_FLG = 'N'\r\n" + 
//				"AND GAM.ENTITY_CRE_FLG = 'Y'\r\n" + 
//				"UNION\r\n" + 
//				"SELECT ACID\r\n" + 
//				"FROM ADT\r\n" + 
//				"WHERE AUDIT_bod_DATE >= trunc(date_add(current_date(), -2), 'DD')\r\n" + 
//				"AND ACID!='!') Q ON GAM.ACID = Q.ACID\r\n" + 
				"LEFT JOIN GEM ON GAM.ACID = GEM.ACID\r\n" + 
				"LEFT JOIN lrp p ON gam.acid=p.acid AND p.entity_cre_flg='Y' AND p.del_flg='N'\r\n" + 
				"WHERE GAM.ACCT_OWNERSHIP <> 'O'\r\n" + 
				"AND GAM.DEL_FLG = 'N'\r\n" + 
				"AND GAM.ENTITY_CRE_FLG = 'Y'"
				+ "");
	*/	
		//Fetch GAM DF

		Dataset<Row> GAM_DF = funcFetchGAMDF(spark);

		printMessage(" No of rows = " + GAM_DF.count() + "  & no of colums in GAM_DF  = " + GAM_DF.columns().length);

		GAM_DF.createOrReplaceTempView("GAM");
		//GAM_DF.show();
		printMessage("GAM_DF----------------Ends");		
		
		// 24. GAM_DF - Ends
	
 //  ETL JOins Begin		
				Dataset<Row> df1 = GAM_DF.join(S_DF, GAM_DF.col("CIF_ID").equalTo(S_DF.col("CUSTID")), "left");  //1
//				printMessage("print joined s");
//				s.show();
				Dataset<Row> df2 = df1.join(PR_DF, df1.col("ACID").equalTo(PR_DF.col("ENTITY_ID")), "left"); //2
				Dataset<Row> df3 = df2.join(V_DF, df2.col("ACID").equalTo(V_DF.col("ACID")), "left").drop(V_DF.col("ACID")); //3 gam
				//df3.show();
				//System.out.println(df3.columns().length);
	//GAC START
				Dataset<Row> df4_1 = GAC_DF.join(RCT_AI_DF,GAC_DF.col("FREE_CODE_8").equalTo(RCT_AI_DF.col("RCT_AI_REF_CODE")), "left");
				//df4_1.show();
				//System.out.println(df4_1.columns().length);
				Dataset<Row> df4_2 = df4_1.join(RCT_CN_DF, df4_1.col("FREE_TEXT_10").equalTo(RCT_CN_DF.col("RCT_CN_REF_CODE")), "left");
				//df4_2.show();
				Dataset<Row> df4_3 = df4_2.join(RCT_OC_DF,  df4_2.col("ACCT_OCCP_CODE").equalTo(RCT_OC_DF.col("RCT_OC_REF_CODE")), "left");
				//df4_3.show();
				Dataset<Row> df4_4 = df4_3.join(RCT_AF_DF,  df4_3.col("FREE_CODE_5").equalTo(RCT_AF_DF.col("RCT_AF_REF_CODE")), "left");
				Dataset<Row> df4_5 = RCT_38_DF.join(df4_4, RCT_38_DF.col("RCT_38_REF_CODE").equalTo(df4_4.col("NATURE_OF_ADVN")),"left");//.withColumnRenamed(RCT_38_DF.col("REF_CODE"), "RCT_38_REF_CODE");
				
	//GAC END
				Dataset<Row> df5 = df3.join(df4_5, df3.col("ACID").equalTo(df4_5.col("ACID")), "left").drop(df4_5.col("ACID")); 
			//	System.out.println(df5.columns().length);
				Dataset<Row> df6 = df5.join(C_DBC_DF, df5.col("FORACID").equalTo(C_DBC_DF.col("FORACID")),"left").drop(C_DBC_DF.col("FORACID"));
				//df6.show();
			//	System.out.println(df6.columns().length);
				Dataset<Row>  df7 = df6.join(ACD_DF,df6.col("ACID").equalTo(ACD_DF.col("B2K_ID")), "left");
			//	df7.show();
			//	System.out.println(df7.columns().length);
		//LAM START		
				Dataset<Row> df8_1 = LAM_DF.join(RCT_EY_DF, LAM_DF.col("PAYOFF_REASON_CODE").equalTo(RCT_EY_DF.col("RCT_EY_REF_CODE")),"left");
		//LAM END		
				Dataset<Row> df9 = df7.join(df8_1, df7.col("ACID").equalTo(df8_1.col("ACID")),"left").drop(df8_1.col("ACID")) ;
			//	df9.show();
			//	System.out.println(df9.columns().length);
				
				Dataset<Row> df10 = df9.join(LHT_DF, df9.col("ACID").equalTo(LHT_DF.col("ACID")),"left").drop(LHT_DF.col("ACID"));
			//	df10.show();
			//	System.out.println(df10.columns().length);
				
				
				Dataset<Row> df11 = df10.join(LA_SAM_DF, df10.col("ACID").equalTo(LA_SAM_DF.col("ACID")),"left").drop(LA_SAM_DF.col("ACID"));
			//	System.out.println(df11.columns().length);
			
				Dataset<Row> df12 = df11.join(CFCM_DF, df11.col("FORACID").equalTo(CFCM_DF.col("ENTITY_ID")),"left"); 	
				
				Dataset<Row> df13 = df12.join(RCT_31_DF, df12.col("FREZ_REASON_CODE").equalTo(RCT_31_DF.col("RCT_31_REF_CODE")),"left");

				Dataset<Row> df14 = df13.join(RCT_27_DF, df13.col("MODE_OF_OPER_CODE").equalTo(RCT_27_DF.col("RCT_27_REF_CODE")),"left"); 

				Dataset<Row> df15 = df14.join(TAM_DF, df14.col("ACID").equalTo(TAM_DF.col("ACID")),"left").drop(TAM_DF.col("ACID")); 
				
				Dataset<Row> df16 = df15.join(SOL_DF, df15.col("SOL_ID").equalTo(SOL_DF.col("SOL_ID")),"left").drop(SOL_DF.col("SOL_ID"));
	//CAM			
				Dataset<Row> df17_1 = CAM_DF.join(RCT_CAM_DF, CAM_DF.col("ACCT_CLS_REASON_CODE").equalTo(RCT_CAM_DF.col("REF_CODE")),"left"); 
				
				
				Dataset<Row> df18 = df16.join(df17_1, df16.col("ACID").equalTo(df17_1.col("ACID")),"left").drop(df17_1.col("ACID")); 
	//ITC			
				Dataset<Row> df19 = df18.join(ITC_DF, df18.col("ACID").equalTo(ITC_DF.col("ENTITY_ID")),"left");
				Dataset<Row> df20 = df19.join(GSH_DF, df19.col("GL_SUB_HEAD_CODE").equalTo(GSH_DF.col("GL_SUB_HEAD_CODE")),"inner"); 
				Dataset<Row> GAM_Final = df20.join(GSP_DF, df20.col("SCHM_CODE").equalTo(GSP_DF.col("SCHM_CODE")),"inner"); 

				//GAM_Final.show();
				System.out.println("outSchema");
				GAM_Final.printSchema();
				System.out.println("outSchema");
				
			//	Dataset<Row> test = GAM_Final.sort("ACID");
			//	test.show();
				
				System.out.println("total columns  = " + GAM_Final.columns().length + " & total rows = "  + GAM_Final.count());
		
		// 1. ETL_Query for Joins Ends
				
		//Move data from GAM to ADF_CLIENT.ACCOUNT_MASTER based on conditions. 		
				
			 	StructField[] outputFields = {
			 			new StructField("ACID", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("GL_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("GL_SUB_HEAD_CODE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("ACCOUNT_NO", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("ACCT_NAME", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("ACCOUNT_HOLDER_NAME", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CUSTOMER_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("OPENING_DATE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("PRODUCT_CODE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("CURRENCY_CODE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("CLOSING_DATE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("LINE_ID", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("BRANCH_CODE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("DORMANT_DATE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("ACCOUNT_TYPE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("ACCOUNT_PREFIX", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("PSL_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PSL_DESC", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("RELIGION_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RELIGION", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("MATURITY_AMOUNT", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("AMOUNT", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("MATURITY_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("INTEREST_RATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PP20_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PP20_DESC", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("ADVANCE_TYPE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ADVANCE_PURPOSE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("ROI_TYPE", DataTypes.StringType, true, Metadata.empty()), 
			 			new StructField("BORROWER_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("OCCUPATION_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("OCCUPATION_DESC", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("BSR3_COMMODITY_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("SUBSIDY", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("INITIAL_SANCTION_LIMIT", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("INITIAL_SANCTION_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("INVESTMENT_FLAG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LAST_CREDIT_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LAST_DEBIT_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("INITIAL_FUNDING", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("OPENING_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ACCT_BUSINESS_SEGMENT", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("BOOKING_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LIVE_CLOSED", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DEPOSIT_STATUS", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("SCHEME_DESCRIPTION", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RM", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("MODE_OF_OPERATION", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("MODE_OF_OPERATION_DESC", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("BAR_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ASSET_CLASSIFICATION", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PROMO_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DEBIT_CARD", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("NPA_FLG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("NPA_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("BRANCH_NAME", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LC_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LG_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("BUSINESS_SEG_DESC", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ACCOUNT_OWNERSHIP", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CHEQ_ALLOWED", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("FREEZE_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("FREEZE_DESC", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("FREEZE_REASON_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("FREEZE_REASON_DESC", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RISK_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ENTERER_ID", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("INACTIVE_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("IB_LAST_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ATM_LAST_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LOAN_PAYOFF_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("REASON_FOR_CLOSURE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("EMIAMT", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("AQB_WAIVER", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RATE_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("EMI_START_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LINK_OPER_ACCOUNT", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ACCOUNT_STATUS", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("GOV_SCHEME_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ROI_CARD_RATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PAYOFF_FLG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LR_FREQ_TYPE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("APP_REF_NO", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PREFERED_ACCOUNT", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CHRGE_OFF_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("REASON_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CC_RENEWAL_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PTC_FLAG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("GROUP_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("URN_NO", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RENEWAL_FLAG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CHARGE_OFF_FLG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DEPOSIT_PERIOD_MTHS", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DEPOSIT_PERIOD_DAYS", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DSB_FLAG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PROMO_CODE_N", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RISK_CATEGORY_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RISK_DESC", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CUSTOMERNREFLG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LIMIT_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LIMIT_EXPIRY_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DRWNG_POWER", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DRWNG_POWER_IND", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LCHG_USER_ID", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("RCRE_USER_ID", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("SCHM_TYPE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("INDUSTRY_TYPE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LINKED_SB_ACID", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("SOURCE_DEAL_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("DISBURSE_DEAL_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CLOSURE_FLAG", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LAST_FREZ_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ACCT_STATUS_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("LAST_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PEG_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("APPLICABLE_DATE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("ACCT_LVL_GROUPING", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("AADHAR_SEEDING_YN", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PROJECT_FINANCE_YN", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("CHRG_LEVEL_CODE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("SIGNATURE_STATUS", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("EMI_TYPE", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("PAYMENT_METHOD", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("repricing_plan", DataTypes.StringType, true, Metadata.empty()),
			 			new StructField("REPAYMENT_ACID", DataTypes.StringType, true, Metadata.empty())};
			 
			 	StructType outputSchema = new StructType(outputFields);
			 	
			 	
			 	ExpressionEncoder<Row> encoder = RowEncoder.apply(outputSchema);
			 
			 	Dataset<Row> OutDF = GAM_Final.flatMap(new FlatMapFunction<Row, Row>() {
			 		@Override
			 		public Iterator<Row> call(Row record) throws Exception {
			 
			 			ArrayList<Row> newRows = new ArrayList<>();
			 			Row recordOut;
			 		//	 Local logic start
			 			
			 			
						String ACID = record.isNullAt(0)? null:record.getAs("ACID");;
			 			String GL_CODE = record.isNullAt(224)? null:record.getString(224);;
			 			String GL_SUB_HEAD_CODE = record.isNullAt(1)? null:record.getString(1);
			 			String ACCOUNT_NO = record.isNullAt(2)? null:record.getString(2);;
			 			String ACCT_NAME = record.isNullAt(3)? null:record.getString(3);;
			 			String ACCOUNT_HOLDER_NAME = record.isNullAt(3)? null:record.getString(3);;
			 			String CUSTOMER_CODE = record.isNullAt(4)? null:record.getString(4);;
			 			String OPENING_DATE = null;//record.isNullAt(7)? null:record.getString(7);; // to do
			 			String PRODUCT_CODE = record.isNullAt(6)? null:record.getString(6);;
			 			String CURRENCY_CODE = record.isNullAt(7)? null:record.getString(7);;
			 			String CLOSING_DATE = record.isNullAt(8)? null:record.getString(8);;
			 			String LINE_ID = record.isNullAt(9)? null:record.getString(9);;
			 			String BRANCH_CODE = record.isNullAt(10)? null:record.getString(10);;
			 			String DORMANT_DATE = null;
			 			String ACCOUNT_TYPE = null; // to do
			 			String ACCOUNT_PREFIX =  record.isNullAt(227)? null:record.getString(227);
			 			String PSL_CODE =  record.getAs("RCT_38_REF_CODE")==null? null:record.getAs("RCT_38_REF_CODE");
			 			String PSL_DESC =  record.getAs("RCT_38_REF_DESC")==null? null:record.getAs("RCT_38_REF_DESC");
			 			String RELIGION_CODE = null;// record.isNullAt(18)? null:record.getString(18);
			 			String RELIGION =  null;//record.isNullAt(19)? null:record.getString(19);
			 			String MATURITY_AMOUNT =  record.isNullAt(204)? null:record.getString(204);
			 			String AMOUNT =  record.isNullAt(205)? null:record.getString(205);
			 			String MATURITY_DATE =  record.isNullAt(203)? null:record.getString(203);
			 			String INTEREST_RATE =  "0";//record.isNullAt(23)? null:record.getString(23);
			 			String PP20_CODE =  record.getAs("RCT_AF_REF_CODE")==null? null:record.getAs("RCT_AF_REF_CODE");
			 			String PP20_DESC =  record.getAs("RCT_AF_REF_DESC")==null? null:record.getAs("RCT_AF_REF_DESC");
			 			String ADVANCE_TYPE =  record.isNullAt(44)? null:record.getString(44);
			 			String ADVANCE_PURPOSE =  record.isNullAt(46)? null:record.getString(46);
			 			String ROI_TYPE = null;// record.isNullAt(28)? null:record.getString(28);
			 			String BORROWER_CODE =  record.isNullAt(57)? null:record.getString(57);
			 			String OCCUPATION_CODE =   record.getAs("RCT_OC_REF_CODE")==null? null:record.getAs("RCT_OC_REF_CODE");
			 			String OCCUPATION_DESC =  record.getAs("RCT_OC_REF_DESC")==null? null:record.getAs("RCT_OC_REF_DESC");
			 			String BSR3_COMMODITY_CODE =  record.isNullAt(51)? null:record.getString(51);
			 			String SUBSIDY =  record.isNullAt(167)? null:record.getString(167);
			 			String INITIAL_SANCTION_LIMIT =  record.isNullAt(11)? null:record.getString(11);
			 			String INITIAL_SANCTION_DATE =  record.isNullAt(160)? null:record.getString(160);
			 			String INVESTMENT_FLAG =  record.isNullAt(85)? null:record.getString(85);
			 			
			 			String LAST_CREDIT_TRAN_DATE = null;// record.isNullAt(37)? null:record.getString(37);
			 			String LAST_DEBIT_TRAN_DATE = null;// record.isNullAt(38)? null:record.getString(38);
			 			String INITIAL_FUNDING =  null;//record.isNullAt(39)? null:record.getString(39);
			 			String OPENING_TRAN_DATE =  record.getAs("GAM_RCRE_TIME")==null? null:record.getAs("GAM_RCRE_TIME");//12
			 			String ACCT_BUSINESS_SEGMENT =  record.isNullAt(86)? null:record.getString(86);
			 			String BOOKING_DATE = null;// record.isNullAt(42)? null:record.getString(42);
			 			String LIVE_CLOSED =  record.isNullAt(13)? null:record.getString(13);
			 			String DEPOSIT_STATUS =  record.isNullAt(206)? null:record.getString(206);
			 			String SCHEME_DESCRIPTION =  record.isNullAt(228)? null:record.getString(228);
			 			String RM =  record.isNullAt(15)? null:record.getString(15);
			 			String MODE_OF_OPERATION =  record.isNullAt(16)? null:record.getString(16);
			 			String MODE_OF_OPERATION_DESC =  record.getAs("RCT_27_REF_DESC")==null? null:record.getAs("RCT_27_REF_DESC");
			 			String BAR_CODE =  record.isNullAt(79)? null:record.getString(79);
			 			String ASSET_CLASSIFICATION =  record.isNullAt(144)? null:record.getString(144);
			 			String PROMO_CODE =  record.isNullAt(63)? null:record.getString(63);
			 			String DEBIT_CARD =  record.isNullAt(142)? null:record.getString(142);
			 			String NPA_FLG =  record.isNullAt(68)? null:record.getString(68);
			 			String NPA_DATE =  null;//record.isNullAt(54)? null:record.getString(54);
			 			String BRANCH_NAME =  record.isNullAt(208)? null:record.getString(208);
			 			String LC_CODE =   record.getAs("CFCM_FREE_CODE_5")==null? null:record.getAs("CFCM_FREE_CODE_5");
			 			String LG_CODE =   record.getAs("CFCM_FREE_CODE_6")==null? null:record.getAs("CFCM_FREE_CODE_6");
			 			String BUSINESS_SEG_DESC =  record.getAs("RCT_CN_REF_DESC")==null? null:record.getAs("RCT_CN_REF_DESC");
			 			String ACCOUNT_OWNERSHIP =  record.isNullAt(17)? null:record.getString(17);
			 			String CHEQ_ALLOWED = null;// record.isNullAt(60)? null:record.getString(60);
			 			String FREEZE_CODE =  record.isNullAt(19)? null:record.getString(19);
			 			String FREEZE_DESC =  record.isNullAt(21)? null:record.getString(21);
			 			String FREEZE_REASON_CODE =  record.isNullAt(23)? null:record.getString(23);
			 			String FREEZE_REASON_DESC = null;//
			 			String RISK_CODE =  record.isNullAt(61)? null:record.getString(61);
			 			String ENTERER_ID =  record.isNullAt(24)? null:record.getString(24);//GAM.RCRE_USER_ID
			 			String INACTIVE_DATE =  null;//record.isNullAt(67)? null:record.getString(67);
			 			String IB_LAST_TRAN_DATE = null;// record.isNullAt(68)? null:record.getString(68);
			 			String ATM_LAST_TRAN_DATE = null;// record.isNullAt(69)? null:record.getString(69);
			 			String LOAN_PAYOFF_DATE = null;// record.isNullAt(70)? null:record.getString(70);
			 			String REASON_FOR_CLOSURE = null;// record.isNullAt(71)? null:record.getString(71);
			 			String EMIAMT = null;// record.isNullAt(72)? null:record.getString(72);
			 			String AQB_WAIVER =  record.isNullAt(63)? null:record.getString(63);
			 			String RATE_CODE =  record.isNullAt(217)? null:record.getString(217);
			 			String EMI_START_DATE =  record.isNullAt(150)? null:record.getString(150);
			 			String LINK_OPER_ACCOUNT =  record.isNullAt(201)? null:record.getString(201);
			 			String ACCOUNT_STATUS = null;//  record.isNullAt(77)? null:record.getString(77);
			 			String GOV_SCHEME_CODE = record.getAs("FREE_CODE_5")==null? null:record.getAs("FREE_CODE_5");
			 			String ROI_CARD_RATE =  record.isNullAt(218)? null:record.getString(218);
			 			String PAYOFF_FLG =  record.isNullAt(148)? null:record.getString(148);
			 			String LR_FREQ_TYPE = null;// record.isNullAt(81)? null:record.getString(81);
			 			String APP_REF_NO =  record.isNullAt(154)? null:record.getString(154);
			 			String PREFERED_ACCOUNT =  record.isNullAt(187)? null:record.getString(187);
			 			String CHRGE_OFF_DATE =  record.isNullAt(37)? null:record.getString(37);
			 			String REASON_CODE =  record.isNullAt(219)? null:record.getString(219);
			 			String CC_RENEWAL_DATE =  record.isNullAt(161)? null:record.getString(161);
			 			String PTC_FLAG =  null;// record.isNullAt(87)? null:record.getString(87);
			 			String GROUP_CODE =  null;// record.isNullAt(88)? null:record.getString(88);
			 			String URN_NO =  null;// record.isNullAt(89)? null:record.getString(89);
			 			String RENEWAL_FLAG =  record.isNullAt(197)? null:record.getString(197);
			 			String CHARGE_OFF_FLG =  null;// record.isNullAt(91)? null:record.getString(91);
			 			String DEPOSIT_PERIOD_MTHS =  record.isNullAt(198)? null:record.getString(198);
			 			String DEPOSIT_PERIOD_DAYS =  record.isNullAt(199)? null:record.getString(199);
			 			String DSB_FLAG =  record.isNullAt(185)? null:record.getString(185);
			 			String PROMO_CODE_N =  record.isNullAt(186)? null:record.getString(186);
			 			String RISK_CATEGORY_CODE =  record.isNullAt(61)? null:record.getString(61);
			 			String RISK_DESC =    record.getAs("RCT_AI_REF_DESC")==null? null:record.getAs("RCT_AI_REF_DESC");
			 			String CUSTOMERNREFLG = null;// record.isNullAt(161)? null:record.getString(161);
			 			String LIMIT_REVIEW_DATE =  record.isNullAt(161)? null:record.getString(161);
			 			String LIMIT_EXPIRY_DATE =  record.isNullAt(162)? null:record.getString(162);
			 			String DRWNG_POWER =  record.isNullAt(25)? null:record.getString(25);
			 			String DRWNG_POWER_IND =  record.isNullAt(26)? null:record.getString(26);
			 			String LCHG_USER_ID =  record.isNullAt(27)? null:record.getString(27);
			 			String RCRE_USER_ID =  record.isNullAt(24)? null:record.getString(24);
			 			String SCHM_TYPE =  record.isNullAt(14)? null:record.getString(14);
			 			String INDUSTRY_TYPE =  record.isNullAt(55)? null:record.getString(55);
			 			String LINKED_SB_ACID =  record.isNullAt(147)? null:record.getString(147);
			 			String SOURCE_DEAL_CODE =  record.isNullAt(28)? null:record.getString(28);
			 			String DISBURSE_DEAL_CODE =  record.isNullAt(29)? null:record.getString(29);
			 			String CLOSURE_FLAG = null;// record.isNullAt(110)? null:record.getString(110);
			 			String LAST_FREZ_DATE =  record.isNullAt(31)? null:record.getString(31);
			 			String ACCT_STATUS_DATE =  record.isNullAt(210)? null:record.getString(210);
			 			String LAST_TRAN_DATE =  record.isNullAt(30)? null:record.getString(30);
			 			String PEG_REVIEW_DATE =  record.getAs("NEXT_PEG_REVIEW_DATE")==null? null:record.getAs("NEXT_PEG_REVIEW_DATE");//36
			 			String APPLICABLE_DATE =  record.isNullAt(163)? null:record.getString(163);
			 			String ACCT_LVL_GROUPING = null;// record.isNullAt(116)? null:record.getString(116);
			 			String AADHAR_SEEDING_YN =  record.isNullAt(191)? null:record.getString(191);
			 			String PROJECT_FINANCE_YN =  record.isNullAt(121)? null:record.getString(121);
			 			String CHRG_LEVEL_CODE =  record.isNullAt(20)? null:record.getString(20);
			 			String SIGNATURE_STATUS =  record.isNullAt(34)? null:record.getString(34);
			 			String EMI_TYPE =  record.isNullAt(155)? null:record.getString(155);
			 			String PAYMENT_METHOD =  record.isNullAt(156)? null:record.getString(156);
			 			String repricing_plan =  record.isNullAt(32)? null : record.getAs("repricing_plan");
			 			String REPAYMENT_ACID =  record.isNullAt(207)? null:record.getString(207);
			 
			 			
			 			String ACCT_OPN_DATE = record.isNullAt(5)? null:record.getAs("ACCT_OPN_DATE");
			 			if (ACCT_OPN_DATE.equals(null)) {
			 				OPENING_DATE = record.isNullAt(202)?null:record.getAs("OPEN_EFFECTIVE_DATE");	
			 			}else
			 			{OPENING_DATE = ACCT_OPN_DATE;}
			 			
//			 			DORMANT_DATE
			 			DORMANT_DATE = null;
			 			String ACCT_STATUS = record.isNullAt(209)? null:record.getString(209);//getAs("ACCT_STATUS");//200
			 			if (ACCT_STATUS !=null) {
			 			if ( ACCT_STATUS.contentEquals("D")) {
			 				DORMANT_DATE = record.isNullAt(210)? null:record.getAs("ACCT_STATUS_DATE");//210
			 			}}

			 			
//			 			ACCOUNT_TYPE
			 			
			 			String ACCT_PREFIX = record.isNullAt(227)?null:record.getAs("ACCT_PREFIX").toString();//227
			 			String PRODUCT_CONCEPT = record.isNullAt(229)? null: record.getString(229);//getAs("PRODUCT_CONCEPT");//229
			 			
			 			if (ACCT_PREFIX.contentEquals("70") && (PRODUCT_CONCEPT != null)) { // || !PRODUCT_CONCEPT.isEmpty()
							ACCOUNT_TYPE = 	PRODUCT_CONCEPT;
						}else
						{  System.out.println("ajtest PRODUCT_CONCEPT = " +PRODUCT_CONCEPT);
						 if (PRODUCT_CONCEPT !=null) {
							if ((PRODUCT_CONCEPT.contentEquals("CC")) || (PRODUCT_CONCEPT.contentEquals("OD"))) {
								ACCOUNT_TYPE = record.isNullAt(229)? null: record.getString(229);//	PRODUCT_CONCEPT;
  						}}else
							{
								switch (ACCT_PREFIX) {
								case "30": ACCOUNT_TYPE = "SB";
									break;
								case "40":
									ACCOUNT_TYPE = "CA";
									break;
								case "60":
									ACCOUNT_TYPE = "CC";
									break;
								case "70":
									ACCOUNT_TYPE = "TD";
									break;
								case "75":
									ACCOUNT_TYPE = "TD";
									break;
								case "80":
									ACCOUNT_TYPE = "RL";
									break;
								case "90":
									ACCOUNT_TYPE = "CL";
									break;
								}
							}	
						}
		//ROI_TYPE
			 			String PEGGED_FLG = record.isNullAt(216)? "N" : record.getAs("PEGGED_FLG");
			 			
			 			String NEXT_PEG_REVIEW_DATE = record.isNullAt(36)?null:record.getAs("NEXT_PEG_REVIEW_DATE");//36
			 			String NEXT_PEG_REVIEW_DATE_temp=MATURITY_DATE;
			 			if (MATURITY_DATE==null)
			 			{
			 				NEXT_PEG_REVIEW_DATE_temp = record.isNullAt(152)?null:record.getAs("EI_PERD_END_DATE");//152
			 			}//else
			 			if(NEXT_PEG_REVIEW_DATE_temp==null){
			 				NEXT_PEG_REVIEW_DATE_temp = record.isNullAt(221)?null:record.getAs("END_DATE");//221
			 			}
			 			
			 			String repricing_plan_temp = repricing_plan==null?" ":repricing_plan;
			 			
			 			if (PEGGED_FLG.contentEquals("N") && repricing_plan_temp.contentEquals("C")) {
			 			
			 				ROI_TYPE = "FIXED";	
			 			}else 
			 			{
			 			//	if (NEXT_PEG_REVIEW_DATE != null))
			 				if (PEGGED_FLG.contentEquals("Y") 
			 						&& (NEXT_PEG_REVIEW_DATE != null)) 
//			 						&& (NEXT_PEG_REVIEW_DATE.compareTo(NEXT_PEG_REVIEW_DATE_temp))<0) 
			 						{
			 					ROI_TYPE = "FLOAT";	
			 				}else
			 				{
			 					if (PEGGED_FLG.contentEquals("Y") 
				 						&& (NEXT_PEG_REVIEW_DATE != null) )
//								 	&& (NEXT_PEG_REVIEW_DATE.compareTo(NEXT_PEG_REVIEW_DATE_temp))>0 || NEXT_PEG_REVIEW_DATE.compareTo(NEXT_PEG_REVIEW_DATE_temp)== 0 )
			 					{
				 					ROI_TYPE = "FIXED";	
				 				}else {
				 					if (PEGGED_FLG.contentEquals("N") && !repricing_plan_temp.contentEquals("C")) {
				 						ROI_TYPE = "FLOAT";		
				 					}else {
				 						ROI_TYPE = "FLOAT";	
				 					}
				 				}
			 				}
			 			}
			 			
			 //BOOKING_DATE
			 			BOOKING_DATE = record.getAs("OPEN_EFFECTIVE_DATE")==null? ACCT_OPN_DATE :record.getAs("OPEN_EFFECTIVE_DATE");
			 			
 			//DEPOSIT_STATUS
			 			// SCHM_TYPE = record.getAs("SCHM_TYPE");
			 			String DEPOSIT_STATUS_src = record.getAs("DEPOSIT_STATUS") ;
			 			if (SCHM_TYPE.contentEquals("TDA") )
			 			{
			 				if ( record.getAs("ACCT_CLS_DATE") == null)	// && MATURITY_DATE<(SELECT DC_CLS_DATE FROM TBAADM.GCT)
			 				{
			 					DEPOSIT_STATUS = "OVERDUE";
			 					
			 				}else {
			 			switch (DEPOSIT_STATUS_src) {
						case "O": DEPOSIT_STATUS = "OVERDUE";
							break;
						case "P":
							DEPOSIT_STATUS = "PREMATURE WITHDRAWAL";
							break;
						case "A":
							DEPOSIT_STATUS = "AUTOMATIC RENEWAL";
							break;
						case "B":
							DEPOSIT_STATUS = "RENEWAL OF BALANCE";
							break;
						case "R":
							DEPOSIT_STATUS = "RENEWAL";
							break;
						case "E":
							DEPOSIT_STATUS = "EXTENDED";
							break;
						case "T":
							DEPOSIT_STATUS = "TRANSFERRED OUT";
							break;
						case "":
							DEPOSIT_STATUS = "REGULAR";
							break;
						case " ":
							DEPOSIT_STATUS = "REGULAR";
							break;
						case "U":
							DEPOSIT_STATUS = "UNCLAIMED";
							break;
						case "N":
							DEPOSIT_STATUS = "NORMAL CLOSURE";
							break;
						default: DEPOSIT_STATUS = "NA";
						}
			 				}
			 			}else {
			 				DEPOSIT_STATUS = null;
			 			}
			 			
			 			
//			 			NPA_DATE
			 					 			
			 		  String PD_FLG = (record.getAs("PD_FLG"))==null? "N" : record.getAs("PD_FLG");//68
			 		  if (PD_FLG.contentEquals("Y")) {
			 			 NPA_DATE = record.getAs("PD_XFER_DATE"); //69
			 		  }else 
			 		  {
			 			 NPA_DATE = null;
			 		  }
			 		  
			 		  
//	            CHEQ_ALLOWED
			 		  
			 		 CHEQ_ALLOWED = (record.getAs("CHQ_ALWD_FLG"))==null ? "N": record.getAs("CHQ_ALWD_FLG");
			 	//	String REF_DESC =   record.getAs("RCT_OC_REF_DESC")==null? null:record.getAs("RCT_OC_REF_DESC"); //record.getAs("REF_DESC") 5 instances
//					FREEZE_REASON_DESC
			 		 
			 		 if (record.getAs("FREZ_CODE")==null) {
			 			FREEZE_REASON_DESC = null;
			 		 }else {
			 			FREEZE_REASON_DESC = record.getAs("RCT_31_REF_DESC")==null? null:record.getAs("RCT_31_REF_DESC");
			 		 }
			 		 
//			 		INACTIVE_DATE
			 			
			 			if (ACCT_STATUS !=null)	{
			 		 if (ACCT_STATUS.contentEquals("I") ) {
			 			INACTIVE_DATE = ACCT_STATUS_DATE;//210
			 		 }}else
			 		 {INACTIVE_DATE = null;
			 		 }
			 		 
//			 		LOAN_PAYOFF_DATE
			 		 
			 		 if (SCHM_TYPE.contentEquals("LAA")||SCHM_TYPE.contentEquals("CLA"))
			 		 {
			 			LOAN_PAYOFF_DATE =  record.getAs("PAYOFF_DATE");//151
			 		 }else {
			 			 LOAN_PAYOFF_DATE = null;
			 		 }
			 		 
//					REASON_FOR_CLOSURE
			 		String RCT_EY_REF_DESC =  record.getAs("RCT_EY_REF_DESC")==null? null:record.getAs("RCT_EY_REF_DESC"); //record.getAs("REF_DESC") 5 instances
			 		 if (RCT_EY_REF_DESC==null)
			 		 {
			 			REASON_FOR_CLOSURE	=	record.getAs("ACCT_CLS_REASON_DESC");
			 		 }else
			 		 {
			 			REASON_FOR_CLOSURE = RCT_EY_REF_DESC;
			 		 }
			 			
//			 		ACCOUNT_STATUS
			 		
			 		 if (!record.isNullAt(200))//ACCT_STATUS
			 		 {
			 			ACCOUNT_STATUS =  record.getString(200);
			 		 }else {
			 			 if (!record.isNullAt(209)) {
			 				ACCOUNT_STATUS =  record.getString(209);
			 			 }else {
			 				ACCOUNT_STATUS =  record.isNullAt(149)?null:record.getString(149); //ACCT_STATUS_FLG
			 			 }
			 		 }
			 		 
//			 		PTC_FLAG
			 		 
			 		 if (ACCT_PREFIX.contentEquals("80")||ACCT_PREFIX.contentEquals("90")) {
			 			PTC_FLAG = record.getAs("FREE_TEXT_14");
			 		 }else {
			 			 PTC_FLAG = "Non-PTC";
			 		 }
			 			 
//			 		GROUP_CODE& URN_NO
			 		 
			 		 if (ACCT_PREFIX.contentEquals("30"))
			 		 {
			 			GROUP_CODE = record.getAs("FREE_TEXT_14");
			 			URN_NO = record.getAs("FREE_TEXT_13");
			 		 }else {
			 			GROUP_CODE = null;
		 		    	URN_NO = null;
			 		 }
			 		 
//					CHARGE_OFF_FLG
			 		 
			 		CHARGE_OFF_FLG =  record.getAs("CHRGE_OFF_FLG")==null? null:record.getAs("CHRGE_OFF_FLG");

			 		 
//			 		DEPOSIT_PERIOD_MTHS
			 		DEPOSIT_PERIOD_MTHS = (record.getAs("DEPOSIT_PERIOD_MTHS"))==null? record.getAs("REP_PERD_MTHS"):record.getAs("DEPOSIT_PERIOD_MTHS");
			 		DEPOSIT_PERIOD_DAYS = (record.getAs("DEPOSIT_PERIOD_DAYS"))==null? record.getAs("REP_PERD_DAYS"):record.getAs("DEPOSIT_PERIOD_DAYS");
			 		
			 		
			 		String CLOSE_ON_MATURITY_FLG_temp =  (record.getAs("CLOSE_ON_MATURITY_FLG"))==null?null:(record.getAs("CLOSE_ON_MATURITY_FLG")).toString().replaceAll(" ", "");
			 		CLOSURE_FLAG = CLOSE_ON_MATURITY_FLG_temp==null ? "N" : CLOSE_ON_MATURITY_FLG_temp;
			 			 
		//			ACCT_LVL_GROUPING
			 		ACCT_LVL_GROUPING =null;
			 		String FREE_CODE_10 =(record.getAs("FREE_CODE_10"))==null?null: record.getAs("FREE_CODE_10").toString();
			 		if (FREE_CODE_10 !=null)
			 		{if (FREE_CODE_10.substring(0, 2).contentEquals("FM")) {
			 			ACCT_LVL_GROUPING = FREE_CODE_10;
			 		}}
			 		
			 		
			 		
			 		
			// 			System.out.println("testval acid = " + ACID);
			 
			 								
			 							
			 
			 			
			 			
			 			
			 			
			 		//	 local logic end
			 			recordOut = RowFactory.create(ACID,GL_CODE,GL_SUB_HEAD_CODE,ACCOUNT_NO,ACCT_NAME,ACCOUNT_HOLDER_NAME,CUSTOMER_CODE,OPENING_DATE,PRODUCT_CODE,CURRENCY_CODE,CLOSING_DATE,LINE_ID,BRANCH_CODE,DORMANT_DATE,ACCOUNT_TYPE,ACCOUNT_PREFIX,PSL_CODE,PSL_DESC, RELIGION_CODE,RELIGION, MATURITY_AMOUNT,AMOUNT,MATURITY_DATE,INTEREST_RATE,PP20_CODE,PP20_DESC, ADVANCE_TYPE,ADVANCE_PURPOSE, ROI_TYPE, BORROWER_CODE,OCCUPATION_CODE,OCCUPATION_DESC,BSR3_COMMODITY_CODE,SUBSIDY,INITIAL_SANCTION_LIMIT,INITIAL_SANCTION_DATE,INVESTMENT_FLAG,LAST_CREDIT_TRAN_DATE,LAST_DEBIT_TRAN_DATE,INITIAL_FUNDING,OPENING_TRAN_DATE,ACCT_BUSINESS_SEGMENT,BOOKING_DATE,LIVE_CLOSED,DEPOSIT_STATUS,SCHEME_DESCRIPTION,RM,MODE_OF_OPERATION,MODE_OF_OPERATION_DESC,BAR_CODE,ASSET_CLASSIFICATION,PROMO_CODE,DEBIT_CARD,NPA_FLG,NPA_DATE,BRANCH_NAME,LC_CODE,LG_CODE,BUSINESS_SEG_DESC,ACCOUNT_OWNERSHIP,CHEQ_ALLOWED,FREEZE_CODE,FREEZE_DESC,FREEZE_REASON_CODE,FREEZE_REASON_DESC,RISK_CODE,ENTERER_ID,INACTIVE_DATE,IB_LAST_TRAN_DATE,ATM_LAST_TRAN_DATE,LOAN_PAYOFF_DATE,REASON_FOR_CLOSURE,EMIAMT,AQB_WAIVER,RATE_CODE,EMI_START_DATE,LINK_OPER_ACCOUNT,ACCOUNT_STATUS,GOV_SCHEME_CODE,ROI_CARD_RATE,PAYOFF_FLG,LR_FREQ_TYPE,APP_REF_NO,PREFERED_ACCOUNT,CHRGE_OFF_DATE,REASON_CODE,CC_RENEWAL_DATE,PTC_FLAG,GROUP_CODE,URN_NO,RENEWAL_FLAG,CHARGE_OFF_FLG,DEPOSIT_PERIOD_MTHS,DEPOSIT_PERIOD_DAYS,DSB_FLAG,PROMO_CODE_N,RISK_CATEGORY_CODE,RISK_DESC,CUSTOMERNREFLG,LIMIT_REVIEW_DATE,LIMIT_EXPIRY_DATE,DRWNG_POWER,DRWNG_POWER_IND,LCHG_USER_ID,RCRE_USER_ID,SCHM_TYPE,INDUSTRY_TYPE,LINKED_SB_ACID,SOURCE_DEAL_CODE,DISBURSE_DEAL_CODE,CLOSURE_FLAG,LAST_FREZ_DATE,ACCT_STATUS_DATE,LAST_TRAN_DATE,PEG_REVIEW_DATE,APPLICABLE_DATE,ACCT_LVL_GROUPING,AADHAR_SEEDING_YN,PROJECT_FINANCE_YN,CHRG_LEVEL_CODE,SIGNATURE_STATUS,EMI_TYPE,PAYMENT_METHOD,repricing_plan,REPAYMENT_ACID);
			 			newRows.add(recordOut);
			 			return newRows.iterator();
			 		}},encoder);
			 	
			    System.out.println("ajoutDF");
			 	OutDF.show(60);
			 	System.out.println(" No of rows = " + OutDF.count() + "  & no of colums in outputDF  = " + OutDF.columns().length);		
			 //	OutDF.write().mode(SaveMode.Overwrite).csv(outPath);
			 	OutDF.coalesce(20)
			    .write().mode(SaveMode.Overwrite)
			    .option("header", "true")
			    .option("delimiter", "|")
			    .csv(outPath);
			 	//OutDF.coalesce(1).write().mode(SaveMode.Overwrite).csv("C:\Users\Administrator\Documents\outdf");
	}
	
	
	public static Dataset<Row> funcFetchGAMDF(SparkSession spark) {

		//Load required CSV data - Starts

		

		// 2. ADT - Starts

		printMessage("ADT----------------Starts");

		Dataset<Row> adt = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")

				.option("header", "true").load("resources/ADT.txt");

		// adt.show();

		adt.createOrReplaceTempView("ADT");



		Dataset<Row> adtDF = spark.sql("SELECT count(*) as cnt FROM ADT");

		adtDF.show();

		printMessage("ADT----------------Ends");

		// 2. ADT - Ends

		

		// 12. GCT - Starts

		printMessage("GCT----------------Starts");

		Dataset<Row> gct = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")

				.option("header", "true").load("resources/GCT.txt");

		// gct.show();

		gct.createOrReplaceTempView("GCT");



		Dataset<Row> gctDF = spark.sql("SELECT count(*) as cnt FROM GCT");

		gctDF.show();

		printMessage("GCT----------------Ends");

		// 12. GCT - Ends

		

		// 13. GEM - Starts

		printMessage("GEM----------------Starts");

		Dataset<Row> gem = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")

				.option("header", "true").load("resources/GEM.txt");

		// gem.show();

		gem.createOrReplaceTempView("GEM");



		Dataset<Row> gemDF = spark.sql("SELECT count(*) as cnt FROM GEM");

		gemDF.show();

		printMessage("GEM----------------Ends");

		// 13. GEM - Ends



		// 20. LRP - Starts

		printMessage("LRP----------------Starts");

		Dataset<Row> lrp = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")

				.option("header", "true").load("resources/LRP.txt");

		// lrp.show();

		lrp.createOrReplaceTempView("LRP");



		Dataset<Row> lrpDF = spark.sql("SELECT count(*) as cnt FROM LRP");

		lrpDF.show();

		printMessage("LRP----------------Ends");

		// 20. LRP - Ends

		

		// 5. C_GAM - Starts

		printMessage("C_GAM----------------Starts");

		Dataset<Row> c_gam = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")

				.option("header", "true").load("resources/C_GAM.txt");

		c_gam.show();

		c_gam.createOrReplaceTempView("C_GAM");

		

		printMessage("C_GAM----------------Ends");

		// 5. C_GAM - Ends

		

		//Load required CSV data - Ends

		

		// GAM Code Starts

		printMessage(" No of rows = " + adt.count() + "  & no of colums in adt  = " + adt.columns().length);

		printMessage(" No of rows = " + c_gam.count() + "  & no of colums in c_gam  = " + c_gam.columns().length);

		printMessage("gam1DF starts");

		Dataset<Row> gam1DF = adt.select("ACID")

							  //Commented as current date data not available - uncomment if we have live data

							  //.filter(functions.col("AUDIT_bod_DATE").geq(functions.trunc(functions.current_date(), "DD")))

							  .filter(functions.col("ACID").notEqual("!"));

		gam1DF.show();

		printMessage(" No of rows = " + gam1DF.count() + "  & no of colums in gam1DF  = " + gam1DF.columns().length);

		printMessage("gam1DF ends");

		printMessage("gam1UnionDF starts");

		Dataset<Row> gam1UnionDF = c_gam.select("ACID")

								  //Commented as current date data not available - uncomment if we have live data

								  //.filter(functions.col("LCHG_TIME").geq(functions.trunc(functions.current_date(), "DD")))

								  .filter(functions.col("ACCT_OWNERSHIP").notEqual("O"))

								  .filter(functions.col("DEL_FLG").equalTo("N"))

								  .filter(functions.col("ENTITY_CRE_FLG").equalTo("Y")).union(gam1DF).distinct();

		gam1UnionDF.show();

		printMessage(" No of rows = " + gam1UnionDF.count() + "  & no of colums in gam1UnionDF  = " + gam1UnionDF.columns().length);

		printMessage("gam1UnionDF ends");

		printMessage("gamOPDF starts");

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

		printMessage(" No of rows = " + gamOPDF.count() + "  & no of colums in gamOPDF  = " + gamOPDF.columns().length);

		printMessage("gamOPDF ends");

		StructField[] gamFields = {

	 			new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("GL_SUB_HEAD_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("FORACID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("ACCT_NAME", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("CIF_ID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("ACCT_OPN_DATE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("SCHM_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("ACCT_CRNCY_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("ACCT_CLS_DATE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("LIMIT_B2KID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("SOL_ID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("SANCT_LIM", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("GAM_RCRE_TIME", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("LIVE_CLOSED", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("SCHM_TYPE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("ACCT_MGR_USER_ID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("MODE_OF_OPER_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("ACCT_OWNERSHIP", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("CHQ_ALWD_FLG", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("FREZ_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("CHRG_LEVEL_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("FREEZE_DESC", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("FREZ_REASON_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("FREEZE_REASON_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("RCRE_USER_ID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("DRWNG_POWER", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("DRWNG_POWER_IND", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("LCHG_USER_ID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("SOURCE_DEAL_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("DISBURSE_DEAL_CODE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("LAST_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("LAST_FREZ_DATE", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("repricing_plan", DataTypes.StringType, true, Metadata.empty())

	 			};

		StructType gamSchema = new StructType(gamFields);

	 	

	 	ExpressionEncoder<Row> gamEncoder = RowEncoder.apply(gamSchema);



	 	Dataset<Row> gamDF = gamOPDF.flatMap(new FlatMapFunction<Row, Row>() {

	 		@Override

	 		public Iterator<Row> call(Row record) throws Exception {

	 			ArrayList<Row> newRows = new ArrayList<>();

	 			Row recordOut;

	 		//	 Local logic start

	 			String ACID = record.getString(0);

	 			String GL_SUB_HEAD_CODE = record.getAs("GL_SUB_HEAD_CODE");

	 			String FORACID = record.getAs("FORACID");

	 			String ACCT_NAME = record.getAs("ACCT_NAME");

	 			String CIF_ID = record.getAs("CIF_ID");

	 			String ACCT_OPN_DATE = record.getAs("ACCT_OPN_DATE");

	 			String SCHM_CODE = record.getAs("SCHM_CODE");

	 			String ACCT_CRNCY_CODE = record.getAs("ACCT_CRNCY_CODE");

	 			String ACCT_CLS_DATE = record.getAs("ACCT_CLS_DATE");

	 			String LIMIT_B2KID = record.getAs("LIMIT_B2KID");

	 			String SOL_ID = record.getAs("SOL_ID");

	 			String SANCT_LIM = record.getAs("SANCT_LIM");

	 			String RCRE_TIME = record.getAs("RCRE_TIME");

	 			String DC_CLS_DATE = record.getAs("DC_CLS_DATE").toString();

	 			String ACCT_OPN_BOD_DATE = record.getAs("ACCT_OPN_BOD_DATE").toString();

	 			DateFormat formatDate = new SimpleDateFormat("DD-MM-YYYY");

	 			Date fmtdate1 = formatDate.parse(DC_CLS_DATE);

	 			Date fmtdate2 = formatDate.parse("03-05-2016");

	 			Date fmtdate3 = formatDate.parse(ACCT_OPN_BOD_DATE);

	 			if(fmtdate1.after(fmtdate2))

	 			{

	 				RCRE_TIME = formatDate.format(fmtdate3);

	 			}

	 			String LIVE_CLOSED = "Closed";

	 			if(ACCT_CLS_DATE == null || ACCT_CLS_DATE.isEmpty())

	 			{

	 				LIVE_CLOSED = "Live";

	 			}

	 			String SCHM_TYPE = record.getAs("SCHM_TYPE");

	 			String ACCT_MGR_USER_ID = record.getAs("ACCT_MGR_USER_ID");

	 			String MODE_OF_OPER_CODE = record.getAs("MODE_OF_OPER_CODE");

	 			String ACCT_OWNERSHIP = record.getAs("ACCT_OWNERSHIP");

	 			String CHQ_ALWD_FLG = record.getAs("CHQ_ALWD_FLG");

	 			if(CHQ_ALWD_FLG == null)

	 			{

	 				CHQ_ALWD_FLG = "N";

	 			}

	 			String FREZ_CODE = record.getAs("FREZ_CODE");

	 			String CHRG_LEVEL_CODE = record.getAs("CHRG_LEVEL_CODE");

	 			String FREEZE_DESC = "NA";

	 			if(FREZ_CODE != null && FREZ_CODE.equals(" "))

	 			{

	 				FREEZE_DESC = "NO FREEZE";

	 			}

	 			else if(FREZ_CODE != null && FREZ_CODE.equals("D"))

	 			{

	 				FREEZE_DESC = "DEBIT FREEZE";

	 			}

	 			else if(FREZ_CODE != null && FREZ_CODE.equals("C"))

	 			{

	 				FREEZE_DESC = "CREDIT FREEZE";

	 			}

	 			else if(FREZ_CODE != null && FREZ_CODE.equals("T"))

	 			{

	 				FREEZE_DESC = "TOTAL FREEZE";

	 			}

	 			String FREZ_REASON_CODE = record.getAs("FREZ_REASON_CODE");

	 			String FREEZE_REASON_CODE = record.getAs("FREZ_REASON_CODE");

	 			if(FREZ_CODE == null)

	 			{

	 				FREEZE_REASON_CODE = null;

	 			}

	 			String RCRE_USER_ID = record.getAs("RCRE_USER_ID");

	 			String DRWNG_POWER = record.getAs("DRWNG_POWER");

	 			String DRWNG_POWER_IND = record.getAs("DRWNG_POWER_IND");

	 			String LCHG_USER_ID = record.getAs("LCHG_USER_ID");

	 			String SOURCE_DEAL_CODE = record.getAs("SOURCE_DEAL_CODE");

	 			String DISBURSE_DEAL_CODE = record.getAs("DISBURSE_DEAL_CODE");

	 			String LAST_TRAN_DATE = record.getAs("LAST_TRAN_DATE");

	 			String LAST_FREZ_DATE = record.getAs("LAST_FREZ_DATE");

	 			if(FREZ_CODE == null)

	 			{

	 				LAST_FREZ_DATE = null;

	 			}

	 			String repricing_plan = record.getAs("repricing_plan");

	 		//	 Local logic end

	 			recordOut = RowFactory.create(ACID,GL_SUB_HEAD_CODE,FORACID,ACCT_NAME,CIF_ID,ACCT_OPN_DATE,SCHM_CODE,

	 					ACCT_CRNCY_CODE,ACCT_CLS_DATE,LIMIT_B2KID,SOL_ID,SANCT_LIM,RCRE_TIME,LIVE_CLOSED,SCHM_TYPE,

	 					ACCT_MGR_USER_ID,MODE_OF_OPER_CODE,ACCT_OWNERSHIP,CHQ_ALWD_FLG,FREZ_CODE,CHRG_LEVEL_CODE,

	 					FREEZE_DESC,FREZ_REASON_CODE,FREEZE_REASON_CODE,RCRE_USER_ID,DRWNG_POWER,DRWNG_POWER_IND,

	 					LCHG_USER_ID,SOURCE_DEAL_CODE,DISBURSE_DEAL_CODE,LAST_TRAN_DATE,LAST_FREZ_DATE,repricing_plan);

	 			newRows.add(recordOut);

	 			return newRows.iterator();

	 		}},gamEncoder);

	 	gamDF.show();

	 	printMessage(" No of rows = " + gamDF.count() + "  & no of colums in gamSCDF  = " + gamDF.columns().length);

		

		//GAM Code - Ends

	 	return gamDF;

	}



	public static Dataset<Row> funcFetchSDF(SparkSession spark) {

		//Load required CSV data - Starts

		

		// 23. SVSUSER_NSIGNCUSTINFO - Starts

		printMessage("SVSUSER_NSIGNCUSTINFO----------------Starts");

		Dataset<Row> svsuser_nsigncustinfo = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")

				.option("header", "true").load("resources/SVSUSER_NSIGNCUSTINFO.txt");

		// svsuser_nsigncustinfo.show();

		svsuser_nsigncustinfo.createOrReplaceTempView("SVSUSER_NSIGNCUSTINFO");

		svsuser_nsigncustinfo.show();

		printMessage("SVSUSER_NSIGNCUSTINFO----------------Ends");

		// 23. SVSUSER_NSIGNCUSTINFO - Ends

		

		// 24. SVSUSER_SIGNCUSTINFO - Starts

		printMessage("SVSUSER_SIGNCUSTINFO----------------Starts");

		Dataset<Row> svsuser_signcustinfo = spark.read().format("com.databricks.spark.csv").option("delimiter", "|")

				.option("header", "true").load("resources/SVSUSER_SIGNCUSTINFO.txt");

		// svsuser_signcustinfo.show();

		svsuser_signcustinfo.createOrReplaceTempView("SVSUSER_SIGNCUSTINFO");



		Dataset<Row> svsuser_signcustinfoDF = spark.sql("SELECT count(*) as cnt FROM SVSUSER_SIGNCUSTINFO");

		svsuser_signcustinfoDF.show();

		printMessage("SVSUSER_SIGNCUSTINFO----------------Ends");

		// 24. SVSUSER_SIGNCUSTINFO - Ends

		

		//Load required CSV data - Ends

		

		//S Code Starts

		StructField[] s1Fields = {

	 			new StructField("CUSTID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("TAG", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("MODIFIEDDATE", DataTypes.StringType, true, Metadata.empty())

	 			};

		StructType s1OPSchema = new StructType(s1Fields);

	 	

	 	ExpressionEncoder<Row> s1Encoder = RowEncoder.apply(s1OPSchema);



	 	Dataset<Row> s1OPDF = svsuser_nsigncustinfo.flatMap(new FlatMapFunction<Row, Row>() {

	 		@Override

	 		public Iterator<Row> call(Row record) throws Exception {

	 			ArrayList<Row> newRows = new ArrayList<>();

	 			Row recordOut;

	 		//	 Local logic start

				String CUSTID = record.isNullAt(0)? null:record.getAs("CUSTID").toString().trim();

	 			String TAG = "1";

	 			String MODIFIEDDATE = record.isNullAt(1)? null:record.getAs("MODIFIEDDATE");

	 		//	 Local logic end

	 			recordOut = RowFactory.create(CUSTID,TAG,MODIFIEDDATE);

	 			newRows.add(recordOut);

	 			return newRows.iterator();

	 		}},s1Encoder);

	 	s1OPDF.show();

	 	printMessage("s1OPDF No of rows = " + s1OPDF.count());

	 	

	 	Dataset<Row> s1OPFilterDF = s1OPDF.filter(s1OPDF.col("CUSTID").isNotNull());

	 	printMessage("s1OPFilterDF No of rows = " + s1OPFilterDF.count());

	 	

	 	Dataset<Row> s2OPDF = svsuser_signcustinfo.flatMap(new FlatMapFunction<Row, Row>() {

	 		@Override

	 		public Iterator<Row> call(Row record) throws Exception {

	 			ArrayList<Row> newRows = new ArrayList<>();

	 			Row recordOut;

	 		//	 Local logic start

				String CUSTID = record.isNullAt(0)? null:record.getAs("CUSTID").toString().trim();

	 			String TAG = "1";

	 			String MODIFIEDDATE = record.isNullAt(1)? null:record.getAs("MODIFIEDDATE");

	 		//	 Local logic end

	 			recordOut = RowFactory.create(CUSTID,TAG,MODIFIEDDATE);

	 			newRows.add(recordOut);

	 			return newRows.iterator();

	 		}},s1Encoder);

	 	s2OPDF.show();

	 	printMessage("s2OPDF No of rows = " + s2OPDF.count());



	 	Dataset<Row> s2OPFilterDF = s2OPDF.filter(s2OPDF.col("CUSTID").isNotNull());

	 	printMessage("s2OPFilterDF No of rows = " + s2OPFilterDF.count());

	 	

	 	Dataset<Row> s1UnionDF = s1OPFilterDF.union(s2OPFilterDF);

	 	printMessage("s1UnionDF No of rows = " + s1UnionDF.count());

	 	

	 	Dataset<Row> s1UnionSortDF = s1UnionDF.withColumn("SRNO", functions.row_number().over(Window.partitionBy("CUSTID").orderBy("CUSTID")

	 								.orderBy(functions.col("MODIFIEDDATE").desc())).alias("SRNO")).filter(functions.col("SRNO").equalTo("1"));

	 	s1UnionSortDF.show();

	 	printMessage("s1UnionSortDF No of rows = " + s1UnionSortDF.count());

	 	

	 	// S_DF CASE STATEMENT STARTS

	 	StructField[] sFields = {

	 			new StructField("CUSTID", DataTypes.StringType, true, Metadata.empty()),

	 			new StructField("SIGNATURE_STATUS", DataTypes.StringType, true, Metadata.empty())

	 			};

		StructType sSchema = new StructType(sFields);

	 	

	 	ExpressionEncoder<Row> sEncoder = RowEncoder.apply(sSchema);



	 	Dataset<Row> sDF = s1UnionSortDF.flatMap(new FlatMapFunction<Row, Row>() {

	 		@Override

	 		public Iterator<Row> call(Row record) throws Exception {

	 			ArrayList<Row> newRows = new ArrayList<>();

	 			Row recordOut;

	 		//	 Local logic start

				String CUSTID = record.getAs("CUSTID");

	 			String SIGNATURE_STATUS = "NOT LINKED";

	 			String TAG = record.getAs("TAG");

	 			if(TAG.equals("1"))

	 			{

	 				SIGNATURE_STATUS = "APPROVED";

	 			}

	 			else if(TAG.equals("3"))

	 			{

	 				SIGNATURE_STATUS = "NOT APPROVED";

	 			}

	 		//	 Local logic end

	 			recordOut = RowFactory.create(CUSTID,SIGNATURE_STATUS);

	 			newRows.add(recordOut);

	 			return newRows.iterator();

	 		}},sEncoder);

	 	sDF.show();

	 	printMessage("sDF No of rows = " + sDF.count());

	 	// S_DF CASE STATEMENT ENDS

	 	//S Code Ends

	 	return sDF;

	}

		

	public static void printMessage(String message) {

		System.out.println(message);

	}
	
}
