package org.demo2.rbl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Column;

public class j_query {

	public static void main(String[] args) {
		
		System.out.println("J_QUERY");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> a = spark.read().format("csv")
              .option("delimiter", ",")
              .option("header", "true")
              .load("/home/sushantnigudkar/Downloads/cdr_updated.csv");
		
//				a.show();
		
		Dataset<Row> b = spark.read().format("csv")
	              .option("delimiter", ",")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/Downloads/altered_htd.csv");
		
               
               Dataset<Row> h = a.join(b, a.col("ACCOUNT_NO").equalTo(b.col("ACCOUNT_NO")))
                                       .select(a.col("ACCOUNT_NO"),a.col("OPENING_TRAN_DATE"),a.col("ACCOUNT_PREFIX")
                                        ,b.col("ACCOUNT_NO").alias("b_ACCOUNT_NO"),b.col("VALUE_DATE"),b.col("PART_TRAN_TYPE"),b.col("PSTD_FLG")
                                        ,b.col("TRAN_AMT"))
                                       .filter(col("OPENING_TRAN_DATE").$greater$eq("2011-01-01"))
                                       .filter(col("ACCOUNT_PREFIX").isin("30","40","60","70","75"))
                                       .filter(col("VALUE_DATE").$less$eq("2015-01-01"))
                                       .filter(col("PART_TRAN_TYPE").equalTo("C"))
                                       .filter(col("PSTD_FLG").equalTo("Y"))                                       
                                       .groupBy("ACCOUNT_NO").agg(sum("TRAN_AMT"))
                                       .withColumnRenamed("sum(TRAN_AMT)","IP_VALUE");
               
//               a.show();
               
               
               Dataset<Row> i = a.join(h, a.col("ACCOUNT_NO").equalTo(h.col("ACCOUNT_NO")));
               
               i.show();
               

//                                       ;
//                                       
//        Dataset<Row> q = a.select(h.col("IP_VALUE"),(h.col("ACCOUNT_NO")))
//                                               .withColumnRenamed("IP_VALUE", "B_IP_VALUE")
//                                               .withColumnRenamed("ACCOUNT_NO", "B_ACCOUNT_NO");
//                         q.show();
//                         
//               Dataset<Row> i = a.join(h, a.col("ACCOUNT_NO")
//                               .equalTo(h.col("B_ACCOUNT_NO")),"left");
//               
//               i.show();              
	}

}
