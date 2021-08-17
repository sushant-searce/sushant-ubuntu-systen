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

public class LHT_DF {

	public static void main(String[] args) {
		
		
System.out.println("LHT_DF");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();

		
		Dataset<Row> LHT = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/LHT.csv");
	
		Dataset<Row> T = LHT.select(LHT.col("ACID"), LHT.col("SANCT_LIM"), LHT.col("LIM_SANCT_DATE"), LHT.col("LIM_REVIEW_DATE")
				, LHT.col("LIM_EXP_DATE"), LHT.col("APPLICABLE_DATE"))
				.filter(LHT.col("STATUS").equalTo("A"))
				.filter(LHT.col("ENTITY_CRE_FLG").equalTo("Y"))
				.filter(LHT.col("DEL_FLG").equalTo("N"))
				.groupBy("ACID")
				.agg(sum(LHT.col("SANCT_LIM")), min(LHT.col("LIM_SANCT_DATE")), min(LHT.col("LIM_REVIEW_DATE"))
						, min(LHT.col("LIM_EXP_DATE")), min(LHT.col("APPLICABLE_DATE")))
				.filter(col("min(APPLICABLE_DATE)").$less$eq("2017-05-22"))
				.withColumnRenamed("sum(SANCT_LIM)","SANCT_LIM")
				.withColumnRenamed("min(LIM_SANCT_DATE)","LIM_SANCT_DATE")
				.withColumnRenamed("min(LIM_REVIEW_DATE)","LIM_REVIEW_DATE")
				.withColumnRenamed("min(LIM_EXP_DATE)","LIM_EXP_DATE")
				.withColumnRenamed("min(APPLICABLE_DATE)","APPLICABLE_DATE");
		  		
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
		
		Dataset<Row> LHT_DF  = T.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String ACID = record.getAs("ACID");
                String SANCT_LIM = record.getAs("SANCT_LIM");
                String LIM_SANCT_DATE = record.getAs("LIM_SANCT_DATE");
                String LIM_REVIEW_DATE = record.getAs("LIM_REVIEW_DATE");
                String LIM_EXP_DATE = record.getAs("LIM_EXP_DATE");
                String APPLICABLE_DATE = record.getAs("APPLICABLE_DATE");
                
                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ACID, SANCT_LIM,LIM_SANCT_DATE , LIM_REVIEW_DATE, LIM_EXP_DATE, APPLICABLE_DATE);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, LHT_DFencoder);
		
		LHT_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}

}
