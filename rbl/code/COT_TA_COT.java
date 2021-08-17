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
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;

public class COT_TA_COT {

	public static void main(String[] args) {
		
		System.out.println("cot_ta_cot");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> COT = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/cot.csv");
		
		
		Dataset<Row> TA_COT = spark.read().format("csv")
	              .option("delimiter", "|")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/TA_COT.csv");

		
		Dataset<Row> T = TA_COT.select(TA_COT.col("ACID"), TA_COT.col("CHRGE_OFF_PRINCIPAL") ,TA_COT.col("CHRGE_OFF_DATE"))
				.filter(TA_COT.col("ENTITY_CRE_FLG").equalTo("Y"))
				.filter(TA_COT.col("DEL_FLG").equalTo("N"))
				.union(COT.select(COT.col("ACID"), COT.col("CHRGE_OFF_PRINCIPAL"), COT.col("CHRGE_OFF_DATE"))
				.filter(COT.col("ENTITY_CRE_FLG").equalTo("Y"))
				.filter(COT.col("DEL_FLG").equalTo("N")));

		Dataset<Row> T_cot = T.select(T.col("ACID"), T.col("CHRGE_OFF_DATE"))
				.filter(T.col("CHRGE_OFF_DATE").notEqual("null"))
				.groupBy(T.col("ACID")).agg(max(T.col("CHRGE_OFF_DATE")))
				.withColumnRenamed("max(CHRGE_OFF_DATE)","CHRGE_OFF_DATE");
		
//		
//

		
		StructField[] V_DFschemaFields = {
                new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("CHRGE_OFF_DATE", DataTypes.StringType, true, Metadata.empty())      
		};
		
		
		StructType V_DFSchema = new StructType(V_DFschemaFields);
        ExpressionEncoder<Row> V_DFencoder = RowEncoder.apply(V_DFSchema);
		
		Dataset<Row> V_DF  = T_cot.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
            	
                String ACID = record.getAs("ACID");
                String CHRGE_OFF_DATE = record.getAs("CHRGE_OFF_DATE");
                

                ArrayList<Row> newRows = new ArrayList<>();
                Row recordOut;
                recordOut = RowFactory.create(ACID, CHRGE_OFF_DATE);
                newRows.add(recordOut);
                return newRows.iterator();
            }
		}, V_DFencoder);
		
		V_DF.show();
		

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}
