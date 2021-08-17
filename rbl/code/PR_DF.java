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
import org.apache.spark.sql.Column;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;

public class PR_DF {

	public static void main(String[] args) {
		
		System.out.println("NEXT_PEG_REVIEW_DATE != 'NULL'");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> MRCT = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/EIT.csv");
		
		Dataset<Row> EIT = MRCT.select(MRCT.col("ENTITY_ID"), MRCT.col("NEXT_PEG_REVIEW_DATE"))
				.filter(MRCT.col("NEXT_PEG_REVIEW_DATE").notEqual("null"));
		
//  in the column NEXT_PEG_REVIEW_DATE value is 'null' in string
//		EIT.show();
				
		StructField[] PR_DFschemaFields = {
                new StructField("ENTITY_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("NEXT_PEG_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty())
		};
		
		
		StructType PR_DFSchema = new StructType(PR_DFschemaFields);
        ExpressionEncoder<Row> PR_DFencoder = RowEncoder.apply(PR_DFSchema);
		
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
		
		PR_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}


