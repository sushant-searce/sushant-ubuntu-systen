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

public class GSH_DF {

	public static void main(String[] args) {
		
		System.out.println("DEL_FLG == 'N'");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark RBL project")
				  .getOrCreate();
		
		Dataset<Row> MRCT = spark.read().format("csv")
              .option("delimiter", "|")
              .option("header", "true")
              .load("/home/sushantnigudkar/GSH.csv");
	
		
		Dataset<Row> GSH_DFs = MRCT.select(MRCT.col("GL_CODE"), MRCT.col("GL_SUB_HEAD_CODE"))
				.distinct()
				.filter(MRCT.col("DEL_FLG").equalTo("N"));
		
				
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
		
		GSH_DF.show();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
	}
}




