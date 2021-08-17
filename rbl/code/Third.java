package org.demo_rbl;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.log4j.Logger;

import java.util.Iterator;
import org.aopalliance.reflect.Metadata;
import org.apache.avro.generic.GenericData.Array;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.log4j.Level;


public class Third {

	public static void main(String[] args) {
		
		System.out.println("hello");
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark SQL basic example")
				  .getOrCreate();
		
		Dataset<Row> sush = spark.read().format("csv")
              .option("delimiter", ",")
              .option("header", "true")
              .load("/home/sushantnigudkar/Desktop/testingofrbl/client.csv");

		Dataset<Row> sush1 = spark.read().format("csv")
	              .option("delimiter", ",")
	              .option("header", "true")
	              .load("/home/sushantnigudkar/Desktop/testingofrbl/cdr.csv");
		
//		sush.show();
//		sush1.show();
		
		Dataset<Row> joined = sush.join(sush1, sush1.col("ACNO").equalTo(sush.col("ACNO")))
							.select(sush1.col("Name"),sush1.col("ACNO"),sush1.col("Age"),sush.col("Address"));
		
//		joined.show();
		
//		Dataset<Row> leftanti =  sush.join(joined , sush.col("ACNO").equalTo(joined.col("ACNO")), "leftanti");
//		leftanti.show();
		
//		Dataset<Row> uni = leftanti.union(joined);
//		uni.show();
		
				
		StructField[] fpVisaOutschemaFields = {
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACNO", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Age", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Address", DataTypes.StringType, true, Metadata.empty())
                };
        StructType fpVisaOutSchema = new StructType(fpVisaOutschemaFields);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(fpVisaOutSchema);
        
        Dataset<Row> testdata  = joined.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row record) throws Exception {
                String name = record.getAs("Name");
                String acno = record.getAs("ACNO");
                Integer age = Integer.parseInt(record.getAs("Age"));
                String address = record.getAs("Address");
                
                Integer my2out =0;
                
                if (address.contentEquals("Mumbai")) {
                     my2out = age+1;
                }else {
                     my2out = age+2;    
                }
                
                Array<Row> newRows = new ArrayListIterator<>();
                Row recordOut;
                recordOut = RowFactory.create(name,acno,my2out,address);
                newRows.add(recordOut);
                return newRows.iterator();
            }
        },encoder);
        //testdata.show();
        		
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
	}

}
