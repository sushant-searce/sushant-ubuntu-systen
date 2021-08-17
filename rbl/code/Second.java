package org.demo_rbl;

import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class Second implements Serializable {
	  private String name;
	  private int age;

	  public String getName() {
	    return name;
	  }

	  public void setName(String name) {
	    this.name = name;
	  }

	  public int getAge() {
	    return age;
	  }

	  public void setAge(int age) {
	    this.age = age;
	  }


	public static void main(String[] args) {
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Java Spark SQL basic example")
				  .getOrCreate();
		
		Second person = new Second();
		person.setName("Andy");
		person.setAge(32);
		

		Encoder<Second> personEncoder = Encoders.bean(Second.class);
		Dataset<Second> javaBeanDS = spark.createDataset(
		  Collections.singletonList(person),
		  personEncoder
		);
		javaBeanDS.show();
		
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(
		    (MapFunction<Integer, Integer>) value -> value + 1,
		    integerEncoder);
		transformedDS.collect(); // Returns [2, 3, 4]

		String path = "/home/sushantnigudkar/Desktop/testingofrbl/people.json";
		Dataset<Second> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
	}
}
