package com.dongqiang.bigdata.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AccumulatorVariable {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Accumulator") 
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Accumulator<Integer> accumulator = sc.accumulator(0);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		numbers.foreach(item -> accumulator.add(item));
		System.out.println(accumulator.value());  
		sc.close();
	}

}
