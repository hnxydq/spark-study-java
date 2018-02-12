package com.dongqiang.bigdata.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastVariable {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1, 2, 4, 6);
		JavaRDD<Integer> numRDD = sc.parallelize(numbers);
		final int factor = 3;
		Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
		JavaRDD<Integer> tripleRDD = numRDD.map(item -> item * factorBroadcast.value());
		tripleRDD.foreach(item -> System.out.println(item));
		sc.close();
	}
}
