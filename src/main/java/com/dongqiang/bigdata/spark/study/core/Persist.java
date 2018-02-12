package com.dongqiang.bigdata.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Persist {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> lines = sc.textFile("C:\\Users\\qiang\\Desktop\\spark.txt").cache();
		JavaRDD<String> rdd = sc.textFile("C:\\Users\\qiang\\Desktop\\spark.txt");
		JavaRDD<String> lines = rdd.cache();
		long begintime = System.currentTimeMillis();
		long count = lines.count();
		System.out.println("count= " + count);
		long endtime = System.currentTimeMillis();
		System.out.println("time= " + (endtime - begintime) + " milliseconds.");
		System.out.println("=====================");
		begintime = System.currentTimeMillis();
		count = lines.count();
		System.out.println("count= " + count);
		endtime = System.currentTimeMillis();
		System.out.println("second time= " + (endtime - begintime) + " milliseconds.");
		sc.close();
	}
}
