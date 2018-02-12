package com.dongqiang.bigdata.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LocalFile {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("C:\\Users\\qiang\\Desktop\\spark.txt");
		int count = lines.map(line -> line.length()).reduce((value1, value2) -> value1 + value2);
		System.out.println(count);
		sc.close();
	}

}
