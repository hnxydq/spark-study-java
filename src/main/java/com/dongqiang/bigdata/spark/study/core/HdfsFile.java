package com.dongqiang.bigdata.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HdfsFile {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("hdfs://spark1.dongqiang.com:8020/spark.txt");
		Integer count = lines.map(line -> line.length()).reduce((v1, v2) -> v1 + v2);
		System.out.println(count);
		sc.close();
	}

}
