package com.dongqiang.bigdata.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 统计每行出现的次数
 * @author dongqiang
 *
 */
public class LineCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("C:\\Users\\qiang\\Desktop\\hello.txt");
		//对lines RDD执行mapToPair算子，将每一行映射为(line, 1)的这种key-value对的格式
		JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(line -> new Tuple2<>(line, 1));
		//统计每一行出现的次数
		JavaPairRDD<String, Integer> lineCounts = pairRDD.reduceByKey((v1, v2) -> (v1 + v2));
		// 执行一个action操作，foreach，打印出每一行出现的次数
		lineCounts.foreach(lineCount -> System.out.println("line=" + lineCount._1 + ", count=" + lineCount._2));
		
		sc.close();
	}

}
