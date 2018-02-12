package com.dongqiang.bigdata.spark.study.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SortWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SortWordCount") 
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("C:\\Users\\qiang\\Desktop\\spark.txt");
		JavaRDD<String> wordsRDD = lines.flatMap(line -> Arrays.asList(line.split(" ")));
		JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
		JavaPairRDD<String, Integer> wordRDD = pairRDD.reduceByKey((count1, count2) -> count1 + count2);
		JavaPairRDD<Integer, String> countWordPairRDD = wordRDD.mapToPair(wordCountTuple -> 
			new Tuple2<Integer, String>(wordCountTuple._2, wordCountTuple._1));
		JavaPairRDD<Integer, String> sortCountWordRDD = countWordPairRDD.sortByKey(false);
		JavaPairRDD<String, Integer> sortWordCountRDD = sortCountWordRDD.mapToPair(
				sortCountWord -> new Tuple2<>(sortCountWord._2, sortCountWord._1));
		sortWordCountRDD.foreach(wordcount -> System.out.println(wordcount._1 + ":" + wordcount._2));
		sc.close();
	}
}
