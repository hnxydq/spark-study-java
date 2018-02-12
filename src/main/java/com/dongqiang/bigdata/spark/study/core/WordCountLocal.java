package com.dongqiang.bigdata.spark.study.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCountLocal {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("C:\\Users\\qiang\\Desktop\\test.txt", 1);
		/*JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String line) throws Exception {
				System.out.println("line=" + line);
				return Arrays.asList(line.split(" "));
			}
		});

		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println("word=" + wordCount._1 + ", count=" + wordCount._2);
				
			}
		});*/
		
		lines.flatMap(line -> Arrays.asList(line.split(" "))).mapToPair(word -> new Tuple2<String, Integer>(word, 1))
			.reduceByKey((count1, count2) -> count1 + count2).foreach(item -> {
				System.out.println("word=" + item._1 + ", count=" + item._2);
			});
		sc.close();
	}
}
