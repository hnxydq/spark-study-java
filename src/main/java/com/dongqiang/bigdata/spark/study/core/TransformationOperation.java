package com.dongqiang.bigdata.spark.study.core;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TransformationOperation {
	
	/**
	 * 1、map：将集合中每个元素乘以2
	 */
	private static void map() {
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> rdd = sc.parallelize(numbers);
		JavaRDD<Integer> multipleRDD = rdd.map(item -> item * 2);
		multipleRDD.foreach(item -> System.out.println(item));
		sc.close();
	}
	
	/**
	 * 2、filter：过滤出集合中的偶数
	 */
	private static void filter() {
		SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> rdd = sc.parallelize(numbers);
		rdd.filter(item -> item % 2 == 0).foreach(item -> System.out.println(item));
		sc.close();
	}
	
	/**
	 * flatMap案例：将文本行拆分为多个单词
	 */
	private static void flatMap() {
		SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> list = Arrays.asList("hello world", "hello hello", "hello nihao");
		JavaRDD<String> wordsRDD = sc.parallelize(list);
		JavaRDD<String> wordRDD = wordsRDD.flatMap(item -> Arrays.asList(item.split(" ")));
		wordRDD.foreach(word -> System.out.println(word));
		sc.close();
	}
	
	/**
	 * groupByKey案例：按照班级对成绩进行分组
	 */
	private static void groupByKey() {
		SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(new Tuple2<>("class1", 80), 
				new Tuple2<>("class2", 90), new Tuple2<>("class1", 75), new Tuple2<>("class2", 70));
		JavaPairRDD<String, Integer> scorePairRDD = sc.parallelizePairs(scoreList, 1);
		JavaPairRDD<String, Iterable<Integer>>  scoresRDD = scorePairRDD.groupByKey();
		scoresRDD.foreach(scorePair -> {
			System.out.println(scorePair._1 + ":");
			scorePair._2.forEach(item -> System.out.println(item));
			System.out.println("=============");
		});
		sc.close();
	}
	
	/**
	 * reduceByKey案例：统计每个班级的总分
	 */
	private static void reduceByKey() {
		SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(new Tuple2<>("class1", 80), 
				new Tuple2<>("class2", 90), new Tuple2<>("class1", 75), new Tuple2<>("class2", 70));
		JavaPairRDD<String, Integer> scorePairRDD = sc.parallelizePairs(scoreList);
		// 对每个key，都会将其value，依次传入call方法
		// 从而聚合出每个key对应的一个value
		// 然后，将每个key对应的一个value，组合成一个Tuple2，作为新RDD的元素
		JavaPairRDD<String, Integer> scoreRDD = scorePairRDD.reduceByKey((score1, score2) -> score1 + score2);
		scoreRDD.foreach(scoreTuple -> System.out.println(scoreTuple._1 + ":" + scoreTuple._2));
		sc.close();
	}
	
	/**
	 * sortByKey案例：按照学生分数进行排序
	 */
	private static void sortByKey() {
		SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, String>> scoreList = Arrays.asList(new Tuple2<>(80, "zhangsan"), 
				new Tuple2<>(70, "lisi"), new Tuple2<>(90, "wangwu"), new Tuple2<>(75, "tom"));
		JavaPairRDD<Integer, String> scorePairRDD = sc.parallelizePairs(scoreList);
		JavaPairRDD<Integer, String> sortscorePairRDD = scorePairRDD.sortByKey(false);//设为false降序排列，默认是升序
		sortscorePairRDD.foreach(score -> System.out.println(score._1 + ":" + score._2));
		sc.close();
	}
	
	/**
	 * join案例：打印学生成绩
	 */
	private static void join() {
		SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<>(1, "zhangsan"),
				new Tuple2<>(2, "lisi"),
				new Tuple2<>(3, "wangwu"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<>(1, 100),
				new Tuple2<>(2, 80),
				new Tuple2<>(3, 90));
		
		JavaPairRDD<Integer, String> namePairsRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, Integer> scorePairsRDD = sc.parallelizePairs(scoreList);
		JavaPairRDD<Integer, Tuple2<String, Integer>>  scoresRDD = namePairsRDD.join(scorePairsRDD);
		scoresRDD.foreach(scorenameTuple -> {
			System.out.println(scorenameTuple._2._1 + ":" + scorenameTuple._2._2);
		});
		sc.close();
	}
	
	
	/**
	 * cogroup案例：打印学生成绩
	 */
	private static void cogroup() {
		SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<>(1, "zhangsan"),
				new Tuple2<>(2, "lisi"),
				new Tuple2<>(3, "wangwu"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<>(1, 100),
				new Tuple2<>(2, 80),
				new Tuple2<>(3, 90),
				new Tuple2<>(1, 90),
				new Tuple2<>(2, 85),
				new Tuple2<>(3, 93));
		
		JavaPairRDD<Integer, String> namePairsRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, Integer> scorePairsRDD = sc.parallelizePairs(scoreList);
		JavaPairRDD<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>  scoresRDD = namePairsRDD.cogroup(scorePairsRDD);
		scoresRDD.foreach(namescorepairs -> {
			System.out.println("id: " + namescorepairs._1);
			System.out.println("name: " + namescorepairs._2._1);
			System.out.println("score: " + namescorepairs._2._2);
		});
		sc.close();
	}
	
	public static void main(String[] args) {
		//map();
		//filter();
		//flatMap();
		//groupByKey();
		//reduceByKey();
		//sortByKey();
		//join();
		cogroup();
	}
}
