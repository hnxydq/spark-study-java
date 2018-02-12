package com.dongqiang.bigdata.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 并行化集合创建RDD (累加1->10)
 * @author dongqiang
 *
 */
public class ParallizeCollection {
	
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf().setAppName("ParallizeCollection").setMaster("local");
		//创建JvaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//要通过并行化集合的方式创建RDD，调用SparkContext的parallelize()方法
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
		JavaRDD<Integer> numbersRDD = sc.parallelize(list);
		//执行reduce汇聚操作： 1+2=3 + 4 =7 +5 ...
		Integer sum = numbersRDD.reduce((item1, item2) -> item1 + item2);
		System.out.println(sum);
		
		sc.close();
	}

}
