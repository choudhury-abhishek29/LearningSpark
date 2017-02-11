package com.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.Iterable;
import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;


public class WordCount 
{
	public static void main(String[] args) throws Exception 
	{
		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("E:/a.txt");
		
		JavaPairRDD<String, Integer> counts = input.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer>call(String x)
					{
						return new Tuple2(x,1);
					}
				}
				).reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer x, Integer y)
					{	return x+y; }
				});
//		try
//		{
//			counts.saveAsTextFile("E:/b.txt");	
//		}
//		catch(Exception e)
//		{
//			System.out.println(e.getMessage());
//		}
		
		Map<String, Integer> m = counts.collectAsMap();		
		for(String s : m.keySet())
		{
			System.out.println("Key: "+s+" Value: "+m.get(s));
		}
		
	}
}