package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.sparkproject.guava.collect.ImmutableList;
import org.sparkproject.guava.collect.ImmutableMap;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

/**
 * @author wenzhihuai
 * @since 2022/5/15 19:55
 */
public class EsSparkStreaming extends EsBaseConfig {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkConf conf = getSparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Seconds.apply(1));

        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
        Queue<JavaRDD<Map<String, ?>>> microbatches = new LinkedList<>();
        microbatches.add(javaRDD);
        JavaDStream<Map<String, ?>> javaDStream = jssc.queueStream(microbatches);

        JavaEsSparkStreaming.saveToEs(javaDStream, "spark-streaming");

        jssc.start();
    }
}
