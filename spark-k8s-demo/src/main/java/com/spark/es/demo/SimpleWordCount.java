package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author wenzhihuai
 * @since 2022/5/14 16:37
 */
public class SimpleWordCount {

    public static void main(String[] args) throws Exception {

        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD")
                .set("spark.executor.memory", "2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // sample collection
        List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // parallelize the collection to two partitions
        JavaRDD<Integer> rdd = sc.parallelize(collection);

        System.out.println("Number of partitions : " + rdd.getNumPartitions());

        rdd.foreach((VoidFunction<Integer>) System.out::println);
    }
}
