package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * @author zepherywen
 * @since 2022/4/20 22:28
 */
public class EsRead extends EsBaseConfig {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {

            JavaPairRDD<String, Map<String, Object>> esRDD =
                    JavaEsSpark.esRDD(jsc, "kibana_sample_data_ecommerce");
            esRDD.collect().forEach(System.out::println);
        }
    }
}
