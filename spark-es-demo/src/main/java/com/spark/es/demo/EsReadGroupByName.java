package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * @author zepherywen
 * @since 2022/4/20 22:40
 */
public class EsReadGroupByName extends EsBaseConfig {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "kibana_sample_data_ecommerce").values();

            System.out.println(esRDD.partitions().size());

            esRDD.map(x -> x.get("customer_full_name"))
                    .countByValue()
                    .forEach((x, y) -> System.out.println(x + ":" + y));

        }
    }
}
