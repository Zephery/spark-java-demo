package com.spark.es.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * @author zepherywen
 * @since 2022/5/12 23:57
 */
@Slf4j
public class EsToMysqlDemo extends EsBaseConfig{
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "kibana_sample_data_ecommerce").values();
        }
    }
}
