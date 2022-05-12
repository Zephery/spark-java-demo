package com.spark.es.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.spark.es.demo.EsBaseConfig.getSparkConf;

/**
 * @author zepherywen
 * @since 2022/5/12 21:30
 */
@Slf4j
public class EsWriteDemo {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            JavaRDD<String> javaRDD = jsc.textFile("doc/UserBehavior.csv");

            JavaRDD<Map<String, String>> rddMap = javaRDD.map(line -> {
                String[] split = line.split(",");
                Map<String, String> data = new HashMap<>();
                data.put("user_id", split[0]);
                data.put("sku_id", split[1]);
                data.put("spu_id", split[2]);
                data.put("action_type", split[3]);
                data.put("@timestamp", split[4]);
                return data;
            }).sample(true, 100);

            JavaEsSpark.saveToEs(rddMap, "spark");
            log.info("");
        }
    }
}
