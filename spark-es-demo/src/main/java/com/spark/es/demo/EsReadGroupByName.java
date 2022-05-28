package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zepherywen
 * @since 2022/4/20 22:40
 */
public class EsReadGroupByName {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark WordCount Application (java)");
        sparkConf.set("es.nodes", "es.wenzhihuai.com")
                .set("es.port", "80")
                .set("es.nodes.wan.only", "true")
                .set("es.net.http.auth.user", "elastic")
                .set("es.net.http.auth.pass", "elastic-admin");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "kibana_sample_data_ecommerce").values();

            System.out.println(esRDD.partitions().size());

            esRDD.map(x -> x.get("customer_full_name"))
                    .countByValue()
                    .forEach((x, y) -> System.out.println(x + ":" + y));
            TimeUnit.HOURS.sleep(1);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
