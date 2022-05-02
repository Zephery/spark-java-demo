package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * @author wenzhihuai
 * @since 2022/4/21 20:04
 */
public class EsReadByQuery extends EsBaseConfig {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            String query = "{\n" +
                    "  \"query\": {\n" +
                    "    \"bool\": {\n" +
                    "      \"must_not\": [\n" +
                    "        {\n" +
                    "          \"match\": {\n" +
                    "            \"customer_full_name.keyword\": \"zhihuaiwen\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "kibana_sample_data_ecommerce", query).values();
            esRDD.collect().forEach(System.out::println);
        }
    }
}
