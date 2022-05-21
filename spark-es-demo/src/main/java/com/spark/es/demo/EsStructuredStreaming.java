package com.spark.es.demo;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author wenzhihuai
 * @since 2022/5/15 19:22
 */
public class EsStructuredStreaming extends EsBaseConfig {
    @Data
    public static class PersonBean {
        private String name;
        private String surname;
        private int age;

    }

    public static void main(String[] args) {
        SparkConf sparkConf = getSparkConf();


        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        Dataset<Row> lines = sparkSession
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();
    }
}
