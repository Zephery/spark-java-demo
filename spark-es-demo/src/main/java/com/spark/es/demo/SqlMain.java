package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author zepherywen
 * @since 2022/4/24 20:44
 */
public class SqlMain extends EsBaseConfig {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        SparkSession sparkSession = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.sql("show tables ");
        dataset.show();
    }
}
