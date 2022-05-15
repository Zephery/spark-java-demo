package com.spark.es.demo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author wenzhihuai
 * @since 2022/5/2 20:07
 */
@Slf4j
public class EsSparkSqlDemo extends EsBaseConfig {


    public static void main(String[] args) {
        SparkConf sparkConf = getSparkConf();


        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        sparkSession.table("kibana_sample_data_ecommerce");
        Dataset<Row> sql = sparkSession.sql("select * from kibana_sample_data_ecommerce");
        log.info("");
    }
}
