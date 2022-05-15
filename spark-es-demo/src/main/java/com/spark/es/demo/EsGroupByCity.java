package com.spark.es.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;

/**
 * @author wenzhihuai
 * @since 2022/5/14 20:43
 */
@Slf4j
public class EsGroupByCity extends EsBaseConfig {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> rowDataset = EsSparkSQL.esDF(sparkSession, "kibana_sample_data_ecommerce");

        System.out.println(rowDataset.rdd().getNumPartitions());
        Dataset<Row> sql = rowDataset.groupBy("customer_full_name").count();


        System.out.println(sql.rdd().getNumPartitions());
        sql.coalesce(1).orderBy("count").foreach(row -> {
            String name = row.getString(0);
            long count = row.getLong(1);
            System.out.println("name:" + name + "  count:" + count);
        });
    }
}
