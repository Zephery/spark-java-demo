package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * @author wenzhihuai
 * @since 2022/5/15 19:55
 */
public class EsSparkStreaming extends EsBaseConfig {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkConf conf = getSparkConf();
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> lines = sparkSession
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

// Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

// Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
