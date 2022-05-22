package com.spark.es.demo;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * 先要在控制台执行：nc -lk 9999
 *
 * @author wenzhihuai
 * @since 2022/5/15 19:22
 */
public class EsStructuredStreaming extends EsBaseConfig {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkConf sparkConf = getSparkConf();
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();


        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();


    }
}
