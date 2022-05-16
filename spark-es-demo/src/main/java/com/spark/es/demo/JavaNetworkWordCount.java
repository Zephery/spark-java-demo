package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 * <p>
 * Usage: JavaNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 */
public final class JavaNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[*]");
        try (JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1))) {

            // Create a JavaReceiverInputDStream on target ip:port and count the
            // words in input stream of \n delimited text (e.g. generated by 'nc')
            // Note that no duplication in storage level only for running locally.
            // Replication necessary in distributed scenario for fault tolerance.
            JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                    "localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER);
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
            JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey(Integer::sum);

            wordCounts.print();
            ssc.start();
            ssc.awaitTermination();
        }
    }
}