package com.spark.es.demo;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static com.spark.es.demo.EsBaseConfig.getSparkConf;

/**
 * @author wenzhihuai
 * @since 2022/5/22 20:39
 */
public class EsStructuredStreamingPerson {
    @Data
    public static class PersonBean {
        private String name;
        private String surname;
    }

    public static void main(String[] args) throws StreamingQueryException {
        SparkConf sparkConf = getSparkConf();
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();


        Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();

        Dataset<PersonBean> people = lines.as(Encoders.STRING())
                .map((MapFunction<String, PersonBean>) value -> {
                    String[] split = value.split(",");
                    PersonBean personBean = new PersonBean();
                    personBean.setName(split[0]);
                    return personBean;
                }, Encoders.bean(PersonBean.class));

        StreamingQuery es = people.writeStream().option("checkpointLocation", "./location")
                .format("es").start("spark-structured-streaming");
        es.awaitTermination();
    }
}
