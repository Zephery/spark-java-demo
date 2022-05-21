package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

import static com.spark.es.demo.EsBaseConfig.getSparkConf;

/**
 * @author wenzhihuai
 * @since 2022/5/21 19:47
 */
public class EsMetricsDemo {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            SparkSession sparkSession = SparkSession.builder()
                    .config(conf)
                    .getOrCreate();
            JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "micrometer-metrics-2022-05-21").values();
            JavaRDD<Row> map = esRDD.filter(v -> v.containsKey("uri")).map(v -> {
                String uri = v.get("uri").toString();
                Double max = Double.parseDouble(v.get("max").toString());

                return RowFactory.create(uri, max);
            });

            Dataset<Row> dataset = sparkSession.createDataFrame(map, StructType.fromDDL("uri string,max double"));
//            dataset.show(2);

            Dataset<Row> count = dataset.select("uri").groupBy("uri").count();
            count.show();


        }
    }
}
