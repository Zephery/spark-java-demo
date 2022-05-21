package com.spark.es.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.List;
import java.util.Map;

/**
 * @author zepherywen
 * @since 2022/5/12 23:57
 */
@Slf4j
public class EsToMysqlDemo extends EsBaseConfig {
    public static void main(String[] args) {
        SparkConf conf = getSparkConf();
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            SparkSession sparkSession = SparkSession.builder()
                    .config(conf)
                    .getOrCreate();
            JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "kibana_sample_data_ecommerce").values();
            JavaRDD<Row> map = esRDD.map(v -> {
                String currency = v.get("currency").toString();
                String customerFullName = v.get("customer_full_name").toString();
                String productsSku = v.getOrDefault("products", "").toString();

                return RowFactory.create(currency, customerFullName, productsSku);
            });
            Dataset<Row> dataset = sparkSession.createDataFrame(map, StructType.fromDDL("currency string,customer_full_name string,products string"));
            dataset.show(2);

            Dataset<Row> count = dataset.select("currency").groupBy("currency").count();
            count.show(2);


        }
    }
}
