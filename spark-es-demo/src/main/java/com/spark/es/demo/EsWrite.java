package com.spark.es.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.List;

/**
 * @author zepherywen
 * @since 2022/4/20 22:27
 */
public class EsWrite {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark WordCount Application (java)");
        conf.set("es.nodes", "es.wenzhihuai.com")
                .set("es.port", "80")
                .set("es.nodes.wan.only", "true")
                .set("es.net.http.auth.user", "elastic")
                .set("es.net.http.auth.pass", "elastic-admin")
                .setMaster("local");
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {

            String json = "{\"category\":[\"Women's Shoes\",\"Women's Clothing\"],\"currency\":\"EUR\",\"customer_first_name\":\"Mary\",\"customer_full_name\":\"zhihuaiwen\",\"customer_gender\":\"FEMALE\",\"customer_id\":20,\"customer_last_name\":\"Hampton\",\"customer_phone\":\"\",\"day_of_week\":\"Wednesday\",\"day_of_week_i\":2,\"email\":\"mary@hampton-family.zzz\",\"manufacturer\":[\"Angeldale\",\"Gnomehouse\"],\"order_date\":\"2022-04-20T13:39:22+00:00\",\"order_id\":568706,\"products\":[{\"base_price\":109.99,\"discount_percentage\":0,\"quantity\":1,\"manufacturer\":\"Angeldale\",\"tax_amount\":0,\"product_id\":15826,\"category\":\"Women's Shoes\",\"sku\":\"ZO0672206722\",\"taxless_price\":109.99,\"unit_discount_amount\":0,\"min_price\":54.99,\"_id\":\"sold_product_568706_15826\",\"discount_amount\":0,\"created_on\":\"2016-12-14T13:39:22+00:00\",\"product_name\":\"Over-the-knee boots - black\",\"price\":109.99,\"taxful_price\":109.99,\"base_unit_price\":109.99},{\"base_price\":49.99,\"discount_percentage\":0,\"quantity\":1,\"manufacturer\":\"Gnomehouse\",\"tax_amount\":0,\"product_id\":11255,\"category\":\"Women's Clothing\",\"sku\":\"ZO0331903319\",\"taxless_price\":49.99,\"unit_discount_amount\":0,\"min_price\":25.99,\"_id\":\"sold_product_568706_11255\",\"discount_amount\":0,\"created_on\":\"2016-12-14T13:39:22+00:00\",\"product_name\":\"Jersey dress - dark navy and white\",\"price\":49.99,\"taxful_price\":49.99,\"base_unit_price\":49.99}],\"sku\":[\"ZO0672206722\",\"ZO0331903319\"],\"taxful_total_price\":159.98,\"taxless_total_price\":159.98,\"total_quantity\":2,\"total_unique_products\":2,\"type\":\"order\",\"user\":\"mary\",\"geoip\":{\"country_iso_code\":\"AE\",\"location\":{\"lon\":55.3,\"lat\":25.3},\"region_name\":\"Dubai\",\"continent_name\":\"Asia\",\"city_name\":\"Dubai\"},\"event\":{\"dataset\":\"sample_ecommerce\"}}";


            JavaRDD<String> javaRDD = jsc.parallelize(List.of(json));
            try {
                JavaEsSpark.saveJsonToEs(javaRDD, "kibana_sample_data_ecommerce");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
