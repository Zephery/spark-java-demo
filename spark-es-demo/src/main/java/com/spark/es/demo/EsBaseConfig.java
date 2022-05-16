package com.spark.es.demo;

import org.apache.spark.SparkConf;

/**
 * @author zepherywen
 * @since 2022/4/20 22:43
 */
public abstract class EsBaseConfig {

    public static SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf().setAppName("Spark WordCount Application (java)");
        sparkConf.set("es.nodes", "es.wenzhihuai.com")
                .set("es.port", "80")
                .set("es.nodes.wan.only", "true")
                .set("es.net.http.auth.user", "elastic")
                .set("es.net.http.auth.pass", "elastic-admin")
                //去掉这三个字段，否则查询会报错
//                .set("es.mapping.exclude","geo.*,geoip.location")
//                .set("es.read.field.exclude","geo.*,geoip.location")
//                .set("es.read.field.as.array.exclude","geo*,geoip.location")
                .setMaster("local[*]");
        return sparkConf;
    }
}
