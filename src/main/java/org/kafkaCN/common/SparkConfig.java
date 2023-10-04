package org.kafkaCN.common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: TODO
 * @date 2023/10/4 15:12
 */
@Configuration
public class SparkConfig {
    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String masterUri;

    @Bean
    public SQLContext SQLContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        return new SQLContext(sparkContext);
    }
}