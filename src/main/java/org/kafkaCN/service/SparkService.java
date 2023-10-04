package org.kafkaCN.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface SparkService {
    Dataset<Row> getDatasetByPath(String path);


    public void stop();
}
