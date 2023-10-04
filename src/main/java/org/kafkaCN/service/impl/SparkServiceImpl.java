package org.kafkaCN.service.impl;

import org.apache.ibatis.jdbc.SQL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.kafkaCN.service.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: TODO
 * @date 2023/10/4 15:15
 */
@Service
public class SparkServiceImpl implements SparkService {
    @Autowired
    SQLContext SQLContext;
    @Override
    public Dataset<Row> getDatasetByPath(String path) {
        SQLContext.sparkContext().setJobGroup("job","运行sql命令",true);
        return SQLContext.read().json(path);
    }

    @Override
    public void stop() {
        SQLContext.sparkContext().cancelJobGroup("job");
    }


}
