package org.kafkaCN.controller;

import com.alibaba.fastjson.JSONArray;
import io.swagger.annotations.Api;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.kafkaCN.common.Logger;
import org.kafkaCN.common.Result;
import org.kafkaCN.service.KafkaConsumerService;

import java.io.File;
import java.util.*;

import org.kafkaCN.service.SparkService;
import org.kafkaCN.utils.JSONUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: kafka消费者连接
 * @date 2023/10/3 11:54
 */
@RestController
@Api(tags = "kafka消费者连接")
@CrossOrigin
public class ConsumerController {

    org.apache.log4j.Logger logger = Logger.getInstance();

    @Resource
    KafkaConsumerService kafkaConsumerService;

    @Resource
    SparkService sparkService;

    private final Map<String,KafkaConsumer<String, String>> kafkaTemplates = new ConcurrentHashMap<>();

    @PostMapping("/addConsumer/{addr}/{groupId}/{topic}")
    public Result addConsumer(@PathVariable(value = "addr") String addr,@PathVariable(value = "groupId") String groupId,@PathVariable(value = "topic") String topic){
        try {
            KafkaConsumer<String, String> tKafkaConsumer = kafkaConsumerService.create(addr, groupId, topic);
            if(!kafkaTemplates.containsKey("addr+groupId+topic"))
                kafkaTemplates.put(addr+groupId+topic,tKafkaConsumer);
            else
                return Result.error("会话已存在");
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
        return Result.success(kafkaTemplates.size());
    }


    @PostMapping("/sendExample/{addr}/{groupId}/{topic}")
    public Result sendExample(@PathVariable(value = "addr") String addr,@PathVariable(value = "groupId") String groupId,@PathVariable(value = "topic") String topic){
        KafkaConsumer<String, String> kafkaConsumer = kafkaTemplates.getOrDefault(addr+groupId+topic, null);
        if(kafkaConsumer==null){
            return Result.error("不存在"+addr + "的会话");
        }

//      返回测试数据
        JSONArray dataExample;
        try {
            dataExample = kafkaConsumerService.dataExample(kafkaConsumer);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
        Map<String,Object> retMap = new HashMap<>();
        return Result.success(dataExample);
    }


    @GetMapping("/consumer/sql/{addr}/{groupId}/{topic}")
    public Result sql(@RequestParam String sql,
                      @RequestParam String[] keys,
                      @PathVariable(value = "addr") String addr,
                      @PathVariable(value = "groupId") String groupId,
                      @PathVariable(value = "topic") String topic,
                      @RequestParam(required = false) String table){
        KafkaConsumer<String, String> kafkaConsumer = kafkaTemplates.getOrDefault(addr+groupId+topic, null);
        if(kafkaConsumer==null){
            return Result.error("不存在"+addr + "的会话");
        }
        JSONArray dataExample;
        String path = "./" + addr.split(":")[0] + "/" + groupId + "/" + topic+".json";
        try {
            dataExample = kafkaConsumerService.dataExample(kafkaConsumer);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }

//        存在消费数据
        if(dataExample.size()>0) {
            File saveFile = JSONUtils.saveFile(dataExample, path,keys);//生成json，spark读取
            if(saveFile!=null){
                Dataset<Row> dataset = sparkService.getDatasetByPath(path);
                dataset.createOrReplaceTempView(table!=null?table:topic);
                dataset.sqlContext().sql("REFRESH TABLE  "+(table!=null?table:topic));
                Dataset<Row> result = dataset.sqlContext().sql(sql);
                List<String> resultStrings = result.toJSON().collectAsList();
//                终止spark任务，释放资源
                sparkService.stop();

                boolean delete = saveFile.delete();
                if(!delete){
                    logger.error(path+" 删除失败");
                    return Result.error(path+" 删除失败");
                }
                return Result.success(resultStrings);
            }else {
                logger.error(path+" 创建失败");
                return Result.error(path+" 创建失败");
            }
        }
        return Result.error("无消费数据");
    }
}
