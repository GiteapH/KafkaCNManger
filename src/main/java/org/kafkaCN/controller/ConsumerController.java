package org.kafkaCN.controller;

import com.alibaba.fastjson.JSONArray;
import io.swagger.annotations.Api;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.tomcat.jni.Poll;
import org.kafkaCN.Exception.ContainException;
import org.kafkaCN.common.Logger;
import org.kafkaCN.common.Result;
import org.kafkaCN.controller.domain.PollParams;
import org.kafkaCN.controller.domain.SQLParams;
import org.kafkaCN.controller.domain.ShutDownParams;
import org.kafkaCN.service.KafkaAdminService;
import org.kafkaCN.service.KafkaConsumerService;

import java.io.File;
import java.text.ParseException;
import java.util.*;

import org.kafkaCN.service.SparkService;
import org.kafkaCN.utils.JSONUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: kafka消费者连接
 * @date 2023/10/3 11:54
 */
@RestController
@Api(tags = "kafka消费者连接")
@CrossOrigin
@RequestMapping("/consumer")
public class ConsumerController {

    org.apache.log4j.Logger logger = Logger.getInstance();

    @Resource
    KafkaConsumerService kafkaConsumerService;

    @Resource
    SparkService sparkService;

    @Resource
    KafkaAdminService kafkaAdminService;

    public static String currentTopic = "";
    private final Map<String,KafkaConsumer<String, String>> kafkaTemplates = new ConcurrentHashMap<>();

    private final Set<String>  groupsStates = new CopyOnWriteArraySet<>();


    @PostMapping("/addConsumer/{addr}/{groupId}/{topic}")
    public Result addConsumer(@PathVariable(value = "addr") String addr,@PathVariable(value = "groupId") String groupId,@PathVariable(value = "topic") String topic){
        String[] hp = addr.split(":");
        currentTopic = topic;
//       添加已存在的消费组,验证消费组状态
        if(groupsStates.contains(groupId)){
            //            已创建连接的验证group状态
            try {
                try {
                    kafkaAdminService.create(hp[0], Integer.valueOf(hp[1]));
                } catch (ContainException ignored) {
                }
                Map<String, Map<String, Object>> descriptions = kafkaAdminService.groupsDescription(addr, groupId.split(" "));
                System.err.println(descriptions);

//                处于Rebalance直接返回客户端
                if("PREPARING_REBALANCE".equals(descriptions.get(groupId).get("state"))){
                    return Result.error("负责处理的消费组正在重新平衡");
                }
            } catch (ContainException e) {
                e.printStackTrace();
                return Result.error("管理连接已失效");
            }catch (RuntimeException e){
                e.printStackTrace();
                return Result.error(e.getMessage());
            }
//            状态正常
            //    访问已存在的连接
            if(kafkaTemplates.containsKey(addr+groupId+topic)){
                return Result.success("连接已创建，消费组状态正常");
            }
//            创建新连接
            try {
                KafkaConsumer<String, String> tKafkaConsumer = kafkaConsumerService.create(addr, groupId, topic);
                kafkaTemplates.put(addr+groupId+topic,tKafkaConsumer);
            } catch (Exception e) {
                e.printStackTrace();
                return Result.error(e.getMessage());
            }
            return Result.success(kafkaTemplates.size());
        }else {
//            不存在该消费组,直接创建新连接
            try {
                KafkaConsumer<String, String> tKafkaConsumer = kafkaConsumerService.create(addr, groupId, topic);
                kafkaTemplates.put(addr + groupId + topic, tKafkaConsumer);
                groupsStates.add(groupId);
            } catch (Exception e) {
                e.printStackTrace();
                return Result.error(e.getMessage());
            }
            return Result.success(kafkaTemplates.size());
        }
    }


    @PostMapping("/sendExample/{addr}/{groupId}/{topic}")
    public Result sendExample(@PathVariable(value = "addr") String addr, @PathVariable(value = "groupId") String groupId, @PathVariable(value = "topic") String topic, @RequestBody PollParams pollParams){
        KafkaConsumer<String, String> kafkaConsumer = kafkaTemplates.getOrDefault(addr+groupId+topic, null);
        if(kafkaConsumer==null){
            return Result.error("不存在"+addr + "的会话");
        }

//      返回测试数据
        JSONArray dataExample;
        try {
            dataExample = kafkaConsumerService.dataExample(kafkaConsumer,pollParams);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
        return Result.success(dataExample);
    }

    @PostMapping("/shutdown")
    public Result shutdown(@RequestBody ShutDownParams[] shutDownParamsa){
        Map<String,String> map = new HashMap<>();
        for(ShutDownParams shutDownParams:shutDownParamsa) {
            String addr = shutDownParams.getHost() + ":" + shutDownParams.getPort() + shutDownParams.getGroupId() + shutDownParams.getTopic();
            if (kafkaTemplates.containsKey(addr)) {
                kafkaTemplates.get(addr).close();
                kafkaTemplates.remove(addr);
                groupsStates.remove(shutDownParams.getGroupId());
                map.put(addr,"success");
            }else{
                map.put(addr,"error");
            }
        }
        return Result.success(map);
    }

    @PostMapping("/consumer/sql/{addr}/{groupId}/{topic}")
    public Result sql(@RequestBody SQLParams SQLParams,
                      @PathVariable(value = "addr") String addr,
                      @PathVariable(value = "groupId") String groupId,
                      @PathVariable(value = "topic") String topic
                      ){
        KafkaConsumer<String, String> kafkaConsumer = kafkaTemplates.getOrDefault(addr+groupId+topic, null);
        if(kafkaConsumer==null){
            return Result.error("不存在"+addr + "的会话");
        }
        JSONArray dataExample;
        String path = "./" + addr.split(":")[0] + "/" + groupId + "/" + topic+".json";
        try {
            dataExample = kafkaConsumerService.dataExample(kafkaConsumer,SQLParams.getPollParams());
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
        System.err.println(dataExample);

//        存在消费数据
        if(dataExample.size()>0)
         {
             try{
                File saveFile = JSONUtils.saveFile(dataExample, path, SQLParams.getKeys());//生成json，spark读取
                if (saveFile != null) {
                    Dataset<Row> dataset = sparkService.getDatasetByPath(path);
                    dataset.show();
                    dataset.createOrReplaceTempView(SQLParams.getTable() != null ? SQLParams.getTable() : topic);
                    dataset.sqlContext().sql("REFRESH TABLE  " + (SQLParams.getTable() != null ? SQLParams.getTable() : topic));
                    Dataset<Row> result = dataset.sqlContext().sql(SQLParams.getSql());
                    List<String> resultStrings = result.toJSON().collectAsList();
//                终止spark任务，释放资源
                    sparkService.stop();
//                boolean delete = saveFile.delete();
//                if(!delete){
//                    logger.error(path+" 删除失败");
//                    return Result.error(path+" 删除失败");
//                }
                    return Result.success(resultStrings.toString());
                } else {
                    logger.error(path + " 创建失败");
                    return Result.error(path + " 创建失败");
                }
            }catch (Exception e){
                 return Result.error(e.getMessage());
             }
        }
        return Result.error("无消费数据");
    }
    @GetMapping("/metrics")
    public Result metrics(@RequestParam(required = false) String host,@RequestParam Integer port,@RequestParam String groupId,@RequestParam String topic){
        try {
            String addr = host+":"+port+groupId+topic;
            KafkaConsumer<String, String> template = kafkaTemplates.getOrDefault(addr, null);
            if(template == null)return Result.error(addr+""+"未连接");
            Map<MetricName, ? extends Metric> metrics = template.metrics();
            Map<String, Map<String, String>> metricsRet = new HashMap<>();
            metrics.forEach(new BiConsumer<MetricName, Metric>() {
                @Override
                public void accept(MetricName metricName, Metric metric) {
                    Map<String, String> tips = new HashMap<>();
                    tips.put("description", metricName.description());
                    tips.put("group", metricName.group());
                    tips.put("value", metric.metricValue().toString());
                    metricsRet.put(metricName.name(), tips);
                }
            });
            return Result.success(metricsRet);
        }catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
    }
}


