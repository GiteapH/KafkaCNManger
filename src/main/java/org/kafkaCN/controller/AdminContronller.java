package org.kafkaCN.controller;

import io.swagger.annotations.Api;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.kafkaCN.Exception.ContainException;
import org.kafkaCN.common.Result;
import org.kafkaCN.controller.domain.ProducerSendParams;
import org.kafkaCN.service.KafkaAdminService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: kafka管理端
 * @date 2023/10/7 11:23
 */

@RestController
@Api(tags = "kafka管理端")
@CrossOrigin
@RequestMapping("/admin")
public class AdminContronller {
    @Resource
    KafkaAdminService kafkaAdminService;

    @GetMapping("/topics")
    public Result topics(@RequestParam String host,@RequestParam Integer port){
        try {
            ListTopicsResult topics = kafkaAdminService.getTopics(host, port);
            Map<String, TopicListing> mapKafkaFuture = topics.namesToListings().get();
            Set<String> names = mapKafkaFuture.keySet();
            String[] objects = names.toArray(new String[0]);
            Map<String, List<Map<String,Object>>>  configs = kafkaAdminService.topicsConfig(host, port, objects);
            Map<String, Object> stringObjectMap = kafkaAdminService.describeTopicInfo(host, port, objects,configs);
            return Result.success(stringObjectMap);
        } catch (ContainException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
    }

    @PostMapping("/createTopics")
    public  Result createTopics(@RequestBody ProducerSendParams[] sendParams){
        if(sendParams.length>0){
            try {
                kafkaAdminService.createNewTopic(sendParams);
                return Result.success();
            } catch (ContainException e) {
                return Result.error(e.getMessage());
            }
        }
        return Result.error("topic不能为空");
    }

}
