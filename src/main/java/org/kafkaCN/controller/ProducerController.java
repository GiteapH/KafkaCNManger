package org.kafkaCN.controller;

import io.swagger.annotations.Api;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.kafkaCN.common.Result;
import org.kafkaCN.controller.domain.ProducerSendParams;
import org.kafkaCN.service.KafkaConsumerService;
import org.kafkaCN.service.KafkaProducerService;

import java.util.*;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: kafka生产者连接
 * @date 2023/10/3 11:54
 */
@RestController
@Api(tags = "kafka生产者连接")
@CrossOrigin
public class ProducerController {

    @Resource
    KafkaProducerService kafkaProducerService;

    @Resource
    KafkaConsumerService kafkaConsumerService;
    private final Map<String,KafkaTemplate<String,String>> kafkaTemplates = new ConcurrentHashMap<>();

    @PostMapping("/addConnection/{addr}")
    public Result addConnection(@PathVariable(value = "addr") String addr){
        try {
            KafkaTemplate<String, String> tKafkaTemplate = kafkaProducerService.kafkaTemplate(addr);
            Map<String, List<PartitionInfo>> topics = kafkaConsumerService.checkContainer(addr);
            if(topics != null)
                kafkaTemplates.put(addr,tKafkaTemplate);
            else
                return Result.error("403","连接失败...");
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
        return Result.success(kafkaTemplates.size());
    }


    @PostMapping("/sendMessage/{addr}")
    public Result sendMessage(@PathVariable(value = "addr") String addr, @RequestBody ProducerSendParams producerSendParams){
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplates.getOrDefault(addr, null);
        if(kafkaTemplate==null){
            return Result.error("不存在"+addr + "的连接");
        }
        try {
            ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(producerSendParams.getProducerRecord());
//            发送回调
            send.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onFailure(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onSuccess(SendResult<String, String> stringStringSendResult) {

                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
        return Result.success("发送成功");
    }
    
}
