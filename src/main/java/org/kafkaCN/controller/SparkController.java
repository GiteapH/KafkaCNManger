package org.kafkaCN.controller;

import com.alibaba.fastjson.JSONArray;
import io.swagger.annotations.Api;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.kafkaCN.common.Result;
import org.kafkaCN.service.KafkaConsumerService;
import org.kafkaCN.service.SparkService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
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
public class SparkController {

    @Resource
    SparkService sparkService;


}
