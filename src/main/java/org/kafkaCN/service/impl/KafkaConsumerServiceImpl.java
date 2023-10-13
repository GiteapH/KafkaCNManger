package org.kafkaCN.service.impl;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafkaCN.common.Logger;
import org.kafkaCN.controller.domain.PollParams;
import org.kafkaCN.service.KafkaConsumerService;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 连接kafka
 * @date 2023/10/3 13:07
 */
@Service
public class KafkaConsumerServiceImpl implements KafkaConsumerService {
    org.apache.log4j.Logger logger = Logger.getInstance();
    private Map<String, Object> consumerConfigs(String addr,String groupId) {
        Map<String, Object> props = new HashMap<>();
        List<String> interceptors = new ArrayList<>();
        interceptors.add("org.kafkaCN.interceptor.JsonInterceptor");
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, addr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }


    @Override
    public KafkaConsumer<String, String> create(String addr,String groupId,String topic){
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs(addr, groupId));
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    @Override
    public JSONArray dataExample(KafkaConsumer<String, String> consumer, PollParams pollParams) {
        if(consumer == null){
            return null;
        }
//        offset
        if(pollParams.getPollType()==0){
           for(TopicPartition topicPartition: consumer.assignment()){
               consumer.seek(topicPartition,pollParams.getOffset());
           }
        }
//        latest
        else if (pollParams.getPollType()==1) {
            consumer.seekToEnd(consumer.assignment());
        }
//        earliest
        else {
            consumer.seekToBeginning(consumer.assignment());
        }
        JSONArray JSONArray = new JSONArray();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollParams.getDuration()));
        try {
            for (ConsumerRecord<String, String> record : records) {
//                获取record的offset
                long offset = record.offset();
//                消息格式化json
                try {
                    JSONObject jsonObject = JSONObject.parseObject(record.value());
                    JSONObject ret = new JSONObject();
                    ret.put("offset",offset);
                    ret.put("value",jsonObject);
                    ret.put("partition",record.partition());
                    System.err.println(ret);
                    JSONArray.add(ret);
                }catch (Exception e){
                    System.err.println("记录异常，不符合json格式");
                }
            }
//            读取后，重置偏移量
            consumer.seekToBeginning(records.partitions());
        }catch(Exception e){
            e.printStackTrace();
            logger.error(e);
        }

        return JSONArray;
    }

    @Override
    public Map<String, List<PartitionInfo>> checkContainer(String addr) {
        Map<String, List<PartitionInfo>> listTopics;
        try {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs(addr,"connect"));
            listTopics = consumer.listTopics(Duration.ofSeconds(15));
        } catch (KafkaException e) {
            e.printStackTrace();
            return null;
        }
//        该地址的kafka不存在或未开启
        return listTopics;
    }
}
