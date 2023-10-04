package org.kafkaCN.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kafkaCN.utils.JSONUtils;

import java.util.*;
import java.util.Map;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: Json格式消费者拦截器
 * @date 2023/10/4 14:05
 */
public class JsonInterceptor implements ConsumerInterceptor<String,String> {
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        if (records == null || records.isEmpty()) {
            return records;
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            JSONObject valid = JSONUtils.isValid(record.value());
            if (valid!=null) {
                record = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }else{
                continue;
            }
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            List<ConsumerRecord<String, String>> consumerRecords = newRecords.getOrDefault(topicPartition, new ArrayList<ConsumerRecord<String, String>>());
            consumerRecords.add(record);
            newRecords.put(topicPartition, consumerRecords);
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
