package org.kafkaCN.service;

import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.kafkaCN.controller.domain.PollParams;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Map;

public interface KafkaConsumerService {
    KafkaConsumer<String, String> create(String addr, String groupId, String topic);

    JSONArray dataExample(KafkaConsumer<String, String> consumer, PollParams pollParams);

    Map<String, List<PartitionInfo>> checkContainer(String addr);


}
