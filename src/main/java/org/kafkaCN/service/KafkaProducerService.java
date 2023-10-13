package org.kafkaCN.service;

import org.apache.kafka.common.internals.Topic;
import org.springframework.kafka.core.KafkaTemplate;

public interface KafkaProducerService {
    KafkaTemplate<String, String> kafkaTemplate(String addr);

}
