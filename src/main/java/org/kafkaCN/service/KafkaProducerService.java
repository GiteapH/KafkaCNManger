package org.kafkaCN.service;

import org.springframework.kafka.core.KafkaTemplate;

public interface KafkaProducerService {
    KafkaTemplate<String, String> kafkaTemplate(String addr);

}
