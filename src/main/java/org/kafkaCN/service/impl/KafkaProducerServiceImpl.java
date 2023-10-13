package org.kafkaCN.service.impl;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafkaCN.service.KafkaProducerService;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 连接kafka
 * @date 2023/10/3 13:07
 */
@Service
public class KafkaProducerServiceImpl implements KafkaProducerService {
    private Map<String, Object> producerConfigs(String addr) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, addr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.RETRIES_CONFIG,2);
        return props;
    }

    private Map<String, Object> producerConfigsListener(String advitised,String addr) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, addr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    private ProducerFactory<String, String> producerFactory(String addr) {
        return new DefaultKafkaProducerFactory<>(producerConfigs(addr));
    }

    @Override
    public KafkaTemplate<String, String> kafkaTemplate(String addr) {
        return new KafkaTemplate<>(producerFactory(addr));
    }


}
