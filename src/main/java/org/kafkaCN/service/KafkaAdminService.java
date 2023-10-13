package org.kafkaCN.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.kafkaCN.Exception.ContainException;
import org.kafkaCN.controller.domain.ProducerSendParams;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public interface KafkaAdminService {
    AdminClient create(String host,Integer port) throws ContainException;


//    获取所有topics
    ListTopicsResult getTopics(String host,Integer port) throws ContainException ;


    Map<String,Object> describeTopicInfo(String host, Integer port, String[] topicNames,Map<String, List<Map<String,Object>>>  configs) throws ExecutionException, InterruptedException, ContainException;

    Map<String,Map<String,Object>> groupsDescription(String addr, String[] groupIds) throws ContainException;

    Boolean createNewTopic(ProducerSendParams[] topicNames) throws ContainException;

    Map<String, List<Map<String,Object>>> topicsConfig(String host, Integer port, String[] topicName) throws ContainException;
}
