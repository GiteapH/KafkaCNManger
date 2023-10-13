package org.kafkaCN.service.impl;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.kafkaCN.Exception.ContainException;
import org.kafkaCN.controller.domain.ProducerSendParams;
import org.kafkaCN.service.KafkaAdminService;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 管理端
 * @date 2023/10/7 10:59
 */

@Service
public class KafkaAdminServiceImpl implements KafkaAdminService {

    static Map<String,AdminClient> kafkaAdminClients = new ConcurrentHashMap<>();

    private Map<String, Object> adminConfig(String addr) {
        Map<String, Object> props = new HashMap<>();
        List<String> interceptors = new ArrayList<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, addr);
        return props;
    }


    @Override
    public AdminClient create(String host,Integer port) throws ContainException {
        String addr = host+":"+port;
        if(kafkaAdminClients.containsKey(addr)){
            throw new ContainException(addr+"管理端连接已存在");
        }
        AdminClient adminClient = KafkaAdminClient.create(adminConfig(addr));
        kafkaAdminClients.put(addr, adminClient);
        return adminClient;
    }

    @Override
    public ListTopicsResult getTopics(String host,Integer port) throws ContainException {
        String addr = host+":"+port;
        if(!kafkaAdminClients.containsKey(addr)){
            throw new ContainException(addr+"连接不存在");
        }

        return kafkaAdminClients.get(addr).listTopics();
    }



    public  Map<String,Object> describeTopicInfo(String host,Integer port,String[] topicNames,Map<String, List<Map<String,Object>>>  configs) throws ExecutionException, InterruptedException, ContainException {
        String addr = host+":"+port;
        if(!kafkaAdminClients.containsKey(addr)){
            throw new ContainException(addr+"连接不存在");
        }
        AdminClient adminClient = kafkaAdminClients.get(addr);
        List<String> list = new ArrayList<String>(Arrays.asList(topicNames));
        //调用方法拿到信息
        DescribeTopicsResult topic = adminClient.describeTopics(list);
//        装载结果
//        topics        topic信息   topic的分区信息
        Map<String,Object> store = new HashMap<>();
        Map<String, TopicDescription> map = topic.all().get();
        for (Map.Entry<String, TopicDescription> entry : map.entrySet()) {
            Map<String,Object> topicInfo= new HashMap<>();
            topicInfo.put("config",configs.get( entry.getValue().name()));
            topicInfo.put("topicName", entry.getValue().name()); //当前topic的名字
            topicInfo.put("partitionNum",  entry.getValue().partitions().size());//当前topic的partition数量
            List<TopicPartitionInfo> listp = entry.getValue().partitions();
//           分区集合
            List<Map<String,Object>> partitions = new ArrayList<>();
            for (TopicPartitionInfo info : listp) {
                Map<String,Object> partitionMap =  new HashMap<>();
                partitionMap.put("partition",info.partition());
                partitionMap.put("leaderId",info.leader().id());//领导者所在机器id，也就是机器编号配置文件中的service id
                partitionMap.put("leaderHost",info.leader().host());//领导者所在机器host ip
                partitionMap.put("leaderPort",info.leader().port()); //领导者所在机器port
                List<Node> listInfo = info.replicas();
//                replicas副本集合
                List<Map<String,Object>> replicas = new ArrayList<>();
                //输出node信息
                for (Node n : listInfo) {
                    Map<String,Object> nodes = new HashMap<>();
                    nodes.put("nodeId",n.id()); //副本所在的node id
                    nodes.put("nodeHost",n.host());//副本所在node的host ip
                    nodes.put("nodePort",n.port()); //副本所在node的port
                    replicas.add(nodes);
                }
                partitionMap.put("replicas",replicas);//副本的信息，有多少会拿到多少
                partitions.add(partitionMap);//拿到topic的partitions相关信息
            }
            topicInfo.put("partitions",partitions);
            store.put(entry.getKey(),topicInfo);
        }

        System.err.println(store);
        return store;
    }

    @Override
    public Map<String,Map<String,Object>> groupsDescription(String addr, String[] groupIds) throws ContainException,RuntimeException{
        if(!kafkaAdminClients.containsKey(addr)){
            throw new ContainException(addr+"连接不存在");
        }
        AdminClient adminClient = kafkaAdminClients.get(addr);

//        消费组信息
        DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(Arrays.asList(groupIds));
        try {
            Map<String, ConsumerGroupDescription> groupDescriptions = describeConsumerGroupsResult.all().get();
            Map<String,Map<String,Object>> retDescriptions = new HashMap<>();
            groupDescriptions.forEach(new BiConsumer<String, ConsumerGroupDescription>() {
                @Override
                public void accept(String groupId, ConsumerGroupDescription consumerGroupDescription) {
                    HashMap<String, Object> infos = new HashMap<>();
                    infos.put("groupId",groupId);
//                    消费组状态
                    infos.put("state",consumerGroupDescription.state().name());
                    infos.put("isSimpleConsumerGroup",consumerGroupDescription.isSimpleConsumerGroup());
//                    分区策略
                    infos.put("partitionAssignor",consumerGroupDescription.partitionAssignor());
                    retDescriptions.put(groupId,infos);
                }
            });

            return retDescriptions;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Boolean createNewTopic(ProducerSendParams[] topicNames) throws ContainException {
        String addr = topicNames[0].getHost()+":"+topicNames[0].getPort();
        if(!kafkaAdminClients.containsKey(addr)){
            throw new ContainException(addr+"连接不存在");
        }
        AdminClient adminClient = kafkaAdminClients.get(addr);
        List<NewTopic> list = new ArrayList<>();
        for(ProducerSendParams topic:topicNames){
            System.err.println("topic:");
            System.err.println(topic.getTopic()+","+topic.getPartitions()+","+topic.getReplication());
            NewTopic newTopic = new NewTopic(topic.getTopic(),topic.getPartitions(),topic.getReplication());
            list.add(newTopic);
        }
        try {
            adminClient.createTopics(list).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public Map<String,List<Map<String,Object>>>  topicsConfig(String host, Integer port, String[] topicNames) throws ContainException {
        String addr = host+":"+port;
        if(!kafkaAdminClients.containsKey(addr)){
            throw new ContainException(addr+"连接不存在");
        }
        AdminClient adminClient = kafkaAdminClients.get(addr);
        List<ConfigResource> configResources = new ArrayList<>();
        for(String s:topicNames){
            configResources.add(new ConfigResource(ConfigResource.Type.TOPIC,s));
        }
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(configResources);
        try {
            Map<ConfigResource, Config> configMap = describeConfigsResult.all().get();
            Map<String,List<Map<String,Object>>> topicConfigs = new HashMap<>();
            configMap.forEach(new BiConsumer<ConfigResource, Config>() {
                @Override
                public void accept(ConfigResource configResource, Config config) {
//                                 topicName
//                    configResource.name()
                    List<Map<String,Object>> configs = new ArrayList<>();
                    for(ConfigEntry configEntry:config.entries()){
                        Map<String,Object> configMap = new HashMap<>();
                        configMap.put("name",configEntry.name());
                        configMap.put("default",configEntry.isDefault());
                        configMap.put("value",configEntry.value());
                        configMap.put("sensitive",configEntry.isSensitive());
                        configMap.put("readOnly",configEntry.isReadOnly());
                        configMap.put("source",configEntry.source().name());
                        configs.add(configMap);
                    }
                    topicConfigs.put(configResource.name(),configs);
                }
            });
            return topicConfigs;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

    }
}
