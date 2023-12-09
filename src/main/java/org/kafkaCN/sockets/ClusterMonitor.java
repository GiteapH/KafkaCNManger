package org.kafkaCN.sockets;

import com.alibaba.fastjson.JSONObject;
import org.kafkaCN.controller.ConsumerController;
import org.kafkaCN.controller.ProducerController;
import org.kafkaCN.service.KafkaAdminService;
import org.kafkaCN.service.WSService;
import org.kafkaCN.utils.JMXMBean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: Cluster监控
 * @date 2023/10/14 17:33
 */

@Component
@EnableScheduling
public class ClusterMonitor {
    @Resource
    WSService wsService;

    JMXMBean jmxmBean = new JMXMBean();

    public static void main(String[] args) {
        JMXMBean jmxmBean = new JMXMBean();

          try {
              jmxmBean.create("localhost",9988);
              MBeanServerConnection mBeanServerConnection = JMXMBean.mBeanServerConnectionMap.get("localhost" + 9988).getMBeanServerConnection();
              ClusterMonitor clusterMonitor = new ClusterMonitor();
              String clusterMonitorString = clusterMonitor.createClusterMonitorString(mBeanServerConnection);
              System.err.println(clusterMonitorString);
//              System.err.println(mBeanServerConnection.getMBeanInfo(new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=test")).getAttributes()[0]);
          } catch (Exception e) {
              e.printStackTrace();
          }


    }


    private Map<String, Object> createOsMonitorString(MBeanServerConnection connection) throws Exception {
        return jmxmBean.getCommenAttribute(connection, "java.lang:type=OperatingSystem");
    }

    private String createTopicMonitorString(MBeanServerConnection connection) throws ReflectionException, MalformedObjectNameException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException {
        String[] names = {"BytesOutPerSec","BytesInPerSec","BytesRejectedPerSec","MessagesInPerSec","FailedFetchRequestsPerSec","FailedProduceRequestsPerSec"};
        String[] keys = {"Topic消息出站速率（Byte）","Topic消息入站速率（Byte）","Topic请求被拒速率","Topic消息入站速率（message）","Topic失败拉去请求速率","Topic发送请求失败速率"};
        Map<String,Object> map = new HashMap<>();
        for(int i=0;i < names.length; i++){
            Map<String,Object> attrs = null;
            try {
                attrs = jmxmBean.getTopicAttribute(connection, names[i], ConsumerController.currentTopic);
                map.put(keys[i],attrs);
            } catch (Exception ignored) {
                System.err.println(ignored.getMessage());
            }

        }
        JSONObject jsonObject = new JSONObject(map);
        return jsonObject.toJSONString();
    }

    private String createClusterMonitorString(MBeanServerConnection connection) throws Exception {
        String[][] ons = {
                {"log","LogFlushStats","LogFlushRateAndTimeMs"},
                {"server","ReplicaManager","UnderReplicatedPartitions"},
                {"server","BrokerTopicMetrics","MessagesInPerSec"},
                {"server","BrokerTopicMetrics","BytesOutPerSec"},
                {"server","ReplicaManager","LeaderCount"},
                {"controller","KafkaController","OfflinePartitionsCount"},
                {"controller","ControllerStats","LeaderElectionRateAndTimeMs"},
                {"controller","ControllerStats","UncleanLeaderElectionsPerSec"},
                {"controller","KafkaController","ActiveControllerCount"},
                {"network","SocketServer","NetworkProcessorAvgIdlePercent"},
                {"server","BrokerTopicMetrics","FailedFetchRequestsPerSec"},
                {"server","ReplicaManager","IsrShrinksPerSec"}};
        String[] keys = {"Log flush rate and time","同步失效的副本数","消息入站速率（消息数）","消息出站速率（Byte）",
                        "Leader副本数","下线Partition数量","Leader选举比率","Unclean Leader选举比率","Controller存活数量",
                        "Broker I/O工作处理线程空闲率","失败拉去请求速率","ISR变化速率"};
        Map<String,Object> map = new HashMap<>();
        for(int i=0;i < ons.length; i++){
            Map<String,Object> attrs = null;
            try {
                attrs = jmxmBean.getBrokerAttribute(connection,ons[i][0],ons[i][1],ons[i][2]);
                map.put(keys[i],attrs);
            } catch (Exception ignored) {
                System.err.println(ignored.getMessage());
            }

        }
        Map<String, Object> osMonitorString = createOsMonitorString(connection);

        map.put("系统参数",osMonitorString);
        JSONObject jsonObject = new JSONObject(map);

        return jsonObject.toJSONString();
    }

    @Scheduled(fixedRate = 6000)
    public void run(){
        System.err.println("init");
        JMXMBean.mBeanServerConnectionMap.forEach(new BiConsumer<String, JMXConnector>() {
            @Override
            public void accept(String key, JMXConnector jmxConnector) {
                try {
                    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
                    String topicMonitorString = createTopicMonitorString(mBeanServerConnection);
                    String clusterMonitorString = createClusterMonitorString(mBeanServerConnection);
                    System.err.println(topicMonitorString);
                    if(!"".equals(ConsumerController.currentTopic))
                        wsService.sendMes(key+"?topic",topicMonitorString);
                    wsService.sendMes(key+"?cluster",clusterMonitorString);
                } catch (IOException | MalformedObjectNameException | AttributeNotFoundException |
                         InstanceNotFoundException | ReflectionException | MBeanException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
