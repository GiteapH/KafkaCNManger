package org.kafkaCN.utils;

import org.glassfish.jersey.server.ContainerException;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.kafkaCN.Exception.ContainException;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 获取jmx监控指标
 * @date 2023/10/15 10:48
 */
public class JMXMBean {
    private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    public static Map<String, JMXConnector> mBeanServerConnectionMap = new ConcurrentHashMap<>();

    public static void create(String host,Integer port) throws IOException, ContainException {
//        查看jmx连接是否已存在
        if(mBeanServerConnectionMap.containsKey(host+port)){
            throw new ContainException("jmx连接已存在");
        }
        JMXServiceURL jmxServiceURL ;
        JMXConnector connect = null;
        try {
            jmxServiceURL = new JMXServiceURL(String.format(JMX_URL, host, port));
            connect = JMXConnectorFactory.connect(jmxServiceURL);
            mBeanServerConnectionMap.put( host+port,connect);
        } catch (IOException e) {
            throw e;
        }
    }

    public MBeanServerConnection getMBeanServerCon(String host,Integer port) throws IOException {
        if(!checkContain(host, port)){
            throw new ContainerException(host + port+"jmx连接不存在");
        }
        return mBeanServerConnectionMap.get(host+port).getMBeanServerConnection();
    }

    public  Map<String,Object> getCommenAttribute(MBeanServerConnection mbeanServerCon,String objectName) throws Exception {
        try {
            MBeanAttributeInfo[] attributes = mbeanServerCon.getMBeanInfo(new ObjectName(objectName)).getAttributes();
            String[] attrs = new String[attributes.length];
            for(int i = 0; i < attributes.length; i++){
                attrs[i] = attributes[i].getName();
            }
            Map<String,Object> RET = new HashMap<>();
            AttributeList attributesList = mbeanServerCon.getAttributes(new ObjectName(objectName), attrs);
            attributesList.forEach(new Consumer<Object>() {
                @Override
                public void accept(Object o) {
                    RET.put(((Attribute) o).getName(),((Attribute) o).getValue());
                }
            });
            return RET;
        }  catch (MalformedObjectNameException | ReflectionException  |
                  InstanceNotFoundException  | IOException |IntrospectionException ignored) {
            throw new Exception(objectName);
        }
    }

    public Map<String,Object> getBrokerAttribute(MBeanServerConnection mbeanServerCon,String pkg,String type,String name) throws Exception {
        String objectName = String.format("kafka.%s:type=%s,name=%s", pkg,type,name);
        try {
            MBeanAttributeInfo[] attributes = mbeanServerCon.getMBeanInfo(new ObjectName(objectName)).getAttributes();
            String[] attrs = new String[attributes.length];
            for(int i = 0; i < attributes.length; i++){
                attrs[i] = attributes[i].getName();
            }
            Map<String,Object> RET = new HashMap<>();
            AttributeList attributesList = mbeanServerCon.getAttributes(new ObjectName(objectName), attrs);
            attributesList.forEach(new Consumer<Object>() {
                @Override
                public void accept(Object o) {
                    RET.put(((Attribute) o).getName(),((Attribute) o).getValue());
                }
            });
            return RET;
        }  catch (MalformedObjectNameException | ReflectionException  |
                  InstanceNotFoundException  | IOException ignored) {
            throw new Exception(objectName);
        }
    }

    public Object getBrokerAttribute(String host,Integer port,String pkg,String type,String name,String attributeName) throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException {
        if(!checkContain(host, port)){
            throw new ContainerException(host + port+"jmx连接不存在");
        }
        String objectName = String.format("kafka.%s:type=%s,name=%s", pkg,type,name);
        return mBeanServerConnectionMap.get(host+port).getMBeanServerConnection().getAttribute(new ObjectName(objectName),attributeName);
    }

    public Map<String,Object> getTopicAttribute(MBeanServerConnection mbeanServerCon,String name,String topic) throws Exception {
        String objectName = String.format("kafka.server:type=BrokerTopicMetrics,name=%s,topic=%s", name,topic);
        try {
//            return mbeanServerCon.getAttribute(new ObjectName(objectName), attributeName);
            MBeanAttributeInfo[] attributes = mbeanServerCon.getMBeanInfo(new ObjectName(objectName)).getAttributes();
            String[] attrs = new String[attributes.length];
            for(int i = 0; i < attributes.length; i++){
                attrs[i] = attributes[i].getName();
            }
            Map<String,Object> RET = new HashMap<>();
            AttributeList attributesList = mbeanServerCon.getAttributes(new ObjectName(objectName), attrs);
            attributesList.forEach(new Consumer<Object>() {
                @Override
                public void accept(Object o) {
                    RET.put(((Attribute) o).getName(),((Attribute) o).getValue());
                }
            });
            return RET;
        }  catch (MalformedObjectNameException | ReflectionException  |
                  InstanceNotFoundException  | IOException ignored) {
            throw new Exception(objectName);
        }
    }

    public Object getTopicAttribute(String host,Integer port,String name,String topic,String attributeName) throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException {
        if(!checkContain(host, port)){
            throw new ContainerException(host + port+"jmx连接不存在");
        }
        String objectName = String.format("kafka.server:type=BrokerTopicMetrics,name=%s,topic=%s", name,topic);
        return mBeanServerConnectionMap.get(host+port).getMBeanServerConnection().getAttribute(new ObjectName(objectName),attributeName);
    }

    public void close(String host,Integer port){
        if(!checkContain(host, port)){
            throw new ContainerException(host + port+"jmx连接不存在");
        }
        try {
            mBeanServerConnectionMap.get(host+port).close();
        } catch (IOException ignored) {
        }finally {
            mBeanServerConnectionMap.remove(host+port);
        }
    }

    private boolean checkContain(String host,Integer port){
        return mBeanServerConnectionMap.containsKey(host + port);
    }
}
