package org.kafkaCN.controller;

import com.jcraft.jsch.*;
import io.swagger.annotations.Api;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.spark.sql.execution.columnar.MAP;
import org.kafkaCN.Exception.ContainException;
import org.kafkaCN.common.Result;
import org.kafkaCN.controller.domain.ProducerSendParams;
import org.kafkaCN.controller.domain.SSHParams;
import org.kafkaCN.controller.domain.ShutDownParams;
import org.kafkaCN.service.KafkaAdminService;
import org.kafkaCN.service.KafkaConsumerService;
import org.kafkaCN.service.KafkaProducerService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: kafka生产者连接
 * @date 2023/10/3 11:54
 */
@RestController
@Api(tags = "kafka生产者连接")
@CrossOrigin
@RequestMapping("/producer")
public class ProducerController {

    @Resource
    KafkaAdminService kafkaAdminService;
    @Resource
    KafkaProducerService kafkaProducerService;

    @Resource
    KafkaConsumerService kafkaConsumerService;
    private final Map<String,KafkaTemplate<String,String>> kafkaTemplates = new ConcurrentHashMap<>();

    @PostMapping("/addConnection/{host}/{port}")
    public Result addConnection(@PathVariable(value = "host") String host,@PathVariable(value = "port") Integer port){
        try {
            String addr = host+":"+port;
            if(kafkaTemplates.containsKey(addr)){
                return Result.error("连接已存在");
            }

            KafkaTemplate<String, String> tKafkaTemplate = kafkaProducerService.kafkaTemplate(addr);
            Map<String, List<PartitionInfo>> topics = kafkaConsumerService.checkContainer(addr);
            if(topics != null) {
                kafkaTemplates.put(addr, tKafkaTemplate);
                Map<String,String> map = new HashMap<>();
                map.put("proxy",addr);
                map.put("connectionSize", String.valueOf(kafkaTemplates.size()));
                //            创建管理端连接
                try {
                    kafkaAdminService.create(host, port);
                    map.put("admin","管理端连接创建成功。");
                }catch (ContainException ex){
                    ex.printStackTrace();
                    map.put("admin","管理端连接创建失败，可能会影响一些功能使用。");
                    return Result.error(map);
                }
                return Result.success(map);
            }
            else
                return Result.error("403","连接失败...");
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
    }

    @PostMapping("/addConnectionSSH/{host}/{port}")
    public Result addConnectionSSH(@PathVariable(value = "host") String host,@PathVariable(value = "port") Integer port, @RequestBody SSHParams sshparams){
//        创建ssh转发
        JSch jSch = new JSch();
        int localPort;
        try {
            Session session = jSch.getSession(sshparams.getSSHUsername(),sshparams.getSSHIP(),sshparams.getSSHPort());
            session.setPassword(sshparams.getSSHPassword());
            session.setConfig("StrictHostKeyChecking", "no");
//            建立SSH连接
            try {
                session.connect();
//            设置SSH隧道转发
//            绑定随机空闲端口
                localPort = session.setPortForwardingL(0, host, port);
            }catch (Exception e) {
//                ssh连接失败
                return Result.error("ssh转发失败,请检擦ssh账号密码是否正确");
            }
            String addr = "localhost:" + localPort;
            KafkaTemplate<String, String> tKafkaTemplate = kafkaProducerService.kafkaTemplate(addr);
            Map<String, List<PartitionInfo>> topics = kafkaConsumerService.checkContainer(addr);
            if(topics != null) {
                kafkaTemplates.put(addr, tKafkaTemplate);
//                返回服务器代理端口号
                Map<String,Object> map = new HashMap<>();
                map.put("proxy",localPort);
                map.put("connectionSize",kafkaTemplates.size());
                //            创建管理端连接
                try {
                    kafkaAdminService.create("localhost", port);
                    map.put("admin","管理端连接创建成功。");
                }catch (ContainException ex){
                    map.put("admin","管理端连接创建失败，可能会影响一些功能使用。");
                    ex.printStackTrace();
                    return Result.error(map);
                }
                return Result.success(map);
            }
            else
                return Result.error("403","连接失败...");
        } catch (JSchException e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
    }



    @PostMapping("/sendMessage")
    public Result sendMessage( @RequestBody ProducerSendParams producerSendParams){
        String addr = (producerSendParams.getHost()==null?"localhost":producerSendParams.getHost())+":"+producerSendParams.getPort();
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplates.getOrDefault(addr, null);
        if(kafkaTemplate==null){
            return Result.error("不存在"+addr + "的连接");
        }
        try {
            if(!producerSendParams.getSync()) {
                SendResult<String, String> SendResult = kafkaTemplate.send(producerSendParams.getProducerRecord()).get();
                ProducerRecord<String, String> producerRecord = SendResult.getProducerRecord();
                Map<String, String> map = new HashMap<>();
                map.put("sendData", producerRecord.value());
                map.put("partition", String.valueOf(producerRecord.partition()));
                map.put("res",producerRecord.key());
                map.put("topic", producerRecord.topic());
                return Result.success(map);
            }else{
                ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(producerSendParams.getProducerRecord());
                send.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                    @Override
                    public void onFailure(Throwable throwable) {

                    }

                    @Override
                    public void onSuccess(SendResult<String, String> stringStringSendResult) {

                    }
                });
                return Result.success("异步发送成功");
            }
//            发送回调
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
    }

    @PostMapping("/shutdown")
    public Result shutdown(@RequestBody ShutDownParams[] shutDownParamsa){
        Map<String,String> map = new HashMap<>();
        for(ShutDownParams shutDownParams:shutDownParamsa){
            String addr = (shutDownParams.getHost()==null?"localhost":shutDownParams.getHost())+":"+shutDownParams.getPort();
            if(kafkaTemplates.containsKey(addr)){
                kafkaTemplates.remove(addr);
                map.put(addr,"success");
            }else{
                map.put(addr,"error");
            }
        }
        return Result.success(map);
    }

//    public static void main(String[] args) throws JSchException, IOException {
//        JSch jSch = new JSch();
//        Session session = jSch.getSession("Administrator","47.107.41.17",22);
//        session.setPassword("ppnn13%dkjrFeb.1");
//        session.setConfig("StrictHostKeyChecking", "no");
////            建立SSH连接
//        session.connect();
//
//
//        Channel channel = session.openChannel("exec");
//        ((ChannelExec) channel).setCommand("ipconfig");
//        channel.setInputStream(null);
//        ((ChannelExec) channel).setErrStream(System.err);
//
//        InputStream in = channel.getInputStream(); //这一部分都是官方的固定写法
//        channel.connect();
//        try{
//            // 加GBK，解决中文乱码
//            BufferedReader inputReader = new BufferedReader(new InputStreamReader(in,"GBK"));
//            String inputLine = null;
//            while((inputLine = inputReader.readLine()) != null) {
//                System.out.println(inputLine);
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        channel.disconnect();
//        session.disconnect();
//    }
//
    @GetMapping("/metrics")
    public Result metrics(@RequestParam(required = false) String host,@RequestParam Integer port){
        String addr = (host==null?"localhost":host)+":"+port;
        KafkaTemplate<String, String> template = kafkaTemplates.getOrDefault(addr, null);
        if(template == null)return Result.error(addr+""+"未连接");
        Map<MetricName, ? extends Metric> metrics =template.metrics();
        Map<String,Map<String,String>> metricsRet = new HashMap<>();
        metrics.forEach(new BiConsumer<MetricName, Metric>() {
            @Override
            public void accept(MetricName metricName, Metric metric) {
                Map<String,String> tips = new HashMap<>();
                tips.put("description",metricName.description());
                tips.put("group",metricName.group());
                tips.put("value",metric.metricValue().toString());
                metricsRet.put(metricName.name(),tips);
            }
        });
        return Result.success(metricsRet);
    }
}
