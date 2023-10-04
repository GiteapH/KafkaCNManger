package org.kafkaCN.controller.domain;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import lombok.Data;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 生产者发送参数
 * @date 2023/10/3 14:22
 */
@Data
public class ProducerSendParams {
    @ApiParam(value = "主题")
    String topic;

    @ApiParam(value = "分区数")
    Integer partitions;

    @ApiParam(value = "分区关键字")
    String key;

    @ApiParam(value = "发送时间戳")
    Long timestamp;

    @ApiParam(value = "数据")
    String data;



    public ProducerRecord<String,String> getProducerRecord(){
        return new ProducerRecord<>(topic,partitions,timestamp,key,data);
    }
}
