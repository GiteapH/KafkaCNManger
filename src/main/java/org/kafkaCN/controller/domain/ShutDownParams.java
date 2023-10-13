package org.kafkaCN.controller.domain;

import io.swagger.annotations.ApiParam;
import lombok.Data;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 停止连接
 * @date 2023/10/5 14:48
 */
@Data
public class ShutDownParams {
    @ApiParam(value = "ip")
    String host;

    @ApiParam(value = "端口号")
    Integer port;

    String groupId;

    String topic;
}
