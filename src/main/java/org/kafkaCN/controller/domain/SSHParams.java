package org.kafkaCN.controller.domain;

import io.swagger.annotations.ApiParam;
import lombok.Data;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: SSH转发的参数
 * @date 2023/10/5 10:35
 */
@Data
public class SSHParams {
    @ApiParam("用户名")
    String  SSHUsername;

    @ApiParam("ip")
    String SSHIP;

    @ApiParam("端口号")
    Integer SSHPort;

    @ApiParam("密码")
    String SSHPassword;

    @ApiParam("私钥")
    String PrivateKey;

    @ApiParam("通行密语")
    String PassPhase;

}
