package org.kafkaCN.controller.domain;

import io.swagger.annotations.ApiParam;
import lombok.Data;

import java.time.Duration;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 拉取配置
 * @date 2023/10/8 12:48
 */

@Data
public class PollParams {

    @ApiParam("拉取类型")
    Integer pollType;

    @ApiParam("手动偏移量")
    Long offset;

    @ApiParam("监听时长:s")
    long duration;

}
