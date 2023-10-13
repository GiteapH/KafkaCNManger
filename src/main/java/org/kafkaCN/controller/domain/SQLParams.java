package org.kafkaCN.controller.domain;

import lombok.Data;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: SQL 参数
 * @date 2023/10/10 11:08
 */
@Data

public class SQLParams {

    String sql;
    String[] keys;
    String table;

    PollParams pollParams;
}
