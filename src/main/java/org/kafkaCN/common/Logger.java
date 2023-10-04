package org.kafkaCN.common;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: TODO
 * @date 2023/10/4 11:28
 */

public class Logger {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Logger.class);
    public static org.apache.log4j.Logger getInstance(){
        return logger;
    }

}
