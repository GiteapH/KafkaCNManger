package org.kafkaCN.Exception;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 创建关于存在的连接异常
 * @date 2023/10/7 11:14
 */
public class ContainException extends Exception{
    public ContainException(String msg){
        super(msg);
    }

    public ContainException(){
        super("连接已存在");
    }
}
