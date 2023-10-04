package org.kafkaCN.common;

public class Constant {


    public String getPath(){
        return this.getClass().getClassLoader().getResource("").getPath();
    }

}
