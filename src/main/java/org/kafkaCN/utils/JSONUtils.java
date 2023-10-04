package org.kafkaCN.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: JSON工具类
 * @date 2023/10/4 14:20
 */
public class JSONUtils {
    private static final boolean isTab = true;
    public static JSONObject isValid(String s){
        try {
            JSONObject jsonObject = JSONObject.parseObject(s);
            return jsonObject;
        }catch (Exception e) {
            return null;
        }
    }

    public static File saveFile(JSONArray jsonArray,String path,String ...keys) {
        System.err.println(Arrays.toString(keys));
        //        滤除不存在主要的关键字的对象
        JSONArray array = new JSONArray();
        for(int i=0;i<jsonArray.size(); i++){
            JSONObject json = (JSONObject) jsonArray.get(i);
//            判断对象中是否有key
            boolean f = true;
            for(String key:keys){
//                不存在必须的key
                if(json.getString(key)==null){
                    f = false;
                    break;
                }
            }
//            满足key都存在
            if(f){
                array.add(json);
            }
        }
//        清空源数据
        jsonArray.clear();
        jsonArray = array;
        File file = new File(path);
        try {
            if (!file.getParentFile().exists()) { // 如果父目录不存在，创建父目录
                file.getParentFile().mkdirs();
            }
            if (!file.exists())//判断文件是否存在，若不存在则新建
            {
                file.createNewFile();
            }
            FileOutputStream fileOutputStream = new FileOutputStream(file);//实例化FileOutputStream
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8);//将字符流转换为字节流
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);//创建字符缓冲输出流对象

            String jsonString = jsonArray.toString();//将jsonarray数组转化为字符串
//            String JsonString = stringToJSON(jsonString);//将jsonarrray字符串格式化
            bufferedWriter.write(jsonString);//将格式化的jsonarray字符串写入文件
            bufferedWriter.flush();//清空缓冲区，强制输出数据
            bufferedWriter.close();//关闭输出流
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return file;
    }

    public static String stringToJSON(String strJson) {
        int tabNum = 0;
        StringBuffer jsonFormat = new StringBuffer();
        int length = strJson.length();
        for (int i = 0; i < length; i++) {
            char c = strJson.charAt(i);
            if (c == '{') {
                tabNum++;
                jsonFormat.append(c).append("\n");
                jsonFormat.append(getSpaceOrTab(tabNum));
            } else if (c == '}') {
                tabNum--;
                jsonFormat.append("\n");
                jsonFormat.append(getSpaceOrTab(tabNum));
                jsonFormat.append(c);
            } else if (c == ',') {
                jsonFormat.append(c).append("\n");
                jsonFormat.append(getSpaceOrTab(tabNum));
            } else {
                jsonFormat.append(c);
            }
        }
        return jsonFormat.toString();
    }
    private static String getSpaceOrTab(int tabNum) {
        StringBuffer sbTab = new StringBuffer();
        for (int i = 0; i < tabNum; i++) {
            if (isTab) {
                sbTab.append('\t');
            } else {
                sbTab.append("    ");
            }
        }
        return sbTab.toString();
    }
}
