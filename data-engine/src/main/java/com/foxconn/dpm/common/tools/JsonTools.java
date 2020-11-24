package com.foxconn.dpm.common.tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.foxconn.dpm.util.sql.SqlGetter;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @Author HY
 * @Date 2020/6/29 4:00
 * @Description 读取配置json文件
 */
public class JsonTools {
    private JsonTools(){}
    private static final Charset CHARSET = Charset.defaultCharset();

    public static <T> T readJsonObject(String path, Class<T> classType) {
        String jsonStr = readFile(path);
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        return JSON.parseObject(jsonObject.toString(), classType);
    }

    public static <T> List<T> readJsonArray(String path, Class<T> classType)  {
        String jsonStr = readFile(path);
        JSONArray jsonArray = JSON.parseArray(jsonStr);
        return JSON.parseArray(jsonArray.toString(), classType);
    }


    private static String readFile(String file) {
        URL resource = SqlGetter.class.getClassLoader().getResource(file);
        if (resource == null) {
            throw new RuntimeException("无法获取"+file+"配置文件！");
        }

        StringBuilder sb = new StringBuilder();
        try (InputStreamReader isr = new InputStreamReader(resource.openStream(), CHARSET); BufferedReader br = new BufferedReader(isr) ) {

            while (br.ready()) {
                sb.append(br.readLine()).append("\r\n");
            }

        } catch (IOException e) {
            throw new RuntimeException("读取配置文件"+file+"异常：" + e.getMessage());
        }

        return sb.toString();
    }

}
