package com.foxconn.dpm.util.data_quality_alert;

import com.google.gson.Gson;
import com.squareup.okhttp.*;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.io.IOException;
import java.util.List;

/**
 * @author HS
 * @className EmailWarning
 * @description TODO
 * @date 2020/6/15 13:16
 */
public class EmailWarning {
    private static OkHttpClient okHttpClient = new OkHttpClient();
    private static Gson gson = new Gson();

    public static void main(String[] args) throws IOException {
        //postDataEmailOkhttp("1164935104@qq.com", "TEST_DATA", "TEST_EMAIL");


    }

    public static void postRowStructContent(JavaRDD<Row> data, List<StructField> fields) {
        for (StructField field : fields) {

        }





    }

    public static void postDataEmailOkhttp(String to, String subject, String text) throws IOException {
        RequestBody postBody = new FormEncodingBuilder()
                .add("to", to)
                .add("text", subject)
                .add("subject", text)
                .build();
        Request build = preparePublicHeader(new Request.Builder())
                .url("http://10.60.136.153:8090/sendmail")
                .post(postBody)
                .build();
        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);
    }

    public static void postDataEmailJDK(String to, String subject, String text) throws IOException {
        try {
            String postURL = "http://10.60.136.153:8090/sendmail";
            PostMethod postMethod = null;
            postMethod = new PostMethod(postURL);

            postMethod.setRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
            NameValuePair[] data = {
                    new NameValuePair("to", to),
                    new NameValuePair("text", subject),
                    new NameValuePair("subject", text)
            };
            postMethod.setRequestBody(data);

            org.apache.commons.httpclient.HttpClient httpClient = new org.apache.commons.httpclient.HttpClient();
            int response = httpClient.executeMethod(postMethod);
            String result = postMethod.getResponseBodyAsString();
            System.out.println(response);
            System.out.println(result);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private static Request.Builder preparePublicHeader(Request.Builder builder) {
        return builder
                .header("Content-Type", "application/x-www-form-urlencoded");
    }


}
