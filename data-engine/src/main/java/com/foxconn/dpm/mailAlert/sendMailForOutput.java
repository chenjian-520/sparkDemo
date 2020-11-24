package com.foxconn.dpm.mailAlert;

import com.foxconn.dpm.sprint1_2.dwd_dws.FtyDwdToDwsSix;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple6;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
* dpm_dwd_personnel_emp_workhours 人力类型为空的邮件报警功能
*
* */
public class sendMailForOutput extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //业务逻辑
        sendMailForManType();
        //释放资源
        DPSparkApp.stop();
    }

    public void sendMailForManType() throws Exception {
        String startTime = batchGetter.getStDateDayStampAdd(-1);
        String endTime = batchGetter.getStDateDayStampAdd(0);
        //邮件内容
        String textStrByL5 = getRdd(startTime, endTime, "WH", "L5");
        if(textStrByL5 != null){
            System.out.println("========================WH-L5-start===========================");
            String mailObjectListByL5 = "1091064910@qq.com;anne.xw.liu@mail.foxconn.com";
            sendAlarmEmail("http://10.124.160.10:8090/sendmail",mailObjectListByL5,"约当产量数据校验",textStrByL5);
            System.out.println("========================WH-L5-end===========================");
        }

        String textStrByL6 = getRdd(startTime, endTime, "WH", "L6");
        if(textStrByL6 != null){
            System.out.println("========================WH-L6-start===========================");
            String mailObjectListByL6 = "1091064910@qq.com;anne.xw.liu@mail.foxconn.com";
            sendAlarmEmail("http://10.124.160.10:8090/sendmail",mailObjectListByL6,"约当产量数据校验",textStrByL6);
            System.out.println("========================WH-L6-end===========================");
        }

        String textStrByL10 = getRdd(startTime, endTime, "WH", "L10");
        if(textStrByL10 != null){
            System.out.println("========================WH-L10-start===========================");
            String mailObjectListByL10 = "1091064910@qq.com;anne.xw.liu@mail.foxconn.com";
            sendAlarmEmail("http://10.124.160.10:8090/sendmail",mailObjectListByL10,"约当产量数据校验",textStrByL10);
            System.out.println("========================WH-L10-end===========================");
        }

    }

    public String getRdd(String startTime,String endTime,String site,String level) throws Exception {
        JavaRDD<Tuple6<String, String, String, String, String, String>> textRdd = DPHbase.saltRddRead("dpm_dws_production_output_dd", startTime, endTime, new Scan(), true).filter(r ->
                "0.0".equals(FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("normalized_output_qty")))).toString())  &&
                        site.equals(FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("site_code")))))  &&
                        level.equals(FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("level_code")))))
        ).map(r -> {
            return new Tuple6<>(
                    FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("site_code")))),
                    FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("level_code")))),
                    FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("line_code")))),
                    FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("part_no")))),
                    FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("work_dt")))),
                    FtyDwdToDwsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("normalized_output_qty"))))
            );
        });
        List<Tuple6<String, String, String, String, String, String>> textList = textRdd.collect();
        StringBuffer textStr = new StringBuffer();
        //设置一个邮件发送的最大数，防止发送邮箱数据过多
        int index = 200;
        if(textList.size() < index){
            index = textList.size();
        }
        System.out.println("异常数据count:"+textList.size());
        for(Tuple6<String, String, String, String, String, String> tuple6 : textList.subList(0,index)){
            textStr= textStr.append("<br>site_code:"+tuple6._1().toString()+" "+
                    "level_code:"+tuple6._2().toString()+" "+
                    "line_code:"+tuple6._3().toString()+" "+
                    "part_no:"+tuple6._4().toString()+" "+
                    "work_dt:"+tuple6._5().toString()+" "+
                    "normalized_output_qty:"+tuple6._6().toString()+"<br>");
        }
        if(index == 0){
            return null;
        }
        return "异常数据count:"+index+textStr.toString()+"请相关人员注意维护";
    }

    //发送邮件得方法
    public static  void sendAlarmEmail(String emailApi,String toEmail,String subject,String text) throws Exception {
        HttpPost httpPost = new HttpPost(emailApi);
        CloseableHttpClient httpClient = HttpClients.createDefault();

        List<BasicNameValuePair> paramsList = new ArrayList<BasicNameValuePair>();
        paramsList.add(new BasicNameValuePair("to",toEmail));
        paramsList.add(new BasicNameValuePair("subject",subject));
        paramsList.add(new BasicNameValuePair("text",text));

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(paramsList, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        String conResult = "";
        HttpResponse response = httpClient.execute(httpPost);
        if (response.getStatusLine().getStatusCode() == 200) {
            // 读取返回数据
            conResult = (EntityUtils.toString(response.getEntity()));
        } else {
            throw  new Exception("邮件发送失败");
        }
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
