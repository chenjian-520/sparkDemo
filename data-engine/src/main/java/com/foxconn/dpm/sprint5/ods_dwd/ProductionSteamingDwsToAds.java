package com.foxconn.dpm.sprint5.ods_dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.foxconn.dpm.sprint5.ods_dwd.bean.DWSProductionOutputStreaming;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 */
//  造kafka测试数据 post接口  ：  http://10.60.136.156:8089/api/dlapiservice/v1/kafka
//         {
//        "topic": "chen_topic",
//        "msg": "{\"dpHbasecolumns\":[{\"columnName\":\"datajson\",\"family\":\"DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE_BAK\",\"value\":\"{\\\"site_code\\\":\\\"WH\\\",\\\"level_code\\\":\\\"L11\\\",\\\"factory_code\\\":\\\"DT1\\\",\\\"process_code\\\":\\\"Assy\\\",\\\"area_code\\\":\\\"11\\\",\\\"line_code\\\":\\\"LINE_A\\\",\\\"work_shift\\\":\\\"D\\\",\\\"work_dt\\\":\\\"2020-07-06\\\",\\\"wo\\\":\\\"111\\\",\\\"workorder_type\\\":\\\"1111\\\",\\\"part_no\\\":\\\"51452sd\\\",\\\"customer\\\":\\\"HP\\\",\\\"model_no\\\":\\\"11111\\\",\\\"sn\\\":\\\"11111\\\",\\\"station_code\\\":\\\"text\\\",\\\"station_name\\\":\\\"text\\\",\\\"is_pass\\\":\\\"1\\\",\\\"is_fail\\\":\\\"1\\\",\\\"scan_by\\\":\\\"cj\\\",\\\"scan_dt\\\":\\\"1594026130000\\\",\\\"update_dt\\\":\\\"1594026130000\\\",\\\"update_by\\\":\\\"sfc\\\",\\\"data_from\\\":\\\"cj\\\"}\"}],\"rowKey\":\"03:1575525600500:DAX1:D624:2a6ae81e-4738-4801-87c6-fcfeb89bdbb0\"}"
//        }

//    运行参数：测试环境
//    {\"dprunclass\":\"com.foxconn.dpm.sprint5.ods_dwd.ProductionSteaming\",\"dpappName\":\"streaming-dwd\",\"dpuserid\":\"8F70E561-ED63-4B88-A343-UIDE89FF9156\",\"dpmaster\":\"local[*]\",\"pmClass\":\"com.tm.dl.javasdk.dpspark.common.ProdPermissionManager\",\"dpType\":\"streaming\",\"dpStreaming\":{\"topics\":\"chen_topic\",\"groupId\":\"chen\",\"batchDuration\":\"4\",\"windowDurationMultiple\":\"4\",\"sliverDurationMultiple\":\"4\"}}

 // host 环境QA     sparkSDK   http://10.60.136.145:806/jar/sparksdk/



@Slf4j
public class ProductionSteamingDwsToAds extends DPSparkBase {
    private static ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
    private static SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {
        //接收流式数据
        dpStreaming.startJob(rdd -> {
            //接收ods的数据
            //"{\"dpHbasecolumns\":[{\"columnName\":\"datajson\",\"family\":\"DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE_BAK\",\"value\":\"{\"site_code\":\"WH\",\"level_code\":\"L5\",\"factory_code\":\"DT1\",\"process_code\":\"Assy\",\"area_code\":\"11\",\"line_code\":\"LINE_A\",\"work_shift\":\"D\",\"work_dt\":\"2020-07-06\",\"wo\":\"111\",\"workorder_type\":\"1111\",\"part_no\":\"51452sd\",\"customer\":\"HP\",\"model_no\":\"11111\",\"sn\":\"11111\",\"station_code\":\"text\",\"station_name\":\"text\",\"is_pass\":\"1\",\"is_fail\":\"1\",\"scan_by\":\"cj\",\"scan_dt\":\"1594026130000\",\"update_dt\":\"1594026130000\",\"update_by\":\"sfc\",\"data_from:\"cj\"}}],\"rowKey\":\"03:1575525600500:DAX1:D624:2a6ae81e-4738-4801-87c6-fcfeb89bdbb0\"}"
            log.error("2222222222222222");
            try {
                JavaRDD<DWSProductionOutputStreaming> map1 = rdd.map(record -> {
                    JSONObject odsData = JSON.parseObject(record.value().toString());
                    DWSProductionOutputStreaming dwsProductionOutputStreaming = JSONObject.toJavaObject((JSON) JSON.parse(odsData.toString()), DWSProductionOutputStreaming.class);
                    return dwsProductionOutputStreaming;
                });
                map1.take(5).forEach(r-> System.out.println(r));
            } catch (Exception e) {
                DPSparkApp.doThrow(e);
                log.error("数据对象转换异常"+e.toString());
            }



        });

    }
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

    }
}
