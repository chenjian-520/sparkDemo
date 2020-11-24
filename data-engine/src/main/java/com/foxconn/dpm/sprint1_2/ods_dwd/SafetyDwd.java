package com.foxconn.dpm.sprint1_2.ods_dwd;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple11;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;


/**
 * @author cj
 * @version 1.0
 * @timestamp 2020/1/10
 * 安全事故统计
 * 抽取 dpm_ods_safety_detail 安全事故表 , 放入dwd表 dpm_dwd_safety_detail 安全事故表
 */
public class SafetyDwd extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    /**
     * dpm_ods_safety_detail  安全事故
     * com.dl.spark.safety.SafetyDwd
     * site_code
     * case_no
     * accident_classification  事故類別
     * accident_date            事故發生時間
     * accident_location        事故發生地點
     * accident_owner           責任單位
     * accident_desc            事故描述
     * accident_status          事故的狀態
     * update_dt
     * update_by
     * data_from
     */
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        System.out.println("==============================>>>Programe Start<<<==============================");
        //        初始化环境
        String yesterday3Stamp = batchGetter.getStDateDayStampAdd(-3);
        String tomoreStamp = batchGetter.getStDateDayStampAdd(1);
        //初始化时间
        Date today = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");
        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = simpleDateFormat.format(today);
        String yesterdayOut = simpleDateFormat1.format(today);
        System.out.println(yesterday + "--" + yesterdayOut);
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(yesterday3Stamp));
        scan.withStopRow(Bytes.toBytes(tomoreStamp));

        JavaRDD<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> safetyRdd = DPHbase.rddRead("dpm_ods_safety_detail", scan, true).map(r -> {
            return new Tuple11<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("case_no"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("accident_classification"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("accident_date"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("accident_location"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("accident_owner"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("accident_desc"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("accident_status"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("update_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("update_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_SAFETY_DETAIL"), Bytes.toBytes("data_from")))
            );
        }).filter(r -> r._4().equals(yesterday));

        safetyRdd.take(5).forEach(r -> System.out.println(r));
        JavaRDD<Put> putJavaRdd = safetyRdd.mapPartitions(r -> {
            ArrayList<Put> puts1 = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            while (r.hasNext()) {
                Tuple11<String, String, String, String, String, String, String, String, String, String, String> line = r.next();
                //update_dt+accident_classification+accident_date+accident_location
                String baseRowKey = sb.append(line._9()).append(":").append(yesterdayOut).append(":").append(line._4()).append(":").append(line._5()).toString();
                //dpm_dwd_safety_detail
                Put put2 = new Put(Bytes.toBytes(baseRowKey));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("site_code"), Bytes.toBytes(line._2()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_classification"), Bytes.toBytes(line._3()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_date"), Bytes.toBytes(yesterdayOut));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_location"), Bytes.toBytes(line._5()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_owner"), Bytes.toBytes(line._6()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_desc"), Bytes.toBytes(line._7()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_status"), Bytes.toBytes(line._8()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("update_dt"), Bytes.toBytes(line._9()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("update_by"), Bytes.toBytes(line._10()));
                put2.addColumn(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("data_from"), Bytes.toBytes(line._11()));
                puts1.add(put2);
                sb.delete(0, sb.length());
            }
            return puts1.iterator();
        });
        putJavaRdd.take(5).forEach(r -> System.out.println(r));
        //  safety 写入Dwd
        DPHbase.rddWrite("dpm_dwd_safety_detail", putJavaRdd);
        System.out.println("==============================>>>Programe End<<<==============================");
        DPSparkApp.stop();
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
