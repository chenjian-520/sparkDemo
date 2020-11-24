package com.foxconn.dpm.sprint1_2.dwd_dws;

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
import scala.Tuple10;
import scala.Tuple3;
import scala.Tuple7;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className SafetyDwdToDws
 * @data 2019/12/20
 * <p>
 * 安全事故统计
 * 输入表： 抽取dwd表 dpm_dwd_safety_detail 安全事故表
 * 输出表：放入dws表 dpm_dws_safety_dd 安全事故表
 */
public class SafetyDwdToDws extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //com.dl.spark.safety.SafetyDwdToDws
        //        初始化环境
        String yesterday3Stamp = batchGetter.getStDateDayStampAdd(-3);
        String tomoreStamp = batchGetter.getStDateDayStampAdd(1);
        //初始化时间
        Date today = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = simpleDateFormat.format(today);
        System.out.println(yesterday);
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(yesterday3Stamp));
        scan.withStopRow(Bytes.toBytes(tomoreStamp));

        System.out.println("==============================>>>Programe Start<<<==============================");
        JavaRDD<Tuple10<String, String, String, String, String, String, String, Long, String, String>> safetyRdd = DPHbase.rddRead("dpm_dwd_safety_detail", scan, true).map(r -> {
            return new Tuple10<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_classification"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_date"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_location"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_owner"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_desc"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("accident_status"))),
                    Bytes.toLong(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("update_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("update_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_SAFETY_DETAIL"), Bytes.toBytes("data_from")))
            );
        }).filter(r -> r._3().equals(yesterday));
        JavaRDD<Tuple7<String, String, String, String, Long, String, String>> safetyDwsRdd = safetyRdd.groupBy(r -> {
            return new Tuple3<>(r._1(), r._3(), r._7());
        }).map(r -> {
            int sum = 0;
            Iterator<Tuple10<String, String, String, String, String, String, String, Long, String, String>> iterator = r._2.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                sum = sum + 1;
            }
            return new Tuple7<>(r._1._1(), r._1._2(), String.valueOf(sum), r._1._3(), 0L, "cj", "DWD");
        });
        safetyDwsRdd.take(5).forEach(r -> System.out.println(r));

        JavaRDD<Put> putJavaRdd = safetyDwsRdd.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            while (r.hasNext()) {
                Tuple7<String, String, String, String, Long, String, String> line = r.next();
                long timeMillis = System.currentTimeMillis();
                // update_dt + site_code + accident_day + accident_status
                String baseRowKey = sb.append(timeMillis).append(":").append(line._1()).append(":").append(line._2()).append(":").append(line._4()).toString();
                //dpm_dwd_safety_detail
                Put put = new Put(Bytes.toBytes(baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(line._1()));
                put.addColumn(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_day"), Bytes.toBytes(line._2()));
                put.addColumn(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_count"), Bytes.toBytes(line._3()));
                put.addColumn(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_status"), Bytes.toBytes(line._4()));
                put.addColumn(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(timeMillis)));
                put.addColumn(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("update_by"), Bytes.toBytes(line._6()));
                put.addColumn(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("data_from"), Bytes.toBytes(line._7()));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        putJavaRdd.take(5).forEach(r -> System.out.println(r));
        //  safety Dwd写入Dws
        DPHbase.rddWrite("dpm_dws_safety_dd", putJavaRdd);
        System.out.println("==============================>>>Programe End<<<==============================");
        DPSparkApp.stop();
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
