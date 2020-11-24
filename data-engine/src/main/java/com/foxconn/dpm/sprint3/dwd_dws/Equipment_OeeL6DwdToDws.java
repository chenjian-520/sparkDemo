package com.foxconn.dpm.sprint3.dwd_dws;

import com.foxconn.dpm.sprint1_2.dws_ads.L6oeeDwsToAds;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple11;
import scala.Tuple12;
import scala.Tuple9;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
/**
 * 数据处理业务类 
 *
 * @author cj
 * @version 1.0.0
 * @className 
 * @data 2020-05-11
 */
public class Equipment_OeeL6DwdToDws extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String formatYesterday = simpleDateFormat.format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-1))));
        Calendar c = Calendar.getInstance();


        String finalFormatYesterday = formatYesterday;
        String formatYesterday1 = simpleDateFormat.format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-2))));
        JavaRDD<Tuple11<String, String, String, String, String, String, String, String,String, String,String>> equipmentDayRdd = DPHbase.rddRead("dpm_dwd_production_equipment",new Scan(), true).map(r -> {
            return new Tuple11<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("level_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("work_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("oee1_ttl_time"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("oee2_ttl_time"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("planned_not_scheduled"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("data_granularity"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("update_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("update_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("data_from")))
            );
        }).filter(r-> (finalFormatYesterday.equals(r._4()) || formatYesterday1.equals(r._4())) && "WH".equals(r._1()) && "L6".equals(r._2()) );


        JavaRDD<Put> putJavaRdd = equipmentDayRdd.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
            long timeMillis = System.currentTimeMillis();
            while (r.hasNext()) {
                //addsalt+update_dt+site_code+level_code+line_code+work_dt
                Tuple11<String, String, String, String, String, String, String, String, String, String, String> line = r.next();
                long time = simpleDateFormat.parse(line._4()).getTime();
                String baseRowKey = sb.append(String.valueOf(time)).append(":").append(line._1()).append(":").append(line._2()).append(":").append(line._3()).append(":").append(line._4()).toString();

                String salt = consistentHashLoadBalance.selectNode(baseRowKey);
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));

                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(line._1()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("level_code"), Bytes.toBytes(line._2()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("line_code"), Bytes.toBytes(line._3()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("work_dt"), Bytes.toBytes(line._4()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("planned_downtime_loss_hours"), Bytes.toBytes(line._7()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("production_time"), Bytes.toBytes(line._6()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("data_granularity"), Bytes.toBytes("line"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(timeMillis)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("update_by"), Bytes.toBytes("cj"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_EQUIPMENT_LINE_DD"), Bytes.toBytes("data_from"), Bytes.toBytes("dwd"));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        // dpm_ods_production_standary_ct_lsix写入Dwd
        putJavaRdd.take(5).forEach(r -> System.out.println(r));
        DPHbase.rddWrite("dpm_dws_production_equipment_line_dd", putJavaRdd);
        System.out.println("数据写入dpm_dws_production_equipment_line_dd完毕");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
