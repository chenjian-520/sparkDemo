package com.foxconn.dpm.sprint3.ods_dwd;

import com.foxconn.dpm.sprint1_2.dws_ads.L6oeeDwsToAds;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple10;
import scala.Tuple11;
import scala.Tuple6;
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
public class Equipment_OeeL6 extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter metaGetter = MetaGetter.getSql();

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


        JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> equipmentDayRdd = DPHbase.rddRead("dpm_ods_production_equipment_day_lsix", new Scan(), true).map(r -> {
            return new Tuple10<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("line_code"))),
                    L6oeeDwsToAds.formatData(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("work_dt")))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("ttl_time"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("planned_downtime_loss_hours"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("status"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("sfc_line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("update_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("update_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("data_from"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("site_code")))
            );
        }).filter(r->("WH".equals(r._10()) || "CQ".equals(r._10())) && r._2().matches("\\d{4}-\\d{2}-\\d{2}")).keyBy(r -> r._1())
                .coalesce(10, false).reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return simpleDateFormat.parse(v1._2()).getTime() >= simpleDateFormat.parse(v2._2()).getTime() ? v1 : v2;
                }).map(r -> {
                    //还原RDD
                    return r._2();
                });
        System.out.println(equipmentDayRdd.count());
        /**
         * site_code
         * level_code
         * line_code
         * work_dt
         * oee1_ttl_time
         * oee2_ttl_time
         * planned_not_scheduled
         * data_granularity
         * update_dt
         * update_by
         * data_from
         */

        String finalFormatYesterday = formatYesterday;
        JavaRDD<Put> putJavaRdd = equipmentDayRdd.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
            long timeMillis = System.currentTimeMillis();
            while (r.hasNext()) {
                //addsalt+site_code+work_dt+level_code+factory_code+process_code+line_code
                Tuple10<String, String, String, String, String, String, String, String, String, String> line = r.next();
                String baseRowKey = sb.append("WH").append(":").append(finalFormatYesterday).append(":").append("L6").append(":").append("factory_code").append(":").append("process_code").append(":").append(line._1()).toString();
                //dpm_dwd_safety_detail
                String salt = consistentHashLoadBalance.selectNode(baseRowKey);
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                String site = line._10();
                if(site!=null || site!=""){
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("site_code"), Bytes.toBytes(line._10()));
                }else {
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("site_code"), Bytes.toBytes("WH"));
                }
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("level_code"), Bytes.toBytes("L6"));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("line_code"), Bytes.toBytes(line._1()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("work_dt"), Bytes.toBytes(finalFormatYesterday));
                if("Y".equals(line._5())){
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("oee1_ttl_time"), Bytes.toBytes(line._3()));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("oee2_ttl_time"), Bytes.toBytes(line._3()));
                }else {
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("oee1_ttl_time"), Bytes.toBytes("0"));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("oee2_ttl_time"), Bytes.toBytes("0"));
                }
               put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("planned_not_scheduled"), Bytes.toBytes(line._4()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("data_granularity"), Bytes.toBytes("line"));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(timeMillis)));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("update_by"), Bytes.toBytes("cj"));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_EQUIPMENT"), Bytes.toBytes("data_from"), Bytes.toBytes("ods"));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        // dpm_ods_production_standary_ct_lsix写入Dwd
        putJavaRdd.take(5).forEach(r -> System.out.println(r));
        System.out.println(putJavaRdd.count());
        DPHbase.rddWrite("dpm_dwd_production_equipment", putJavaRdd);
        System.out.println("数据写入dpm_dwd_production_equipment完毕");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
