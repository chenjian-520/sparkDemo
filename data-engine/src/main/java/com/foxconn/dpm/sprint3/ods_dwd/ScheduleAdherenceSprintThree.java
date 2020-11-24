package com.foxconn.dpm.sprint3.ods_dwd;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * @author HS
 * @className ScheduleAdherenceSprintThree
 * @description TODO
 * @date 2020/5/15 11:22
 */
public class ScheduleAdherenceSprintThree extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        loadODSASTODWD(batchGetter.getStDateDayStampAdd(-1, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
    }


    public void loadODSASTODWD(String yesterdayStamp, String todayStamp) throws Exception {
        JavaRDD<Result> filted = DPHbase.saltRddRead("dpm_ods_production_planning_day", yesterdayStamp, todayStamp, new Scan(), true).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_PLANNING_DAY", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "work_dt", "customer", "key").toArray(new String[0])
            );
        }).reduceByKey((kv1, kv2) -> {
            return
                    Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_ODS_PRODUCTION_PLANNING_DAY", "update_dt"))
                            >
                            Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_ODS_PRODUCTION_PLANNING_DAY", "update_dt"))
                            ?
                            kv1
                            :
                            kv2
                    ;
        }).map(t -> {
            return t._2;
        });

        try {
            for (Result result : filted.take(5)) {
                System.out.println(result);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>filted End<<<==============================");

        JavaRDD<Put> putJavaRDD = filted.keyBy(r -> {
            return UUID.randomUUID().toString().replace("-", "");
        }).partitionBy(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                String rdkey = (String) key;
                return rdkey.hashCode() % 100;
            }

            @Override
            public int numPartitions() {
                return 100;
            }
        }).mapPartitions(b -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Put> puts = new ArrayList<>();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
            while (b.hasNext()) {
                StringBuilder sb = new StringBuilder();
                ArrayList<String> r = beanGetter.resultGetConfDeftColumnsValues(b.next()._2, "dpm_ods_production_planning_day", "DPM_ODS_PRODUCTION_PLANNING_DAY");
                //salt+work_dt+site_code+level_code+line_code+machine_id+key
                Date parseWorkdtStamp = null;
                try {

                    parseWorkdtStamp = new SimpleDateFormat("yyyy-MM-dd").parse(r.get(7));
                } catch (Exception e) {
                    continue;
                }
                String baseKey = sb
                        .append(parseWorkdtStamp).append(":")
                        .append(r.get(0)).append(":")
                        .append(r.get(1)).append(":")
                        .append(r.get(5)).append(":")
                        .append(r.get(6)).append(":")
                        .append(r.get(10)).toString();

                String node = consistentHashLoadBalance.selectNode(baseKey);
                r.add(0, node + ":" + baseKey);

                Put put = beanGetter.getPut("dpm_dwd_production_planning_day", "DPM_DWD_PRODUCTION_PLANNING_DAY", r.toArray(new String[0]));

                if (put != null) {
                    puts.add(put);
                }
            }
            return puts.iterator();
        });

        try {
            for (Put put : putJavaRDD.take(5)) {
                System.out.println(put);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>putJavaRDD End<<<==============================");

        try {
            DPHbase.rddWrite("dpm_dwd_production_planning_day", putJavaRDD);
        } catch (Exception e) {
            System.out.println("==============================>>>ERR Data<<<==============================");
        }
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
