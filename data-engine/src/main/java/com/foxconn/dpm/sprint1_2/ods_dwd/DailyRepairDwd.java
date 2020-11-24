package com.foxconn.dpm.sprint1_2.ods_dwd;

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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @version 1.0
 * @program: L10DailyRepairDwd
 * @description: ODS層dpm_ods_production_repair_day_lten直接原樣轉到
 * DWD層dpm_dwd_production_repair（bu_code轉成level_code成L10）
 * @author: Axin
 * @create: 2020-01-15 17:25
 **/
public class DailyRepairDwd extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        System.out.println("==============================>>>Programe Start<<<==============================");
        String yesterday = null;
        String tomorrow = null;
        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-4, "-");
            tomorrow = batchGetter.getStDateDayAdd(1, "-");
        } else {
            yesterday = String.valueOf(map.get("workDate"));
            tomorrow = batchGetter.getStDateDayStrAdd(String.valueOf(map.get("workDate")), 1, "-");
        }
        SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");
        String startRowkey = String.valueOf(yyyy_MM_dd.parse(yesterday).getTime());
        String endRowkey = String.valueOf(yyyy_MM_dd.parse(tomorrow).getTime());
        System.out.println(startRowkey + "_" + endRowkey);

        /**
         *
         * 获取维修信息,不加盐
         */
        JavaRDD<Result> repairData = DPHbase.saltRddRead("dpm_ods_production_repair_day_lten", startRowkey, endRowkey, new Scan(), true);

        JavaRDD<ArrayList<String>> arrayListJavaRDD = repairData.mapPartitions(batchData -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
            String family = "DPM_ODS_PRODUCTION_REPAIR_DAY_LTEN";
            ArrayList<ArrayList<String>> strings = new ArrayList<>();
            while (batchData.hasNext()) {
                Result output = batchData.next();
                SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
                ArrayList<String> rs = beanGetter.resultGetConfDeftColumnsValues(output, "dpm_ods_production_repair_day_lten", family);

                /**
                 * - site_code=String
                 * - area_code=String
                 * - line_code=String
                 * - work_shift=String
                 * - work_dt=String
                 * - part_no=String
                 * - customer=String
                 * - model_no=String
                 * - wo=String
                 * - sn=String
                 * - repair_in_dt=String
                 * - fail_code=String
                 * - fail_desc=String
                 * - repair_out_dt=String
                 * - fail_station=String
                 * - repair_code=String
                 * - repair_code_desc=String
                 * - bu_code=String
                 * - update_dt=String
                 */
                String bu_code = rs.get(17);
                String site_code = rs.get(0);
                String update_by = rs.get(18);
                if (update_by == "") {
                    update_by = String.valueOf(System.currentTimeMillis());
                }

                switch (bu_code) {
                    case "BU1008":
                        site_code = "WH";
                        break;
                    case "BU1055":
                        site_code = "CQ";
                        break;
                }
                rs.remove(17);

                //addsalt+work_dt+site_code+level_code+line_code+sn+uuid
                String rowkey = formatWorkDt.parse(rs.get(4)).getTime() + ":" + site_code + ":" + "L10" + ":" + rs.get(2) + ":" + rs.get(9) + ":" + UUID.randomUUID().toString().replace("-", "");
                rowkey = consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
                rs.add(0, rowkey);
                rs.set(1, site_code);
                rs.add(2, "L10");
                rs.set(19, update_by);
                rs.add("HS");
                rs.add("ODS");
                strings.add(rs);
                /*      - rowkey
                 *      - site_code=String
                 *      - level_code
                 *      - area_code=String
                 *      - line_code=String
                 *      - work_shift=String
                 *      - work_dt=String
                 *      - part_no=String
                 *      - customer=String
                 *      - model_no=String
                 *      - wo=String
                 *      - sn=String
                 *      - repair_in_dt=String
                 *      - fail_code=String
                 *      - fail_desc=String
                 *      - repair_out_dt=String
                 *      - fail_station=String
                 *      - repair_code=String
                 *      - repair_code_desc=String
                 *      - update_dt=String
                 */
            }
            return strings.iterator();
        }).keyBy(r -> {
            //sn + work_dt
            return r.get(11) + r.get(4);
        }).reduceByKey((v1, v2) -> {
            return Long.valueOf(v1.get(19))
                    >
                    Long.valueOf(v2.get(19))
                    ?
                    v1
                    :
                    v2;
        }).map(t -> {
            return t._2;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Put> toWriteData = arrayListJavaRDD.mapPartitions(iterator -> {

            List<Put> puts = new ArrayList<>();

            String toWriteFamily = "DPM_DWD_PRODUCTION_REPAIR";
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            while (iterator.hasNext()) {
                ArrayList<String> next = iterator.next();
                puts.add(beanGetter.getPut("dpm_dwd_production_repair", toWriteFamily, next.toArray(new String[0])));
            }
            return puts.iterator();
        });

        DPHbase.rddWrite("dpm_dwd_production_repair", toWriteData);
        System.out.println("==============================>>>Programe End<<<==============================");
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
