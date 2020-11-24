package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.DsnDayOutPut;
import com.foxconn.dpm.sprint1_2.dwd_dws.beans.ManualNormalization;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className DayOutPutCalculate
 * @description TODO
 * @date 2019/12/29 19:09
 */
public class DayOutPutCalculate extends DPSparkBase {

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        dayOutPutCalculate(map);
    }

    /*
     * ====================================================================
     * 描述:
     *      按照过站信息计算日产量
     *
     *      单个SN生产完毕规则: SN 开始工站 ispass  结束工站 ispass     记 1
     *
     *      dwd数据源有用字段：
     *          "SiteCode","LevelCode","PlantCode","ProcessCode","AreaCode","LineCode","MachineID","PartNo","Sku","Plantform",
     *          "Customer","WorkorderType","WorkDT","WorkShifitClass","SN","StationCode","IsFail","ScanDT","DataFrom"
     *      dws目标字段：
     *          "SiteCodeID","LevelCodeID","PlantCodeID","ProcessCodeID","AreaCodeID","LineCodeID","MachineID","PartNo","Sku",
     *          "Plantform","WorkorderType","WorkDT","WorkShifitClass","normalized_output_qty","InsertDT","InsertBy","UpdateDT","UpdateBy","DataFrom"
     *
     * ====================================================================
     */
    public void dayOutPutCalculate(Map<String, Object> map) throws Exception {
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();

        String startDay = (String) map.get("startDay");
        String endDay = (String) map.get("endDay");
        LoadKpiTarget.getLineDateset();
        String startStamp = "";
        String endStamp = "";
        if (!(null == startDay || "".equals(startDay)) && !(null == endDay || "".equals(endDay))) {
            startStamp = String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(startDay + " 00:00:00.000").getTime());
            endStamp = String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(endDay + " 00:00:00.000").getTime());
        } else {
            startStamp = batchGetter.getStDateDayStampAdd(-2);
            endStamp = batchGetter.getStDateDayStampAdd(1);
        }
        LoadKpiTarget.getLineDateset();
        System.out.println(startStamp + "_" + endStamp);


        JavaRDD<Result> day_dop_rdd = DPHbase.saltRddRead("dpm_dwd_production_output", startStamp, endStamp, new Scan(), true);
        JavaPairRDD<String, ArrayList<String>> checkedDataRDD = day_dop_rdd.map((r) -> {
            return batchGetter.resultGetColumns(r, "DPM_DWD_PRODUCTION_OUTPUT",
                    "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "part_no", "sku", "platform", "customer", "wo", "workorder_type", "work_dt", "work_shift", "sn", "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"
            );
        }).filter(r -> {

            if (r == null) {
                return false;
            }

            //"SiteCode", "LevelCode", "Key", "WorkDT", "WorkShifitClass", "SN", "StationCode", "IsFail", "ScanDT", "WorkorderType"
            //L5    PACKING
            //L6    PACKING
            //L10   STORAGE-PALLET
            return LoadKpiTarget.levelOutPutFilter("line=packing", "D", r.get(0), r.get(1), r.get(10), r.get(16), r.get(18));

        }).keyBy(r -> {
            //day site level sn station_code
            return r.get(13) + r.get(0) + r.get(1) + r.get(15) + r.get(16);
        }).reduceByKey((rv1, rv2) -> {
            //使用最后更新时间进行去重
            return Long.valueOf(rv1.get(22)) >= Long.valueOf(rv2.get(22)) ? rv1 : rv2;
        }).filter(r -> {
            return r != null && r._2 != null;
        }).map(t -> {
            return t._2;
        }).keyBy((r) -> {
            return batchGetter.getStrArrayOrg("=", "",
                    //"site_code","level_code","factory_code","process_code","area_code","line_code","machine_id","part_no","sku","platform","customer",
                    // "wo","workorder_type","work_dt","work_shift","sn","station_code","station_name","is_fail","scan_by","scan_dt","output_qty","update_dt",
                    // "update_by","data_from"
                    //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","workorder_type","work_dt","customer"
                    r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(7), r.get(8), r.get(9), r.get(12), r.get(13), r.get(10), r.get(14)
            );
        }).persist(StorageLevel.MEMORY_AND_DISK());

        try {
            JavaPairRDD<String, ArrayList<String>> whfilted = checkedDataRDD.filter(r -> {
                return "WH".equals(r._2.get(0)) && "D".equals(r._2.get(14)) && "2020-06-30".equals(r._2.get(13));
            });
            /*long count = whfilted.filter(r -> {
                return "2020-06-17".equals(r._2.get(13)) && "L10".equals(r._2.get(1)) && "D".equals(r._2.get(14));
            }).count();
            System.out.println(count);*/
            for (Tuple2<String, ArrayList<String>> stringArrayListTuple2 : whfilted.take(5)) {
                System.out.println(stringArrayListTuple2);
            }
            List<Tuple2<String, ArrayList<String>>> take1 = checkedDataRDD.filter(r -> {
                return "CQ".equals(r._2.get(0));
            }).take(5);
            for (Tuple2<String, ArrayList<String>> stringArrayListTuple2 : take1) {
                System.out.println(stringArrayListTuple2);
            }

        } catch (Exception e) {

        }
        System.out.println("==============================>>>Dsn Checked End<<<==============================");

        /* ********************************************************************************************
         * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRIBE             <<<<<<<<<<<<<<<<<<<<<<<<<<<
         * ********************************************************************************************
         *      key=> //"site_code","level_code","factory_code","process_code","area_code","line_code",
         *      "part_no","sku","platform","workorder_type","work_dt","customer"
         *
         *      计算每天的产量只需要修正每条SN凌晨过站记录的WorkDate即可，因为按照工作日统计产量
         *      修正后的每条SN记录GroupBy后就是每天的产量。
         *
         *                                                                             **   **
         *
         *                                                                           ************
         ********************************************************************************************** */
        JavaPairRDD<String, Long> everyDayOutPutCountsRDD = checkedDataRDD/*.map(t -> {
            //"site_code","level_code","factory_code","process_code","area_code","line_code","machine_id","part_no","sku","platform","customer",
            // "wo","workorder_type","work_dt","work_shift","sn","station_code","station_name","is_fail","scan_by","scan_dt","output_qty","update_dt",
            // "update_by","data_from"

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


            ArrayList<String> r = t._2;
            String workDt = r.get(13);
            String scanDt = simpleDateFormat.format(new Date(Long.valueOf(r.get(20))));

            //今天零点到12点的晚班
            String todayDateZero = workDt + " 00:00:00.000";
            //昨天凌晨晚班结束也就是今天12点以前
            String todayDateEnd = workDt + " 12:00:00.000";
            //如果扫描时间大于等于今天零点并且小于等于今天凌晨6点并且班别为晚班的则为昨天的产量
            if (batchGetter.dateStrCompare(scanDt, todayDateZero, "yyyy-MM-dd HH:mm:ss.SSS", ">=") && batchGetter.dateStrCompare(scanDt, todayDateEnd, "yyyy-MM-dd HH:mm:ss.SSS", "<=") && r.get(14).equals("N")) {
                //工作日期要算昨天的
                r.set(13, batchGetter.getStDateDayStrAdd(workDt, -1, "-"));
                return new Tuple2<String, ArrayList<String>>(batchGetter.getStrArrayOrg("=", "",
                        //"site_code","level_code","factory_code","process_code","area_code","line_code","machine_id","part_no","sku","platform","customer",
                        // "wo","workorder_type","work_dt","work_shift","sn","station_code","station_name","is_fail","scan_by","scan_dt","output_qty","update_dt",
                        // "update_by","data_from"
                        //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","workorder_type","work_dt","customer"
                        r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(7), r.get(8), r.get(9), r.get(12), r.get(13), r.get(10), r.get(14)
                ), r);
            } else {
                //如果不是凌晨扫描的则不需要修正
                return t;
            }
        })*/.mapToPair(new PairFunction<Tuple2<String, ArrayList<String>>, String, Long>() {
            public Tuple2<String, Long> call(Tuple2<String, ArrayList<String>> r) throws Exception {
                return new Tuple2(r._1, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long c1, Long c2) throws Exception {
                return c1 + c2;
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());
        try {
           /* List<Tuple2<String, Long>> take = everyDayOutPutCountsRDD.filter(r -> {
                return r._1.contains("1A315HV00-600-G");
            }).take(5);
            for (Tuple2<String, Long> stringLongTuple2 : take) {
                System.out.println(stringLongTuple2);
            }*/
            for (Tuple2<String, Long> stringLongTuple2 : everyDayOutPutCountsRDD/*.filter(r -> {
                return !r._1.split("^")[10].matches("[\\d]{4}-[\\d]{2}-[\\d]{2}");
            })*/.take(5)) {
                System.out.println(stringLongTuple2);
            }

        } catch (Exception e) {

        }
        System.out.println("==============================>>>WorkDate Modified End<<<==============================");

        JavaRDD<DsnDayOutPut> dayOutPutRDD = everyDayOutPutCountsRDD.map(r -> {
            String[] orgSplit = r._1.split("=");
            //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","workorder_type","work_dt","customer", "work_shift"

            //String siteCode, String levelCode, String plantCode, String processCode, String area_code, String line_code, String part_no, String sku,
            // String platform, String WOType, String workDate, String customer, String key, Long QTY
            String key = "";
            switch (orgSplit[1]) {
                case "L5":
                case "L6":
                    key = orgSplit[6];
                    break;
                case "L10":
                    if ("CQ".equals(orgSplit[0])) {
                        key = orgSplit[6];
                    } else if ("WH".equals(orgSplit[0])) {
                        key = orgSplit[8];
                    }
                    /*
                     * L10 使用机种作为关联key
                     */
                    break;
            }
            return new DsnDayOutPut(orgSplit[0], orgSplit[1], orgSplit[2], orgSplit[3], orgSplit[4], orgSplit[5], orgSplit[6],
                    orgSplit[7], orgSplit[8], orgSplit[9], orgSplit[10], "LENOVO_CODE".equals(orgSplit[11].toUpperCase()) ? "LENOVO" : orgSplit[11], key, r._2, orgSplit[12]);


        }).filter(r -> {
            return r != null;
        });


        try {
            for (DsnDayOutPut dsnDayOutPut : dayOutPutRDD.take(5)) {
                System.out.println(dsnDayOutPut);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>OutPutDay End<<<==============================");

        /*
         * ====================================================================
         *  约当系数表
         * ====================================================================
         */
        Scan normalization_scan = new Scan();
        JavaRDD<Result> manual_normalization_Rdd = DPHbase.saltRddRead("dpm_dim_production_normalized_factor", "!", "~", normalization_scan, true);
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DIM_PRODUCTION_NORMALIZED_FACTOR", "key", "level_code", "normalization", "normalization_bto", "normalization_cto", "update_dt");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DIM_PRODUCTION_NORMALIZED_FACTOR", "key", "level_code", "normalization", "normalization_bto", "normalization_cto", "update_dt", "site_code");
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return r.get(0) + r.get(1) + r.get(6);
        }).reduceByKey((v1, v2) -> {

            return Long.valueOf(v1.get(5)) > Long.valueOf(v2.get(5)) ? v1 : v2;

        }).map(t -> {
            return t._2;
        }).map(r -> {
            return new ManualNormalization(r.get(0).trim(), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)), r.get(6));
        }).distinct();


        try {

            for (ManualNormalization manualNormalization : format_manual_normalization_Rdd.take(10)) {

                System.out.println(manualNormalization);
            }

        } catch (Exception e) {

        }


        System.out.println("manualNormalization========================>>>");


        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(dayOutPutRDD, DsnDayOutPut.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);

        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_dsn_day_output");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_dim_production_normalized_factor");

        sqlContext.sql("select * from dpm_dim_production_normalized_factor limit 10").show();
        sqlContext.sql("select * from dpm_dws_dsn_day_output limit 10").show();


        /*---------------------------------------------[发送无约当系数数据邮件到用户邮箱]-------------------------------------------------*/

        //  sqlContext.sql(sqlGetter.Get("dpm_dws_dsn_day_output_calculate_err_data.sql").replace("${etl_time}", System.currentTimeMillis() + ""));

        /*--------------------------------------------------------------------------------------------------------------------------------*/


        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("dpm_dws_dsn_day_output_calculate.sql").replace("${etl_time}", System.currentTimeMillis() + ""));


        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            //rowDataset.javaRDD().repartition(1).saveAsTextFile("hdfs://10.124.160.20:8020/dpuserdata/41736e50-883d-42e0-a484-0633759b92/OutputDay" + System.currentTimeMillis());
            for (Row row : rowDataset.javaRDD().collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        JavaRDD<Put> calculatedOutPutRDD = rowDataset.toJavaRDD().mapPartitions(it -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Put> puts = new ArrayList<>();
            while (it.hasNext()) {
                Row next = it.next();
                //[null,WH,L5,DT2,,,L6_05,1A52CB000-600,,ASSY,CHAS,L4,15,1.528860165,line,MICRO,1589277683,HS,DWD,3070]
                if (next.getString(1) == null || "null".equals(next.getString(1))) {
                    continue;
                }
                Put put = beanGetter.getPut("dpm_dws_production_output_dd", "DPM_DWS_PRODUCTION_OUTPUT_DD", next);
                puts.add(put);
            }
            return puts.iterator();
        });

        DPHbase.rddWrite("dpm_dws_production_output_dd", calculatedOutPutRDD);
        System.out.println("==============================>>>Programe End<<<==============================");
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
