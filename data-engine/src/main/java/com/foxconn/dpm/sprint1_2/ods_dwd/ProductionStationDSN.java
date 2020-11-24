package com.foxconn.dpm.sprint1_2.ods_dwd;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;


import javax.xml.stream.events.EndDocument;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className DSNUPPHCalculate
 * @description TODO
 * @date 2019/12/22 16:48
 */
public class ProductionStationDSN extends DPSparkBase {


    /* *********************************************************************************************
     * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRI             <<<<<<<<<<<<<<<<<<<<<<<<<<<<
     * ********************************************************************************************
     * 逻辑：
     *      直接把ODS的DSN扫描数据导入DWD层的汇总表
     *                                                                             **   **
     *
     *                                                                           ************
     ********************************************************************************************** */

    public String DSN_START_MIN = "";
    public String DSN_END_MIN = "";


    public static void main(String[] args) {
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        System.out.println(batchGetter.getStDataMiniteTimestampAdd(-20));
        System.out.println(batchGetter.getStDataMiniteTimestampAdd(0));
    }

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        String dsn_target_day = (String) map.get("DSN_TARGET_DAY");


        if (dsn_target_day == null || "".equals(dsn_target_day)) {
            DSN_START_MIN = (String) map.get("DSN_START_MIN");
            DSN_END_MIN = (String) map.get("DSN_END_MIN");


            BatchGetter batchGetter = MetaGetter.getBatchGetter();
            if (DSN_START_MIN == null || DSN_END_MIN == null) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:00:00.000");
                String nowTime = format.format(new Date());
                Date nowTimeDate = format.parse(nowTime);
                /*---------------------------------------------[每次以当前小时00:00.000为结束,上一个小时为开始]-------------------------------------------------*/
                long startZeroStamp = nowTimeDate.getTime();
                DSN_END_MIN = String.valueOf(startZeroStamp + 10000);

                Calendar calendar = Calendar.getInstance();
                calendar.setTime(nowTimeDate);
                calendar.add(Calendar.HOUR, -3);
                DSN_START_MIN = String.valueOf(calendar.getTime().getTime() - 10000);

                /*-----------------------------------------------------------------------------------------------------------------------------*/
            } else {
                DSN_START_MIN = batchGetter.getStDataMiniteTimestampAdd(Integer.valueOf(DSN_START_MIN));
                DSN_END_MIN = batchGetter.getStDataMiniteTimestampAdd(Integer.valueOf(DSN_END_MIN));
            }
        } else {

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            DSN_START_MIN = String.valueOf(format.parse(dsn_target_day + " 00:00:00.000").getTime() - 10000);
            DSN_END_MIN = String.valueOf(format.parse(dsn_target_day + " 23:59:59.999").getTime() + 10000);
        }


        System.out.println(DSN_START_MIN);
        System.out.println(DSN_END_MIN);

/*        DSN_START_MIN = "1590940800000";
        DSN_END_MIN = "1591804800000";*/

        dpm_ods_production_output_day_lten();
        dpm_ods_production_output_lsix_day();
        dpm_ods_production_output_lfive_day();


        DPSparkApp.stop();
    }

    public void dpm_ods_production_output_day_lten() throws Exception {

        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        JavaRDD<Result> production_station_dsn_rdd = DPHbase.saltRddRead("dpm_ods_production_output_day_lten", DSN_START_MIN, DSN_END_MIN, new Scan(), true);

        if (production_station_dsn_rdd == null) {
            System.out.println("==============================>>>Not Data Source End<<<==============================");
            return;
        }

        JavaRDD<ArrayList<String>> filter = production_station_dsn_rdd.repartition(300).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LTEN",
                    "Rowkey", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "part_no", "sku", "model_no", "customer", "wo", "workorder_type", "work_dt", "work_shift", "sn", "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"
            );
        }).map(r -> {
            String site_code = r.get(1);
            if ("".equals(r.get(1))) {
                r.set(1, "N/A");
            }
            String level_code = r.get(2);
            switch (level_code) {
                case "BU1008":
                    site_code = "WH";
                    level_code = "L10";
                    break;
                case "BU1055":
                    site_code = "CQ";
                    level_code = "L10";
                    break;
                default:
                    return null;
            }


            StringBuilder sb = new StringBuilder();
            String workDate = r.get(14);
            Date parse = new SimpleDateFormat("yyyy-MM-dd").parse(workDate);
            SimpleDateFormat formatScanDt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String baseRowKey = sb.append(String.valueOf(parse.getTime())).append(":").append(site_code).append(":").append(level_code).append(":").append(r.get(6)).append(":").append(r.get(19)).append(":").append(UUID.randomUUID().toString().replace("-", "")).toString();
            String rowkey = new ConsistentHashLoadBalance(20).selectNode(baseRowKey) + ":" + baseRowKey;
            r.set(0, rowkey);
            r.set(1, site_code);
            r.set(2, level_code);
            r.set(22, "1");
            r.set(23, String.valueOf(System.currentTimeMillis()));
            r.set(24, "HS");
            r.set(25, "ODS");
            return r;
        }).filter(r -> {
            return r != null;
        });
        try {
            for (ArrayList<String> list : filter.filter(r -> {
                return "WH".equals(r.get(1));
            }).take(5)) {
                System.out.println(list.toString());
            }
            for (ArrayList<String> list : filter.filter(r -> {
                return "CQ".equals(r.get(1));
            }).take(5)) {
                System.out.println(list.toString());
            }
        } catch (Exception e) {

        }
        JavaRDD<Put> dpm_ods_production_output_day_lten_Puts = filter.map(p -> {

            ArrayList<String> next = p;

            String rowKey = next.get(0);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("site_code"), Bytes.toBytes(next.get(1)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("level_code"), Bytes.toBytes(next.get(2)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("factory_code"), Bytes.toBytes(next.get(3)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("process_code"), Bytes.toBytes(next.get(4)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("area_code"), Bytes.toBytes(next.get(5)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("line_code"), Bytes.toBytes(next.get(6)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("machine_id"), Bytes.toBytes(next.get(7)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("part_no"), Bytes.toBytes(next.get(8)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sku"), Bytes.toBytes(next.get(9)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("platform"), Bytes.toBytes(next.get(10)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("customer"), Bytes.toBytes(next.get(11)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("wo"), Bytes.toBytes(next.get(12)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("workorder_type"), Bytes.toBytes(next.get(13)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_dt"), Bytes.toBytes(next.get(14)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_shift"), Bytes.toBytes(next.get(15)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sn"), Bytes.toBytes(next.get(16)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_code"), Bytes.toBytes(next.get(17)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_name"), Bytes.toBytes(next.get(18)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("is_fail"), Bytes.toBytes(next.get(19)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_by"), Bytes.toBytes(next.get(20)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_dt"), Bytes.toBytes(next.get(21)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("output_qty"), Bytes.toBytes(next.get(22)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("update_dt"), Bytes.toBytes(next.get(23)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("update_by"), Bytes.toBytes(next.get(24)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("data_from"), Bytes.toBytes(next.get(25)));
            return put;
        }).filter(r -> {
            return null != r;
        });
        DPHbase.rddWrite("dpm_dwd_production_output", dpm_ods_production_output_day_lten_Puts);
        System.out.println("==============================>>>L10 Programe End<<<==============================");
    }

    public void dpm_ods_production_output_lsix_day() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //读取产量数据
        JavaRDD<Result> production_station_dsn_l6_rdd = DPHbase.saltRddRead("dpm_ods_production_output_day_lsix", DSN_START_MIN, DSN_END_MIN, new Scan(), true);

        if (production_station_dsn_l6_rdd == null) {
            System.out.println("==============================>>>Not Data Source End<<<==============================");
            return;
        }

        JavaRDD<ArrayList<String>> filter = production_station_dsn_l6_rdd.repartition(300).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LSIX",
                    "Rowkey", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "part_no", "sku", "model_no", "customer", "wo", "workorder_type", "work_dt", "work_shift", "sn", "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"
            );
        }).map(r -> {
            String site_code = r.get(1);
            if ("".equals(r.get(1))) {
                r.set(1, "N/A");
            }
            String level_code = r.get(2);
            switch (level_code) {
                case "BU1061":
                    site_code = "WH";
                    level_code = "L6";
                    break;
                case "BU1036":
                    site_code = "CQ";
                    level_code = "L6";
                    break;
            }


            StringBuilder sb = new StringBuilder();
            String workDate = r.get(14);
            Date parse = new SimpleDateFormat("yyyy-MM-dd").parse(workDate);
            SimpleDateFormat formatScanDt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String baseRowKey = sb.append(String.valueOf(parse.getTime())).append(":").append(site_code).append(":").append(level_code).append(":").append(r.get(6)).append(":").append(r.get(19)).append(":").append(UUID.randomUUID().toString().replace("-", "")).toString();
            String rowkey = new ConsistentHashLoadBalance(20).selectNode(baseRowKey) + ":" + baseRowKey;
            r.set(0, rowkey);
            r.set(1, site_code);
            r.set(2, level_code);
            r.set(22, "1");
            r.set(23, String.valueOf(System.currentTimeMillis()));
            r.set(24, "HS");
            r.set(25, "ODS");
            return r;
        }).filter(r -> {
            return r != null;
        });

        try {
            for (ArrayList<String> list : filter.filter(r -> {
                return "WH".equals(r.get(1));
            }).take(5)) {
                System.out.println(list.toString());
            }
            for (ArrayList<String> list : filter.filter(r -> {
                return "CQ".equals(r.get(1));
            }).take(5)) {
                System.out.println(list.toString());
            }
        } catch (Exception e) {

        }
        JavaRDD<Put> dpm_ods_production_output_day_lsix_Puts = filter.map(p -> {
            ArrayList<String> next = p;
            String rowKey = next.get(0);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("site_code"), Bytes.toBytes(next.get(1)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("level_code"), Bytes.toBytes(next.get(2)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("factory_code"), Bytes.toBytes(next.get(3)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("process_code"), Bytes.toBytes(next.get(4)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("area_code"), Bytes.toBytes(next.get(5)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("line_code"), Bytes.toBytes(next.get(6)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("machine_id"), Bytes.toBytes(next.get(7)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("part_no"), Bytes.toBytes(next.get(8)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sku"), Bytes.toBytes(next.get(9)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("platform"), Bytes.toBytes(next.get(10)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("customer"), Bytes.toBytes(next.get(11)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("wo"), Bytes.toBytes(next.get(12)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("workorder_type"), Bytes.toBytes(next.get(13)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_dt"), Bytes.toBytes(next.get(14)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_shift"), Bytes.toBytes(next.get(15)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sn"), Bytes.toBytes(next.get(16)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_code"), Bytes.toBytes(next.get(17)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_name"), Bytes.toBytes(next.get(18)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("is_fail"), Bytes.toBytes(next.get(19)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_by"), Bytes.toBytes(next.get(20)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_dt"), Bytes.toBytes(next.get(21)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("output_qty"), Bytes.toBytes(next.get(22)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("update_dt"), Bytes.toBytes(next.get(23)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("update_by"), Bytes.toBytes(next.get(24)));
            put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("data_from"), Bytes.toBytes(next.get(25)));
            return put;
        });

        DPHbase.rddWrite("dpm_dwd_production_output", dpm_ods_production_output_day_lsix_Puts);
        System.out.println("==============================>>>L6 Programe End<<<==============================");
    }

    public void dpm_ods_production_output_lfive_day() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();

        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //读取产量数据
        //00:1577696096198:BU1008:00000001:1:a2aeead7-d493-41ad-af62-885889224221
        JavaRDD<Result> production_station_dsn_rdd = DPHbase.saltRddRead("dpm_ods_production_output_day_lfive", DSN_START_MIN, DSN_END_MIN, new Scan(), true);

        if (production_station_dsn_rdd == null) {
            System.out.println("==============================>>>Not Data Source End<<<==============================");
            return;
        }

        JavaRDD<ArrayList<String>> filter = production_station_dsn_rdd.repartition(300).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE",
                    "Rowkey", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "part_no", "sku", "model_no", "customer", "wo", "workorder_type", "work_dt", "work_shift", "sn", "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"
            );
        }).map(r -> {
            String site_code = r.get(1);
            if ("".equals(site_code)) {
                r.set(1, "N/A");
            }
            String level_code = r.get(2);
            switch (level_code) {
                case "BU1001":
                    site_code = "WH";
                    level_code = "L5";
                    break;
                default:
                    return null;
            }


            StringBuilder sb = new StringBuilder();
            String workDate = r.get(14);
            Date parse = new SimpleDateFormat("yyyy-MM-dd").parse(workDate);
            SimpleDateFormat formatScanDt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String baseRowKey = sb.append(String.valueOf(parse.getTime())).append(":").append(site_code).append(":").append(level_code).append(":").append(r.get(6)).append(":").append(r.get(19)).append(":").append(UUID.randomUUID().toString().replace("-", "")).toString();
            String rowkey = new ConsistentHashLoadBalance(20).selectNode(baseRowKey) + ":" + baseRowKey;
            r.set(0, rowkey);
            r.set(1, site_code);
            r.set(2, level_code);
            r.set(22, "1");
            r.set(23, String.valueOf(System.currentTimeMillis()));
            r.set(24, "HS");
            r.set(25, "ODS");
            return r;
        }).filter(r -> {
            return r != null;
        });

        try {
            for (ArrayList<String> list : filter.filter(r -> {
                return "WH".equals(r.get(1));
            }).take(5)) {
                System.out.println(list.toString());
            }
            for (ArrayList<String> list : filter.filter(r -> {
                return "CQ".equals(r.get(1));
            }).take(5)) {
                System.out.println(list.toString());
            }
        } catch (Exception e) {

        }

        JavaRDD<Put> dpm_ods_production_output_day_lfive_Puts =
                filter.map(p -> {
                    ArrayList<String> next = p;
                    String rowKey = next.get(0);

                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("site_code"), Bytes.toBytes(next.get(1)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("level_code"), Bytes.toBytes(next.get(2)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("factory_code"), Bytes.toBytes(next.get(3)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("process_code"), Bytes.toBytes(next.get(4)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("area_code"), Bytes.toBytes(next.get(5)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("line_code"), Bytes.toBytes(next.get(6)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("machine_id"), Bytes.toBytes(next.get(7)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("part_no"), Bytes.toBytes(next.get(8)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sku"), Bytes.toBytes(next.get(9)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("platform"), Bytes.toBytes(next.get(10)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("customer"), Bytes.toBytes(next.get(11)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("wo"), Bytes.toBytes(next.get(12)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("workorder_type"), Bytes.toBytes(next.get(13)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_dt"), Bytes.toBytes(next.get(14)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_shift"), Bytes.toBytes(next.get(15)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sn"), Bytes.toBytes(next.get(16)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_code"), Bytes.toBytes(next.get(17)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_name"), Bytes.toBytes(next.get(18)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("is_fail"), Bytes.toBytes(next.get(19)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_by"), Bytes.toBytes(next.get(20)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_dt"), Bytes.toBytes(next.get(21)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("output_qty"), Bytes.toBytes(next.get(22)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("update_dt"), Bytes.toBytes(next.get(23)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("update_by"), Bytes.toBytes(next.get(24)));
                    put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("data_from"), Bytes.toBytes(next.get(25)));
                    return put;
                });
        DPHbase.rddWrite("dpm_dwd_production_output", dpm_ods_production_output_day_lfive_Puts);
        System.out.println("==============================>>>L5 Programe End<<<==============================");
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {
    }
}
