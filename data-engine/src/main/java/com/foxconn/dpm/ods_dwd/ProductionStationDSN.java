package com.foxconn.dpm.ods_dwd;

import com.foxconn.dpm.Test;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.hbaseread.HGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.HBTableEntity;
import com.tm.dl.javasdk.dpspark.common.entity.HBcolumnfamilyEntity;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.hbase.DPhbBackup;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className DSNUPPHCalculate
 * @description TODO
 * @date 2019/12/22 16:48
 */
public class ProductionStationDSN extends DPSparkBase {

    /* ********************************************************************************************
     * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRI             <<<<<<<<<<<<<<<<<<<<<<<<<<<<
     * ********************************************************************************************
     * 逻辑：
     *      1.关联维度
     *      2.每日产量(已约当) / 每日工时
     * 资源：
     *      1.人力工时表：dpm_ods_manual_manhour  列簇：DPM_MANUAL_MANHOUR
     *      2.日产量表：dpm_dws_if_m_dop     列簇：DPM_DWS_IF_M_DOP
     *                                                                             **   **
     *
     *                                                                           ************
     ********************************************************************************************** */
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //dpm_ods_production_output_day_lten();
        dpm_ods_production_output_lsix_day();
        //dpm_ods_production_output_lfive_day();

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
        SqlGetter sqlGetter = MetaGetter.getSql();
        HGetter hGetter = MetaGetter.getHGetter();
        BeanGetter beanGetter = MetaGetter.getBeanGetter();

        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //读取产量数据
        System.out.println(batchGetter.getStDataMiniteTimestampAdd(-10) + ":BU1008");
        System.out.println(batchGetter.getStDataMiniteTimestampAdd(10) + ":BU1008");
        //JavaRDD<Result> production_station_dsn_rdd = hGetter.saltRead("dpm_ods_production_output_day_lten", batchGetter.getStDataMiniteTimestampAdd(-10) + ":BU1008", batchGetter.getStDataMiniteTimestampAdd(10) + ":BU1008", true, ":", 20);
        JavaRDD<Result> production_station_dsn_rdd = DPHbase.saltRddRead("dpm_ods_production_output_day_lten", batchGetter.getStDataMiniteTimestampAdd(-10) + ":BU1008", batchGetter.getStDataMiniteTimestampAdd(10) + ":BU1008", new Scan());


        JavaRDD<Put> dpm_ods_production_output_day_lten_Puts = production_station_dsn_rdd.filter(r -> {
            return batchGetter.checkColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LTEN",
                    "BUCode", "ShiftCode", "LineCode", "WorkDate", "WO", "WOType", "PartNo", "Customer", "ModelNo", "SN", "StationCode", "IsFail", "ScanDT"
            );
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LTEN",
                    "Rowkey", "site_code", "BUCode", "plant_code", "process_code", "AreaCode", "LineCode", "machine_id", "PartNo", "sku", "ModelNo", "Customer", "WO", "WOType", "WorkDate", "ShiftCode", "SN", "StationCode", "StationName", "IsFail", "ScanBy", "ScanDT", "output_qty", "UpdateDT", "UpdateBy", "DataFrom"
            );
        }).repartition(40).map(r -> {
            switch (r.get(2)) {
                case "BU1008":
                    StringBuilder sb = new StringBuilder();
                    String workDate = r.get(14);
                    Date parse = new SimpleDateFormat("yyyy-MM-dd").parse(workDate);
                    SimpleDateFormat formatScanDt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                    String baseRowKey = sb.append(String.valueOf(parse.getTime())).append(":").append("WH").append(":").append("L10").append(":").append(r.get(6)).append(":").append(r.get(19)).append(":").append(UUID.randomUUID().toString()).toString();
                    String rowkey = new ConsistentHashLoadBalance(20).selectNode(baseRowKey) + ":" + baseRowKey;
                    r.set(0, rowkey);
                    r.set(1, "WH");
                    r.set(2, "L10");
                    r.set(21, String.valueOf(formatScanDt.parse(r.get(21)).getTime()));
                    r.set(22, "1");
                    r.set(23, String.valueOf(System.currentTimeMillis()));
                    r.set(24, "HS");
                    r.set(25, "L10");
                    return r;
                default:
                    return null;
            }
        }).filter(r -> {
            return r != null;
        }).mapPartitions(p -> {
            ArrayList<Put> puts = new ArrayList<>();
            while (p.hasNext()) {
                try {

                    Put put = MetaGetter.getBeanGetter().getPut("dpm_dwd_production_output_day", "DPM_DWD_PRODUCTION_OUTPUT_DAY", p.next().toArray(new String[0]));
                    if (put != null){
                        puts.add(put);
                    }
                } catch (Exception e) {
                    System.out.println("err put=====>>>>" + e.toString());
                }
            }
            return puts.iterator();
        });
        List<Put> take = dpm_ods_production_output_day_lten_Puts.take(10);
        for (Put put : take) {
            System.out.println(put);
        }

        //00:1577776502305:BU1008:00000001:1:a2aeead7-d493-41ad-af62-885889224221
        DPHbase.rddWrite("dpm_dwd_production_output_day", dpm_ods_production_output_day_lten_Puts);
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
        SqlGetter sqlGetter = MetaGetter.getSql();
        HGetter hGetter = MetaGetter.getHGetter();
        BeanGetter beanGetter = MetaGetter.getBeanGetter();

        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //读取产量数据
        JavaRDD<Result> production_station_dsn_l6_rdd = DPHbase.saltRddRead("dpm_ods_production_output_day_lsix", batchGetter.getStDataMiniteTimestampAdd(-10) + ":BU1061", batchGetter.getStDataMiniteTimestampAdd(10) + ":BU1061", new Scan());


        JavaRDD<Put> dpm_ods_production_output_day_lsix_Puts = production_station_dsn_l6_rdd.filter(r -> {
            return batchGetter.checkColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LSIX",
                    "BUCode", "ShiftCode", "Process", "LineCode", "WorkDate", "PartNo", "Customer", "ModelNo", "SN", "StationCode", "IsPass", "IsFail", "ScanDT"
            );
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LSIX",
                    "Rowkey","site_code","BUCode","plant_code","Process","area_code","LineCode","machine_id","PartNo","sku","ModelNo","Customer","WO","workorder_type","WorkDate","ShiftCode","SN","StationCode","StationName","IsFail","ScanBy","ScanDT","output_qty","UpdateDT","UpdateBy","DataFrom"
            );
        }).repartition(40).map(r -> {
            switch (r.get(2)) {
                case "BU1061":
                    StringBuilder sb = new StringBuilder();
                    String workDate = r.get(14);
                    Date parse = new SimpleDateFormat("yyyy-MM-dd").parse(workDate);
                    SimpleDateFormat formatScanDt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                    String baseRowKey = sb.append(String.valueOf(parse.getTime())).append(":").append("WH").append(":").append("L6").append(":").append(r.get(6)).append(":").append(r.get(19)).append(":").append(UUID.randomUUID().toString()).toString();
                    String rowkey = new ConsistentHashLoadBalance(20).selectNode(baseRowKey) + ":" + baseRowKey;
                    r.set(0, rowkey);
                    r.set(1, "WH");
                    r.set(2, "L10");
                    r.set(21, String.valueOf(formatScanDt.parse(r.get(21)).getTime()));
                    r.set(22, "1");
                    r.set(23, String.valueOf(System.currentTimeMillis()));
                    r.set(24, "HS");
                    r.set(25, "L10");
                    return r;
                default:
                    return null;
            }
        }).filter(r -> {
            return r != null;
        }).mapPartitions(p -> {
            ArrayList<Put> puts = new ArrayList<>();
            while (p.hasNext()) {
                try {
                    Put put = MetaGetter.getBeanGetter().getPut("dpm_dwd_production_output_day", "DPM_DWD_PRODUCTION_OUTPUT_DAY", p.next().toArray(new String[0]));
                    puts.add(put);
                }catch (Exception e){
                    System.out.println("err put=====>>>>" + e.toString());
                }
            }
            return puts.iterator();
        });
        for (Put put : dpm_ods_production_output_day_lsix_Puts.take(10)) {
            System.out.println(put);
        }
        DPhbBackup.rddWrite("dpm_dwd_production_output_day", dpm_ods_production_output_day_lsix_Puts);
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
        SqlGetter sqlGetter = MetaGetter.getSql();
        HGetter hGetter = MetaGetter.getHGetter();
        BeanGetter beanGetter = MetaGetter.getBeanGetter();

        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //读取产量数据
        //00:1577696096198:BU1008:00000001:1:a2aeead7-d493-41ad-af62-885889224221
        JavaRDD<Result> production_station_dsn_rdd = hGetter.saltRead("dpm_ods_production_output_day_lfive", batchGetter.getStDataMiniteTimestampAdd(-10) + ":BU1001", batchGetter.getStDataMiniteTimestampAdd(10) + ":BU1001", true, ":", 20);
        //JavaRDD<Result> production_station_dsn_rdd = DPHbase.saltRddRead("dpm_ods_production_output_day_lfive", batchGetter.getNowMinuteAdd(-600), batchGetter.getNowMinuteAdd(1), new Scan());

        JavaRDD<Put> dpm_ods_production_output_day_lfive_Puts = production_station_dsn_rdd.filter(r -> {
            return batchGetter.checkColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE",
                    "BUCode", "AreaCode", "ShiftCode", "LineCode", "WorkDate", "WO", "WOType", "PartNo", "Customer", "ModelNo", "SN", "StationCode", "StationName", "IsPass", "IsFail", "ScanDT"
            );
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE",
                    "BUCode", "PlantCode", "AreaCode", "ShiftCode", "LineCode", "WorkDate", "WOType", "PartNo", "Customer", "ModelNo", "SN", "StationCode", "StationName", "IsFail", "ScanBy", "ScanDT"
            );
        }).repartition(40).map(r -> {
            ArrayList<String> formatResultRowList = new ArrayList<>();
            switch (r.get(0)) {
                case "BU1001":
                    StringBuilder sb = new StringBuilder();
                    String workDate = r.get(5);
                    Date parse = new SimpleDateFormat("yyyy-MM-dd").parse(workDate);
                    String baseRowKey = sb.append(String.valueOf(parse.getTime())).append(":").append("WH").append(":").append("L5").append(":").append(r.get(4)).append(":").append(r.get(13)).append(":").append(UUID.randomUUID().toString()).toString();
                    r.remove(0);
                    String rowkey = new ConsistentHashLoadBalance(20).selectNode(baseRowKey) + ":" + baseRowKey;
                    formatResultRowList.add(rowkey);
                    formatResultRowList.add("WH");
                    formatResultRowList.add("L5");
                    formatResultRowList.addAll(r);
                    formatResultRowList.add("1");
                    formatResultRowList.add(String.valueOf(System.currentTimeMillis()));
                    formatResultRowList.add("HS");
                    formatResultRowList.add(String.valueOf(System.currentTimeMillis()));
                    formatResultRowList.add("HS");
                    formatResultRowList.add("L5");

                    return formatResultRowList;
                default:
                    return null;
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return r.get(6);
        }).mapPartitions(p -> {
            ArrayList<Put> puts = new ArrayList<>();
            while (p.hasNext()) {
                Tuple2<String, ArrayList<String>> next = p.next();
                Put put = MetaGetter.getBeanGetter().getPut("dpm_dwd_production_output_day_lfive", "DPM_DWD_PRODUCTION_OUTPUT_DAY", next._2.toArray(new String[0]));

                if (put == null) {
                    System.out.println(next);
                }

                puts.add(put);
            }
            return puts.iterator();
        });
        List<Put> take = dpm_ods_production_output_day_lfive_Puts.top(10);
        for (Put put : take) {
            System.out.println(put);
        }
        DPHbase.rddWrite("dpm_dwd_production_output_day", dpm_ods_production_output_day_lfive_Puts);

        System.out.println("==============================>>>L5 Programe End<<<==============================");
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {
    }
}
