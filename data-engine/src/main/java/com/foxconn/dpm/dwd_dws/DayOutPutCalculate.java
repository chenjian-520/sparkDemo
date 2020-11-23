package com.foxconn.dpm.dwd_dws;

import com.foxconn.dpm.Test;
import com.foxconn.dpm.dwd_dws.beans.DsnDayOutPut;
import com.foxconn.dpm.temporary.beans.DayDop;
import com.foxconn.dpm.temporary.beans.ManualHour;
import com.foxconn.dpm.temporary.beans.ManualNormalization;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.hbaseread.HGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author HS
 * @className DayOutPutCalculate
 * @description TODO
 * @date 2019/12/29 19:09
 */
public class DayOutPutCalculate extends DPSparkBase {

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        dayOutPutCalculate();

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
    public void dayOutPutCalculate() throws Exception {
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        HGetter hGetter = MetaGetter.getHGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();
        System.out.println(batchGetter.getStDateDayStampAdd(-100, new String[0]));
        System.out.println(batchGetter.getStDateDayStampAdd(0, new String[0]));


        JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dwd_production_output_day", batchGetter.getStDateDayStampAdd(-100, new String[0]), batchGetter.getStDateDayStampAdd(0, new String[0]), true, ":", 20, new String[0]);
        //JavaRDD<Result> day_dop_rdd = DPHbase.saltRddRead("dpm_dwd_production_output_day", batchGetter.getStDateDayAdd(-1, new String[0]), batchGetter.getStDateDayAdd(0, new String[0]), new Scan());
        JavaPairRDD<String, ArrayList<String>> checkedDataRDD = day_dop_rdd.filter((r) -> {
            return batchGetter.checkColumns(r, "DPM_DWD_PRODUCTION_OUTPUT_DAY",
                    new String[]{"SiteCode", "LevelCode", "PartNo", "Plantform", "WorkDT", "WorkShifitClass", "SN", "StationCode", "IsFail", "ScanDT"});
        }).map((r) -> {
            return batchGetter.resultGetColumnsCount(r, "DPM_DWD_PRODUCTION_OUTPUT_DAY",
                    new String[]{"SiteCode", "LevelCode", "PartNo", "Plantform", "WorkDT", "WorkShifitClass", "SN", "StationCode", "IsFail", "ScanDT", "WorkorderType"});
        }).map(r -> {
            //2019-12-1412:45:23.012
            try {

                String destScanDT = batchGetter.formatDateStrTo(r.get(9), "yyyy-MM-ddhh:mm:ss.SSS", "yyyy-MM-dd hh:mm:ss.SSS");
                if (destScanDT == null || destScanDT.equals("")) {
                    return null;
                } else {
                    r.set(9, destScanDT);
                }
            }catch (Exception e){
                return null;
            }
            switch (r.get(1)) {
                case "L5":
                case "L6":
                    r.remove(3);
                    break;
                case "L10":
                    r.remove(2);
                    break;
                default:
                    return null;
            }
            return r;
        }).filter(r -> {
            if (r == null){return false;}
            //"SiteCode", "LevelCode", "Key", "WorkDT", "WorkShifitClass", "SN", "StationCode", "IsFail", "ScanDT", "WorkorderType"
            String stationCode = r.get(6);
            String IsFail = r.get(7);
            if ((stationCode.trim().equals("PACKING") || stationCode.trim().equals("PALLETIZATION")) && IsFail.equals("0")) {
                return true;
            }
            return false;
        }).distinct().keyBy((r) -> {
            return batchGetter.getStrArrayOrg(",", new String[]{(String) r.get(0), (String) r.get(1), (String) r.get(2), (String) r.get(3), r.get(9)});
        }).persist(StorageLevel.MEMORY_AND_DISK());


        /* ********************************************************************************************
         * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRIBE             <<<<<<<<<<<<<<<<<<<<<<<<<<<
         * ********************************************************************************************
         *      计算每天的产量只需要修正每条SN凌晨过站记录的WorkDate即可，因为按照工作日统计产量
         *      修正后的每条SN记录GroupBy后就是每天的产量。
         *
         *                                                                             **   **
         *
         *                                                                           ************
         ********************************************************************************************** */
        JavaPairRDD<String, Long> everyDayOutPutCountsRDD = checkedDataRDD.filter((r) -> {
            return r != null;
        }).map(t -> {
            //"SiteCode", "LevelCode", "Key", "WorkDT", "WorkShifitClass", "SN", "StationCode", "IsFail", "ScanDT"
            ArrayList<String> r =  t._2;
            String workDt = r.get(3);
            String scanDt = (String) r.get(8);

            //今天零点，默认WorkDt为昨天
            String todayDateZero = workDt + " 00:00:00.000";
            //昨天凌晨晚班结束也就是今天6点以前
            String todayDateEnd = batchGetter.getStDateDayStrAdd(workDt,1, "-") + " 06:00:00.000";
            //如果扫描时间大于等于今天零点并且小于等于今天凌晨6点并且班别为晚班的则为昨天的产量
            if (batchGetter.dateStrCompare(scanDt, todayDateZero, "yyyy-MM-dd hh:mm:ss.SSS", ">=") && batchGetter.dateStrCompare(scanDt, todayDateEnd, "yyyy-MM-dd hh:mm:ss.SSS", "<=") && r.get(4).equals("N")) {
                //工作日期要算昨天的
                r.set(4, batchGetter.getStDateDayStrAdd(workDt, -1, "-"));
                return new Tuple2<String, ArrayList<String>>(t._1, r);
            } else {
                //如果不是凌晨扫描的则不需要修正
                return t;
            }
        }).filter(r -> {
            return r != null;
        }).mapToPair(new PairFunction<Tuple2<String, ArrayList<String>>, String, Long>() {
            public Tuple2<String, Long> call(Tuple2<String, ArrayList<String>> r) throws Exception {
                return new Tuple2(r._1, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long c1, Long c2) throws Exception {
                return c1 + c2;
            }
        });

        JavaRDD<DsnDayOutPut> dayOutPutRDD = everyDayOutPutCountsRDD.map(r -> {
            String[] orgSplit = r._1.split(",");
            //"SiteCode", "LevelCode", "PlantCode", "ProcessCode", "Key", "WorkDT"      QTY
            //"SiteCode", "LevelCode", "Key", "WorkDT", "WorkorderType"                    QTY
            return new DsnDayOutPut(orgSplit[0], orgSplit[1], "", "", orgSplit[4],orgSplit[2], orgSplit[3], r._2);

        });


        /*
         * ====================================================================
         *  约当系数表
         * ====================================================================
         */
        Scan normalization_scan = new Scan();
        normalization_scan.withStartRow("0".getBytes(), true);
        normalization_scan.withStopRow("z".getBytes(), true);
        JavaRDD<Result> manual_normalization_Rdd = DPHbase.rddRead("dpm_ods_manual_normalization", normalization_scan, true);
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualNormalization(r.get(0).trim(), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)));
        }).distinct();

        System.out.println("manualNormalization========================>>>");


        /*
         * ====================================================================
         *  人力工时
         * ====================================================================
         */
        System.out.println(batchGetter.getStDateDayAdd(-1));
        System.out.println(batchGetter.getStDateDayAdd(0));
        //读取日工时数据
        //JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1), batchGetter.getStDateDayAdd(0), true);
        Scan manual_hour_scan = new Scan();
        manual_hour_scan.withStartRow(batchGetter.getStDateDayAdd(-1).getBytes(), true);
        manual_hour_scan.withStopRow(batchGetter.getStDateDayAdd(0).getBytes(), true);
        JavaRDD<Result> manual_hour_rdd = DPHbase.rddRead("dpm_ods_manual_manhour", manual_hour_scan, true);

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualHour(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), batchGetter.formatDouble(r.get(7)), batchGetter.formatDouble(r.get(8)), batchGetter.formatDouble(r.get(9)), batchGetter.formatDouble(r.get(10)));
        }).distinct();

        System.out.println("manualHour========================>>>");


        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(dayOutPutRDD, DsnDayOutPut.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);

        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_dsn_day_output");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_ods_manual_normalization");

        sqlContext.sql("select * from dpm_ods_manual_manhour limit 10").show();
        sqlContext.sql("select * from dpm_ods_manual_normalization limit 10").show();
        sqlContext.sql("select * from dpm_dws_dsn_day_output limit 10").show();


        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("dpm_dws_dsn_day_output_calculate.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));

        rowDataset.show();


        JavaRDD<Put> calculatedOutPutRDD = rowDataset.toJavaRDD().mapPartitions(it -> {
            ArrayList<Put> puts = new ArrayList<>();
            while (it.hasNext()) {
                puts.add(MetaGetter.getBeanGetter().getPut("dpm_dws_production_output_day", "DPM_DWS_PRODUCTION_OUTPUT_DAY", it.next()));
            }
            return puts.iterator();
        });

        List<Put> take = calculatedOutPutRDD.take(10);
        for (Put put : take) {
            System.out.println(put);
        }

        Test.rddWrite("dpm_dws_production_output_day", calculatedOutPutRDD);

        System.out.println("==============================>>>Programe End<<<==============================");
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
