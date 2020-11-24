package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.FtyByDayBean;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint3.dws_ads.bean.L6FpyLineBean;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;
import org.springframework.util.StringUtils;
import scala.*;
import scala.collection.Seq;

import java.lang.Double;
import java.lang.Float;
import java.lang.Long;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;

/**
 * @author wxj
 * @date 2020/1/14 17:29
 * <p>
 * L6FPY 一次良率
 * 输入表： 抽取dws表 dpm_dws_production_pass_station_dd 日partno Line工站過站表 (L6 FPY因子數據  L10 FPY因子數據)
 * 输出表：放入ads表 dpm_ads_quality_fpy_day/week/month/quarter/year  L6FPY
 */
public class FtyDwsToAdsSix extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    private Double total_count_smt = 0.0;
    private Double total_count_ft = 0.0;
    private Double total_count_ict = 0.0;

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //业务逻辑
        fty();
        //释放资源
        DPSparkApp.stop();
    }


    public void fty() throws Exception {
        //初始化时间
        String yesterday = batchGetter.getStDateDayAdd(-1, "-");
        String today = batchGetter.getStDateDayAdd(1, "-");
        String yearStamp = batchGetter.getStDateYearStampAdd(0)._1();
//        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
        String todayStamp = batchGetter.getStDateDayStampAdd(1);
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("2020-04-15"));
        scan.withStopRow(Bytes.toBytes(today));
        System.out.println("==============================>>>Programe Start<<<==============================");
        System.out.println(yesterday + ":" + today + ":");

        //year
        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                rddByYear1 = DPHbase.saltRddRead("dpm_dws_production_pass_station_dd", yearStamp, todayStamp, new Scan(), true).map(r -> {
            return new Tuple19<>(
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("site_code")))),//1
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("level_code")))),//2
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("factory_code")))),//3
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("process_code")))),//4
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("area_code")))),//5
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("line_code")))),//6
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("work_dt")))),//7
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("work_shift")))),//8
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("sku")))),//9
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("part_no")))),//10
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("platform")))),//11
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_code")))),//12
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_name")))),//13
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("fail_count")))),//14
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("total_count")))),//15
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("customer")))),//16
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("update_dt")))),//17
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("update_by")))),//18
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("data_from"))))//19
            );
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    r._1(), r._2(), r._3(), r._4(), r._5(), r._6(), r._9(), r._11(), r._16(), r._7(), r._8(), r._10(), r._12()
            );
        }).reduceByKey((kv1, kv2) -> {
            return Long.valueOf(kv1._17()) > Long.valueOf(kv2._17()) ? kv1 : kv2;
        }).map(t -> {
            return t._2;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        //WH L6维修数据
        JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>>
                repairRdd = DPHbase.saltRddRead("dpm_dwd_production_repair", yearStamp, todayStamp, new Scan(), true).map(r -> {
            return new Tuple10<>(
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("site_code")))),//1
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("level_code")))),//2
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("line_code")))),//3
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("work_dt")))),//4
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("customer")))),//5
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("sn")))),//6
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("repair_out_dt")))),//7
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("fail_station")))),//8
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("repair_code")))),//9
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_REPAIR"), Bytes.toBytes("update_dt"))))//10
            );
        }).filter(x -> {
            return "L6".equals(x._2()) && "WH".equals(x._1()) && !"".equals(x._7()) && !x._7().startsWith("-");
        }).keyBy(r -> {
            //sn + work_dt
            return r._6() + r._4();
        }).reduceByKey((v1, v2) -> {
            return Long.valueOf(v1._10()) > Long.valueOf(v2._10()) ? v1 : v2;
        }).map(t -> {
            return t._2;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> rddByYear = rddByYear1.filter(r ->
                (timeUtil().get(4) <= Integer.valueOf(r._7().replaceAll("-", "")))
                        && (timeUtil().get(5) >= Integer.valueOf(r._7().replaceAll("-", ""))) && "L6".equals(r._2()) && "WH".equals(r._1())
                        //SMT计入良率工站
                        && (r._12().startsWith("AOI INSPECT") || r._12().startsWith("SMT INSPECT") || "S_VI_T".equals(r._12())
                        //ICT计入良率工站
                        || r._12().startsWith("ICT_") || r._12().startsWith("P_ICT")
                        //FT计入良率工站
                        || r._12().startsWith("FT-") || r._12().startsWith("FBT"))
        );
        JavaRDD<L6FpyLineBean> l6FpyFilterRDD = rddByYear1.filter(r -> "L6".equals(r._2()) && "WH".equals(r._1()) & ("ICT REPAIR".equals(r._12()) || "FCT REPAIR".equals(r._12()) || "R_S_VI_T".equals(r._12()))).map(r -> {
            return new L6FpyLineBean(r._1(), r._2(), r._6(), r._12(), r._7(), r._16(), r._14(), r._15());
        });

        DPSparkApp.getSession().createDataFrame(l6FpyFilterRDD, L6FpyLineBean.class).createOrReplaceTempView("l6FpyFilterView");
        DPSparkApp.getSession().sql("select * from l6FpyFilterView").show();

        rddByYear.take(10).forEach(r -> System.out.println(r));
        String tableNameByYear = "dpm_ads_quality_fpy_year";
        StringBuffer queryByYear = new StringBuffer();

        //year
        Dataset<Row> datasetyear = DPSparkApp.getSession().sql("select level_code,station_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select customer,fail_count,level_code,line_code,site_code,station_code,total_count,year(work_dt) work_dt from l6FpyFilterView) where work_dt = year(from_unixtime((unix_timestamp()), 'yyyy-MM-dd')) group by level_code,station_code,work_dt");
        wucha(datasetyear);
        System.out.println(total_count_smt + "---" + total_count_ft + "---" + total_count_ict);
        //误判 add by chalcis 2020-7-21 15:52:41
        erroneousJudgement(tableNameByYear, repairRdd);
        //更新数据
        common(rddByYear, queryByYear, tableNameByYear);

        //Quarter
        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                rddByQuarter = rddByYear.filter(r ->
                (timeUtil().get(6) <= Integer.valueOf(r._7().replaceAll("-", "")))
                        && (timeUtil().get(7) >= Integer.valueOf(r._7().replaceAll("-", "")))
        );
        String tableNameByQuarter = "dpm_ads_quality_fpy_quarter";
        StringBuffer queryByQuarter = new StringBuffer();

        //Quarter
        Dataset<Row> datasetQuarter = DPSparkApp.getSession().sql("select level_code,station_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select customer,fail_count,level_code,line_code,site_code,station_code,total_count,cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt from l6FpyFilterView) where work_dt = cast(concat(year(from_unixtime((unix_timestamp()), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()), 'yyyy-MM-dd')) ) AS INTEGER) group by level_code,station_code,work_dt");
        wucha(datasetQuarter);
        System.out.println(total_count_smt + "---" + total_count_ft + "---" + total_count_ict);
        //误判 add by chalcis 2020-7-21 15:52:41
        erroneousJudgement(tableNameByQuarter, repairRdd);
        //更新数据
        common(rddByQuarter, queryByQuarter, tableNameByQuarter);


        //month
        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                rddByMonth = rddByYear.filter(r ->
                (timeUtil().get(0) <= Integer.valueOf(r._7().replaceAll("-", "")))
                        && (timeUtil().get(1) >= Integer.valueOf(r._7().replaceAll("-", "")))
        );
        String tableNameByMonth = "dpm_ads_quality_fpy_month";
        StringBuffer queryByMonth = new StringBuffer();
        //month
        Dataset<Row> datasetmonth = DPSparkApp.getSession().sql("select level_code,station_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select customer,fail_count,level_code,line_code,site_code,station_code,total_count,cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt from l6FpyFilterView) where work_dt = cast(from_unixtime((unix_timestamp()), 'yyyyMM') AS INTEGER) group by level_code,station_code,work_dt");
        wucha(datasetmonth);
        System.out.println(total_count_smt + "---" + total_count_ft + "---" + total_count_ict);
        //误判 add by chalcis 2020-7-21 15:52:41
        erroneousJudgement(tableNameByMonth, repairRdd);
        //更新数据
        common(rddByMonth, queryByMonth, tableNameByMonth);

        //week
        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                rddByWeek = rddByYear.filter(r ->
                (timeUtil().get(2) <= Integer.valueOf(r._7().replaceAll("-", "")))
                        && (timeUtil().get(3) >= Integer.valueOf(r._7().replaceAll("-", "")))
        );
        String tableNameByWeek = "dpm_ads_quality_fpy_week";
        StringBuffer queryByWeek = new StringBuffer();
        //week
        //注册week的 udf函数
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
        Dataset<Row> datasetweek = DPSparkApp.getSession().sql("select level_code,station_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select customer,fail_count,level_code,line_code,site_code,station_code,total_count,calculateYearWeek(work_dt) work_dt from l6FpyFilterView) where work_dt = calculateYearWeek(from_unixtime((unix_timestamp()), 'yyyy-MM-dd')) group by level_code,station_code,work_dt");
        wucha(datasetweek);
        System.out.println(total_count_smt + "---" + total_count_ft + "---" + total_count_ict);
        //误判 add by chalcis 2020-7-21 15:52:41
        erroneousJudgement(tableNameByWeek, repairRdd);
        //更新数据
        common(rddByWeek, queryByWeek, tableNameByWeek);

        //day
        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String
                , String, String, String, String, String, String, String, String, String>>
                rddByDay = rddByYear.filter(r ->
                yesterday.equals(r._7())
        );
        String tableNameByDay = "dpm_ads_quality_fpy_day";
        StringBuffer queryByDay = new StringBuffer();

        //day
        Dataset<Row> datasetDay = DPSparkApp.getSession().sql("select level_code,station_code,work_dt,sum(total_count)-sum(fail_count) total_count from l6FpyFilterView where work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd') group by level_code,station_code,work_dt");
        wucha(datasetDay);
        System.out.println(total_count_smt + "---" + total_count_ft + "---" + total_count_ict);
        //误判 add by chalcis 2020-7-21 15:52:41
        erroneousJudgement(tableNameByDay, repairRdd);
        //更新数据
        common(rddByDay, queryByDay, tableNameByDay);
    }

    public void wucha(Dataset<Row> datasetDay) {
        total_count_smt = 0.0;
        total_count_ft = 0.0;
        total_count_ict = 0.0;
        datasetDay.createOrReplaceTempView("FpyLineFilterDayView");
        Dataset<Row> sql = DPSparkApp.getSession().sql("select * from FpyLineFilterDayView where station_code = 'R_S_VI_T' ");
        sql.show();
        Dataset<Row> sql1 = DPSparkApp.getSession().sql("select * from FpyLineFilterDayView where station_code = 'FCT REPAIR' ");
        sql1.show();
        Dataset<Row> sql2 = DPSparkApp.getSession().sql("select * from FpyLineFilterDayView where station_code = 'ICT REPAIR' ");
        sql2.show();

        if (sql.count() != 0) {
            Seq<Seq<String>> rows = sql.getRows(2, 0);
            total_count_smt = Double.valueOf(rows.apply(1).apply(3));
            System.out.println(total_count_smt);
        }

        if (sql1.count() != 0) {
            Seq<Seq<String>> rows1 = sql1.getRows(2, 0);
            total_count_ft = Double.valueOf(rows1.apply(1).apply(3));
            System.out.println(total_count_ft);
        }

        if (sql2.count() != 0) {
            Seq<Seq<String>> rows2 = sql2.getRows(2, 0);
            total_count_ict = Double.valueOf(rows2.apply(1).apply(3));
            System.out.println(total_count_ict);
        }
    }

    /***
     * WH L6误判
     * 逻辑见禅道
     * http://10.202.16.172/zentao/story-view-1710.html
     * @param tableName
     * @throws Exception
     */
    public void erroneousJudgement(String tableName, JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> inRdd) throws Exception {
        JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> repairRdd = null;
        if ("dpm_ads_quality_fpy_day".equals(tableName)) {
            String yesterday = batchGetter.getStDateDayAdd(-1, "-");
            repairRdd = inRdd.filter(r -> yesterday.equals(r._4())).persist(StorageLevel.MEMORY_AND_DISK());
        } else if ("dpm_ads_quality_fpy_week".equals(tableName)) {
            repairRdd = inRdd.filter(r -> timeUtil().get(2) <= Integer.valueOf(r._4().replaceAll("-", ""))
                    && timeUtil().get(3) >= Integer.valueOf(r._4().replaceAll("-", "")))
                    .persist(StorageLevel.MEMORY_AND_DISK());
        } else if ("dpm_ads_quality_fpy_month".equals(tableName)) {
            repairRdd = inRdd.filter(r -> timeUtil().get(0) <= Integer.valueOf(r._4().replaceAll("-", ""))
                    && timeUtil().get(1) >= Integer.valueOf(r._4().replaceAll("-", "")))
                    .persist(StorageLevel.MEMORY_AND_DISK());
        } else if ("dpm_ads_quality_fpy_quarter".equals(tableName)) {
            repairRdd = inRdd.filter(r -> timeUtil().get(6) <= Integer.valueOf(r._4().replaceAll("-", ""))
                    && timeUtil().get(7) >= Integer.valueOf(r._4().replaceAll("-", "")))
                    .persist(StorageLevel.MEMORY_AND_DISK());
        } else if ("dpm_ads_quality_fpy_year".equals(tableName)) {
            repairRdd = inRdd.filter(r -> timeUtil().get(4) <= Integer.valueOf(r._4().replaceAll("-", ""))
                    && timeUtil().get(5) >= Integer.valueOf(r._4().replaceAll("-", "")))
                    .persist(StorageLevel.MEMORY_AND_DISK());
        } else {
            return;
        }

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            repairRdd.take(10).forEach(x -> System.out.println(x));
        } catch (Exception e) {
        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        //DELL误判,x._5():customer,x._8():fail_station,x._9():repair_code
        Long count = repairRdd.filter(x -> "DELL".equalsIgnoreCase(x._5()) && "AOI INSPECT".equals(x._8()) && ("".equals(x._9()) || "GRR025".equals(x._9()))).count();
        total_count_smt += count;
        count = repairRdd.filter(x -> "DELL".equalsIgnoreCase(x._5()) && "ICT".equals(x._8()) && "GRR018".equals(x._9())).count();
        total_count_ict += count;
        count = repairRdd.filter(x -> "DELL".equalsIgnoreCase(x._5()) && "FT1".equals(x._8()) && ("".equals(x._9()) || "FT010".equals(x._9()))).count();
        total_count_ft += count;

        System.out.println("erroneousJudgement DELL之后，" + total_count_smt + "---" + total_count_ft + "---" + total_count_ict);

        //HP&LENOVO误判,誤掃描
        count = repairRdd.filter(x -> ("HP".equalsIgnoreCase(x._5()) || "LENOVO".equalsIgnoreCase(x._5()) || "LENOVO_CODE".equalsIgnoreCase(x._5())) && "R_S_VI_T".equals(x._8()) && "SVI139".equals(x._9())).count();
        total_count_smt += count;
        count = repairRdd.filter(x -> ("HP".equalsIgnoreCase(x._5()) || "LENOVO".equalsIgnoreCase(x._5()) || "LENOVO_CODE".equalsIgnoreCase(x._5())) && "R_P_ICT".equals(x._8()) && ("ICT086".equals(x._9())) || "ICT100".equals(x._9())).count();
        total_count_ict += count;
        count = repairRdd.filter(x -> ("HP".equalsIgnoreCase(x._5()) || "LENOVO".equalsIgnoreCase(x._5()) || "LENOVO_CODE".equalsIgnoreCase(x._5())) && "R_FBT".equals(x._8()) && ("RFT25".equals(x._9()) || "RFT43".equals(x._9()))).count();
        total_count_ft += count;

        System.out.println("erroneousJudgement HP&LENOVO之后，" + total_count_smt + "---" + total_count_ft + "---" + total_count_ict);
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }

    public static String emptyStrNull(String s) {
        if (StringUtils.isEmpty(s)) {
            return "";
        }
        return s;
    }

    public void common(JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                               rdd, StringBuffer sqlString, String tableName) throws Exception {
        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                map = rdd;
        //获取target
        JavaRDD<ArrayList<String>> arrayListJavaRDD = LoadKpiTarget.loadProductionTarget();
        JavaRDD<ArrayList<String>> filter = arrayListJavaRDD.filter(r -> "D".equals(r.get(1)) && "WH".equals(r.get(2)) &&
                "L6".equals(r.get(3)) && "all".equals(r.get(4)) &&
                "all".equals(r.get(5)) && "all".equals(r.get(6)) && "all".equals(r.get(7)));
        Float targetFloat = 0f;
        try {
            targetFloat = Float.valueOf(filter.take(1).get(0).get(16)) * 100;
        } catch (Exception ex) {
            targetFloat = 0f;
        }

        System.out.println(targetFloat);
        BatchGetter batchGetter = BatchGetter.getInstance();
        String day = batchGetter.getStDateDayAdd(1, "-");


        JavaRDD<Tuple3> map1 = map.groupBy(r -> {
            return new Tuple1(r._12());
        }).map(r -> {
            //总数
            int sum = 0;
            int fail = 0;
            Iterator<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                    iterator = r._2.iterator();
            while (iterator.hasNext()) {
                Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>
                        next = iterator.next();
                fail = fail + Integer.valueOf(next._14());
                sum = sum + Integer.valueOf(next._15());
            }
            return new Tuple3(r._1._1(), fail, sum);
        });

        JavaPairRDD<String, Tuple2> stringTuple2JavaPairRDD = map1.filter(r ->
                (!StringUtils.isEmpty(r._1()) && (r._1().toString().startsWith("AOI INSPECT")
                        || r._1().toString().startsWith("SMT INSPECT") || "S_VI_T".equals(r._1()))
                )
        ).mapToPair(new PairFunction<Tuple3, String, Tuple2>() {
            @Override
            public Tuple2<String, Tuple2> call(Tuple3 tuple4) throws Exception {
                return new Tuple2<String, Tuple2>("0", new Tuple2<>(tuple4._2(), tuple4._3()));
            }
        }).reduceByKey((a, b) -> {
            return new Tuple2<>(Integer.valueOf(a._1().toString()) + Integer.valueOf(b._1.toString()), Integer.valueOf(a._2.toString()) + Integer.valueOf(b._2.toString()));
        });
        Double smt = 0.0;
        Double ict = 0.0;
        Double ft = 0.0;
        Iterator<Tuple2<String, Tuple2>> iterator1 = stringTuple2JavaPairRDD.take(1).iterator();
        stringTuple2JavaPairRDD.take(10).forEach(r -> System.out.println(r));
        if (iterator1.hasNext()) {
            Tuple2<String, Tuple2> next1 = stringTuple2JavaPairRDD.take(1).iterator().next();
            //SMT良率
            smt = (Double.valueOf(next1._2()._2.toString()) - Double.valueOf(next1._2()._1.toString()) + Double.valueOf(total_count_smt)) / Double.valueOf(next1._2()._2.toString());
        }

        JavaPairRDD<String, Tuple2> stringTuple2JavaPairRDD1 = map1.filter(r ->
                (!StringUtils.isEmpty(r._1()) && (r._1().toString().startsWith("ICT_") || r._1().toString().startsWith("P_ICT")))
        ).mapToPair(new PairFunction<Tuple3, String, Tuple2>() {
            @Override
            public Tuple2<String, Tuple2> call(Tuple3 tuple4) throws Exception {
                return new Tuple2<String, Tuple2>("0", new Tuple2<>(tuple4._2(), tuple4._3()));
            }
        }).reduceByKey((a, b) -> {
            return new Tuple2<>(Integer.valueOf(a._1().toString()) + Integer.valueOf(b._1.toString()), Integer.valueOf(a._2.toString()) + Integer.valueOf(b._2.toString()));
        });

        Iterator<Tuple2<String, Tuple2>> iterator2 = stringTuple2JavaPairRDD1.take(1).iterator();
        stringTuple2JavaPairRDD1.take(10).forEach(r -> System.out.println(r));
        if (iterator2.hasNext()) {
            Tuple2<String, Tuple2> next = iterator2.next();
            ict = (Double.valueOf(next._2()._2.toString()) - Double.valueOf(next._2()._1.toString()) + Double.valueOf(total_count_ict)) / Double.valueOf(next._2()._2.toString());
        }

        JavaPairRDD<String, Tuple2> stringTuple2JavaPairRDD2 = map1.filter(r ->
                (!StringUtils.isEmpty(r._1()) && (r._1().toString().startsWith("FT-") || r._1().toString().startsWith("FBT")))
        ).mapToPair(new PairFunction<Tuple3, String, Tuple2>() {
            @Override
            public Tuple2<String, Tuple2> call(Tuple3 tuple4) throws Exception {
                return new Tuple2<String, Tuple2>("0", new Tuple2<>(tuple4._2(), tuple4._3()));
            }
        }).reduceByKey((a, b) -> {
            return new Tuple2<>(Integer.valueOf(a._1().toString()) + Integer.valueOf(b._1.toString()), Integer.valueOf(a._2.toString()) + Integer.valueOf(b._2.toString()));
        });

        Iterator<Tuple2<String, Tuple2>> iterator3 = stringTuple2JavaPairRDD2.take(1).iterator();
        stringTuple2JavaPairRDD2.take(10).forEach(r -> System.out.println(r));
        if (iterator3.hasNext()) {
            Tuple2<String, Tuple2> next = iterator3.next();
            ft = (Double.valueOf(next._2()._2.toString()) - Double.valueOf(next._2()._1.toString()) + Double.valueOf(total_count_ft)) / Double.valueOf(next._2()._2.toString());
        }


        //ft良率
        float fty = (float) (smt * ict * ft * 100);
        if (fty > 100) {
            fty = 97;
        }
        System.out.println(smt + ":" + ict + ":" + ft);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        String year = String.valueOf(calendar.get(Calendar.YEAR));

        Date date = new Date();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);//设置周一为一周的第一天
        calendar.setTime(date);
        int week = calendar.get(Calendar.WEEK_OF_YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;

        String quarter = " ";
        if (month >= 1 && month == 3) {
            quarter = "1";
        }
        if (month >= 4 && month <= 6) {
            quarter = "2";
        }
        if (month >= 7 && month <= 9) {
            quarter = "3";
        }
        if (month >= 10 && month <= 12) {
            quarter = "4";
        }

        SparkSession sqlContext = DPSparkApp.getSession();
        FtyByDayBean ftyBean = new FtyByDayBean();
        //实体类赋值
        ftyBean.setId(UUID.randomUUID().toString());
        ftyBean.setFpy_actual(fty);
        ftyBean.setFpy_target(targetFloat);
        ftyBean.setEtl_time(String.valueOf(System.currentTimeMillis()));
        ftyBean.setLevel_code("L6");
        ftyBean.setSite_code("WH");
        Date today = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = simpleDateFormat.format(today);
        ftyBean.setWork_date(yesterday);
        if (week < 10) {
            ftyBean.setWeek_id(year + "0" + week);
        } else {
            ftyBean.setWeek_id(year + week);
        }
        ftyBean.setQuarter_id(year + quarter);
        if (month < 10) {
            ftyBean.setMonth_id(year + "0" + month);
        } else {
            ftyBean.setMonth_id(year + month);
        }

        ftyBean.setYear_id(year);
        List<FtyByDayBean> ftyBeanList = new ArrayList<>();
        ftyBeanList.add(ftyBean);
        JavaRDD<FtyByDayBean> fpyRdd = DPSparkApp.getContext().parallelize(ftyBeanList);
        Dataset<Row> fpySet = sqlContext.createDataFrame(fpyRdd, FtyByDayBean.class);
        fpySet.show();

        HashMap<String, StructField> schemaList = new HashMap<>(16);
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        if ("dpm_ads_quality_fpy_day".equals(tableName)) {
            schemaList.put("work_date", DataTypes.createStructField("work_date", DataTypes.StringType, true));
        } else if ("dpm_ads_quality_fpy_week".equals(tableName)) {
            schemaList.put("week_id", DataTypes.createStructField("week_id", DataTypes.StringType, true));
        } else if ("dpm_ads_quality_fpy_month".equals(tableName)) {
            schemaList.put("month_id", DataTypes.createStructField("month_id", DataTypes.StringType, true));
        } else if ("dpm_ads_quality_fpy_quarter".equals(tableName)) {
            schemaList.put("quarter_id", DataTypes.createStructField("quarter_id", DataTypes.StringType, true));
        } else if ("dpm_ads_quality_fpy_year".equals(tableName)) {
            schemaList.put("year_id", DataTypes.createStructField("year_id", DataTypes.StringType, true));
        }
        schemaList.put("fpy_actual", DataTypes.createStructField("fpy_actual", DataTypes.FloatType, true));
        schemaList.put("fpy_target", DataTypes.createStructField("fpy_target", DataTypes.FloatType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        //测试注释
        DPMysql.commonOdbcWriteBatch("dp_ads", tableName, fpySet.toJavaRDD(), schemaList, fpySet.schema());

        System.out.println("==============================>>>Programe End<<<==============================");
    }


    private static List<Integer> timeUtil() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        List<Integer> list = new ArrayList<>();
        getCurrQuarter(format);
        Calendar currCal = Calendar.getInstance();
        int currentYear = currCal.get(Calendar.YEAR);
        String yearFirst = getYearFirst(currentYear, format);
        String yearLast = getYearLast(currentYear, format);

        // 获取当月第一天和最后一天
        String firstday, lastday;
        // 获取本月的第一天
        Calendar cale = Calendar.getInstance();
        cale.add(Calendar.MONTH, 0);
        cale.set(Calendar.DAY_OF_MONTH, 1);
        firstday = format.format(cale.getTime());
        // 获取本月的最后一天
        cale = Calendar.getInstance();
        cale.add(Calendar.MONTH, 1);
        cale.set(Calendar.DAY_OF_MONTH, 0);
        lastday = format.format(cale.getTime());

        //获取周开始基准
        Date date = new Date();
        String formatWeek = new SimpleDateFormat("yyyy-MM-dd").format(date);
        LocalDate inputDate = LocalDate.parse(formatWeek);
        // 所在周开始时间
        LocalDate beginDayOfWeek = inputDate.with(DayOfWeek.MONDAY);
        // 所在周结束时间
        LocalDate endDayOfWeek = inputDate.with(DayOfWeek.SUNDAY);
        list.add(Integer.valueOf(firstday));
        list.add(Integer.valueOf(lastday));
        list.add(Integer.valueOf(beginDayOfWeek.toString().replace("-", "")));
        list.add(Integer.valueOf(endDayOfWeek.toString().replace("-", "")));
        list.add(Integer.valueOf(yearFirst));
        list.add(Integer.valueOf(yearLast));
        list.add(Integer.valueOf(getCurrQuarter(format)[0]));
        list.add(Integer.valueOf(getCurrQuarter(format)[1]));
        return list;
    }

    public static String getYearFirst(int year, SimpleDateFormat format) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, year);
        Date currYearFirst = calendar.getTime();
        String yearStr = format.format(currYearFirst);
        return yearStr;
    }

    public static String getYearLast(int year, SimpleDateFormat format) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, year);
        calendar.roll(Calendar.DAY_OF_YEAR, -1);
        Date currYearLast = calendar.getTime();
        String yearStr = format.format(currYearLast);
        return yearStr;
    }

    public static String[] getCurrQuarter(SimpleDateFormat format) {
        Calendar c = Calendar.getInstance();
        int month = c.get(Calendar.MONTH) + 1;
        int quarter = 0;
        if (month >= 1 && month <= 3) {
            quarter = 1;
        } else if (month >= 4 && month <= 6) {
            quarter = 2;
        } else if (month >= 7 && month <= 9) {
            quarter = 3;
        } else {
            quarter = 4;
        }
        String[] s = new String[2];
        String str = "";
        // 设置本年的季
        Calendar quarterCalendar = null;
        switch (quarter) {
            case 1: // 本年到现在经过了一个季度，在加上前4个季度
                quarterCalendar = Calendar.getInstance();
                quarterCalendar.set(Calendar.MONTH, 3);
                quarterCalendar.set(Calendar.DATE, 1);
                quarterCalendar.add(Calendar.DATE, -1);
                str = format.format(quarterCalendar.getTime());
                s[0] = str.substring(0, str.length() - 4) + "0101";
                s[1] = str;
                break;
            case 2: // 本年到现在经过了二个季度，在加上前三个季度
                quarterCalendar = Calendar.getInstance();
                quarterCalendar.set(Calendar.MONTH, 6);
                quarterCalendar.set(Calendar.DATE, 1);
                quarterCalendar.add(Calendar.DATE, -1);
                str = format.format(quarterCalendar.getTime());
                s[0] = str.substring(0, str.length() - 4) + "0401";
                s[1] = str;
                break;
            case 3:// 本年到现在经过了三个季度，在加上前二个季度
                quarterCalendar = Calendar.getInstance();
                quarterCalendar.set(Calendar.MONTH, 9);
                quarterCalendar.set(Calendar.DATE, 1);
                quarterCalendar.add(Calendar.DATE, -1);
                str = format.format(quarterCalendar.getTime());
                s[0] = str.substring(0, str.length() - 4) + "0701";
                s[1] = str;
                break;
            case 4:// 本年到现在经过了四个季度，在加上前一个季度
                quarterCalendar = Calendar.getInstance();
                str = format.format(quarterCalendar.getTime());
                s[0] = str.substring(0, str.length() - 4) + "1001";
                s[1] = str.substring(0, str.length() - 4) + "1231";
                break;
        }
        return s;
    }


}
