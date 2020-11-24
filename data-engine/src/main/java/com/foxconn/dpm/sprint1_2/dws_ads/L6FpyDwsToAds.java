package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.FpyPassStationDay;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.FpyRepairStationDay;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author xs
 * @date 2020/7/14 12:00
 * <p>
 * CQ L6FPY 一次良率
 * 输入表： 抽取dws表 dpm_dws_production_pass_station_dd和dpm_dws_production_repair_station_dd
 * 输出表：放入ads表 dpm_ads_quality_fpy_day/week/month/quarter/year  L6FPY
 * ICT良率=1-(ICT repair 數據+SICTrepair 數據)/ PTH生產數據*100%
 * FCT良率 =1-(AV repair 數據+DDC repair 數據+FBT repair 數據+FWDL repair數據+HDCP repair數據+MAC repair數據+OFF-LINE repair數據+OSD TEST repair數據+USBrepair 數據)/ PTH生產數據*100%
 * FPY=ICT良率*FCT良率
 */
public class L6FpyDwsToAds extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    String etl_time = String.valueOf(System.currentTimeMillis());

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        LoadKpiTarget.loadProductionTarget();
        loadPreData(batchGetter.getStDateDayStampAdd(-1, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateL6DayFpyDetail();

        LoadKpiTarget.loadProductionTarget();
        loadPreData(batchGetter.getStDateDayStampAdd(-1, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateL6DayFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        calculateL6WeekFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        calculateL6WeekFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1);
        calculateL6MonthFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        calculateL6MonthFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        calculateL6QuarterFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        calculateL6QuarterFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1);
        calculateL6YearFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        calculateL6YearFpy();
    }

    public void calculateL6DayFpyDetail() throws Exception {
        String sql = sqlGetter.Get("l6_day_fpy_ads_detail_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_detail_day", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_detail_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_detail_day"));
        clearTempTable();
    }

    public void calculateL6DayFpy() throws Exception {
        String sqlCq = sqlGetter.Get("l6_day_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);

        String sqlLog = sqlGetter.Get("l6_day_fpy_ads_log_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsLog = DPSparkApp.getSession().sqlContext().sql(sqlLog);
        System.out.println("==============================>>>QA CQ Start<<<==============================");
        try {
            //查詢Hbase原始表數據
//            DPSparkApp.getSession().sqlContext().sql("select * from fpyRepairStationDay").show(500);
//            DPSparkApp.getSession().sqlContext().sql("select * from fpyPassStationDay").show(500);
            dsCq.show();
            dsLog.show();
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA CQ End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_day", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_day"));
        clearTempTable();
    }

    public void calculateL6WeekFpy() throws Exception {
        String sqlCq = sqlGetter.Get("l6_week_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_week", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_week"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_week"));
        clearTempTable();
    }

    public void calculateL6MonthFpy() throws Exception {
        String sqlCq = sqlGetter.Get("l6_month_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_month", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_month"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_month"));
        clearTempTable();
    }

    public void calculateL6QuarterFpy() throws Exception {
        String sqlCq = sqlGetter.Get("l6_quarter_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_quarter", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_quarter"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_quarter"));
        clearTempTable();
    }

    public void calculateL6YearFpy() throws Exception {
        String sqlCq = sqlGetter.Get("l6_year_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_year", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_year"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_year"));
        clearTempTable();
    }

    /*
     * ====================================================================
     * 描述:
     *      dpm_dws_production_pass_station_dd
     *      dpm_dws_production_repair_station_dd
     *      临时表已经指定为CQ L6数据
     *      PACKING工站集合就是PTH数量
     * ====================================================================
     */
    public void loadPreData(String yesterdayStamp, String todayStamp) throws Exception {
        System.out.println(yesterdayStamp + "_" + todayStamp);

        JavaRDD<FpyPassStationDay> fpyPassStationDay = DPHbase.saltRddRead("dpm_dws_production_pass_station_dd", yesterdayStamp, todayStamp, new Scan(), true).filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "station_code", "total_count", "customer")
                    &&
                    "L6".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "level_code"))
                    &&
                    "CQ".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "site_code"))
                    &&
                    "PACKING".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "station_code"))
                    ;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "site_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "level_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "factory_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "process_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "area_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "line_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "sku"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "platform"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "customer"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "work_dt"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "work_shift"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "part_no"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "line_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "station_code")
            );
        }).reduceByKey((kv1, kv2) -> {

            return Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "update_dt"))
                    >
                    Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "update_dt"))
                    ?
                    kv1
                    :
                    kv2
                    ;
        }).map(t -> {
            return t._2;
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "station_code", "total_count", "customer");
        }).filter(r -> org.apache.commons.lang.StringUtils.isNotEmpty(r.get(7))).map(r -> {
            return new FpyPassStationDay(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7), r.get(8));
        });

        JavaRDD<FpyRepairStationDay> fpyRepairStationDay = DPHbase.saltRddRead("dpm_dws_production_repair_station_dd", yesterdayStamp, todayStamp, new Scan(), true).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "N/A",
                    batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD",
                            "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift", "sku", "part_no", "platform", "fail_station"
                    ).toArray(new String[0])
            );
        }).filter(t -> {
            return batchGetter.resultGetColumn(t._2, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "update_dt").matches("[\\d]{13}");
        }).reduceByKey((rv1, rv2) -> {
            return Long.valueOf(batchGetter.resultGetColumn(rv1, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "update_dt"))
                    >
                    Long.valueOf(batchGetter.resultGetColumn(rv2, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "update_dt"))
                    ?
                    rv1
                    :
                    rv2;
        }).map(t -> {
            return t._2;
        }).mapPartitions(b -> {
            ArrayList<ArrayList<String>> arrayLists = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            while (b.hasNext()) {
                arrayLists.add(beanGetter.resultGetConfDeftColumnsValues(b.next(), "dpm_dws_production_repair_station_dd", "DPM_DWS_PRODUCTION_REPAIR_STATION_DD"));
            }
            return arrayLists.iterator();
        }).map((ArrayList<String> r) -> {
            return new FpyRepairStationDay(r.get(0), r.get(1), r.get(5), r.get(10), r.get(6), r.get(7), r.get(11), r.get(12), r.get(13));
        });

        JavaRDD<FpyRepairStationDay> repairStation = fpyRepairStationDay.filter(r ->
                ("L6".equals(r.getLevel_code()) && "CQ".equals(r.getSite_code())));

        //创建sql表
        SparkSession session = DPSparkApp.getSession();
        Dataset fpyPassStationDayDs = session.createDataFrame(fpyPassStationDay, FpyPassStationDay.class);
        Dataset fpyRepairStationDayDs = session.createDataFrame(repairStation, FpyRepairStationDay.class);

        fpyPassStationDayDs.createOrReplaceTempView("fpyPassStationDay");
        fpyRepairStationDayDs.createOrReplaceTempView("fpyRepairStationDay");

        session.sqlContext().udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        session.sqlContext().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
    }

    public void clearTempTable() {
        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        sqlContext.dropTempTable("fpyPassStationDay");
        sqlContext.dropTempTable("fpyRepairStationDay");
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}