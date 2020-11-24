package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.L10UphActualOutput;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.UphLineInfoDay;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.UphPartnoOutput;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;

/*
 * ====================================================================
 * 描述:
 *
 *      dpm_dws_personnel_overview_dd
 *              dws process_code : ASSEMBLY1       data_granularity: process
 *
 * ====================================================================
 */
public class L10UphactualCalculateSprintThree extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter metaGetter = MetaGetter.getSql();
    String etl_time = String.valueOf(System.currentTimeMillis());
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        LoadKpiTarget.loadProductionTarget();
        loadPreData(batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayStampAdd(-1, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateDayUph();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateWeekAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        calculateWeekUph();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateWeekAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        calculateWeekUph();


        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateMonthAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1);
        calculateMonthUph();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateMonthAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        calculateMonthUph();


        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateQuarterAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        calculateQuarterUph();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateQuarterAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        calculateQuarterUph();


        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateYearAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1);
        calculateYearUph();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateYearAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        calculateYearUph();

    }

    public void calculateDayUph() throws Exception {
        SparkSession session = DPSparkApp.getSession();

        Dataset<Row> ds = session.sql(metaGetter.Get("sprint_three_l10_day_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            /*DPSparkApp.getSession().sql("select * from lineInfoDay").show(2000);
            DPSparkApp.getSession().sql("select * from uphPartnoDay").show(2000);*/
            ds.show(50);
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_day", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_day",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_day"));
        clearTempTable();
    }

    public void calculateWeekUph() throws Exception {
        SparkSession session = DPSparkApp.getSession();

        Dataset<Row> ds = session.sql(metaGetter.Get("sprint_three_l10_week_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_week", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_week",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_week"));
        clearTempTable();
    }

    public void calculateMonthUph() throws Exception {
        SparkSession session = DPSparkApp.getSession();
        Dataset<Row> ds = session.sql(metaGetter.Get("sprint_three_l10_month_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_month", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_month",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_month"));
        clearTempTable();
    }

    public void calculateQuarterUph() throws Exception {
        SparkSession session = DPSparkApp.getSession();
        Dataset<Row> ds = session.sql(metaGetter.Get("sprint_three_l10_quarter_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_quarter", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_quarter",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_quarter"));
        clearTempTable();
    }

    public void calculateYearUph() throws Exception {
        SparkSession session = DPSparkApp.getSession();
        Dataset<Row> ds = session.sql(metaGetter.Get("sprint_three_l10_year_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_year", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_year",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_year"));
        clearTempTable();
    }


    public void loadPreData(String yesterday, String yesterdayStamp, String tomorrow) throws Exception {
        /**
         * 取数据
         * dpm_dws_production_partno_day(加盐取当天)和dpm_dws_production_line_info_day(不加盐)
         */
        JavaRDD<UphPartnoOutput> uphPartnoDay = DPHbase.saltRddRead("dpm_dws_production_partno_dd", yesterdayStamp, tomorrow, new Scan(), true).filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_PARTNO_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "output_qty", "ct", "update_dt", "update_by")
                   &&
                    "L10".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PARTNO_DD", "level_code"))
                    &&
                    (
                            //ASSEMBLY1 制程 + STORAGE-PALLET 标准ct
                            (
                                    "process".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PARTNO_DD", "data_granularity"))
                                            &&
                                            "ASSEMBLY1".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PARTNO_DD", "process_code"))
                            )
                                    ||
                                    (
                                            "line".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PARTNO_DD", "data_granularity"))
                                    )
                    )
                    ;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg("=", "-", batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_PARTNO_DD",
                    "site_code",
                    "level_code",
                    "factory_code",
                    "process_code",
                    "area_code",
                    "line_code",
                    "part_no",
                    "platform",
                    "work_dt",
                    "work_shift",
                    "customer"
                    ).toArray(new String[0])
            );
        }).reduceByKey((kv1, kv2) -> {
            return Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_DWS_PRODUCTION_PARTNO_DD", "update_dt")) > Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWS_PRODUCTION_PARTNO_DD", "update_dt")) ? kv1 : kv2;
        }).map(t -> {
            return t._2;
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_PARTNO_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "output_qty", "ct", "update_dt", "update_by", "data_from", "data_granularity", "process_code", "customer", "area_code");
        }).filter(r -> {
            return batchGetter.dateStrCompare(r.get(4), yesterday, "yyyy-MM-dd", ">=");
        }).filter(r -> StringUtils.isNotEmpty(r.get(7))).map(r -> {
            //此处进行对应产灵标准ct相乘， 因为这里的ct只是该机种的单位ct
            return new UphPartnoOutput(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), String.valueOf(Float.valueOf(r.get(7)) * Float.valueOf(r.get(6))), r.get(8), r.get(9), r.get(10), r.get(11), r.get(12), r.get(13), r.get(14));
        });

        System.out.println(uphPartnoDay.count());

        JavaRDD<UphLineInfoDay> lineInfoDay = DPHbase.rddRead("dpm_dws_production_line_info_dd", new Scan()).filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_LINE_INFO_DD", "site_code", "level_code", "line_code", "work_dt", "work_shift", "work_time", "update_dt", "update_by", "data_from");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_LINE_INFO_DD", "site_code", "level_code", "line_code", "work_dt", "work_shift", "work_time", "update_dt", "update_by", "data_from", "factory_code", "process_code", "area_code", "customer");
        }).filter(r -> {
            return batchGetter.dateStrCompare(r.get(3), yesterday, "yyyy-MM-dd", ">=");
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-", r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(9), r.get(10), r.get(11), r.get(12));
        }).reduceByKey((kv1, kv2) -> {
            //去重
            return Long.valueOf(kv1.get(6)) > Long.valueOf(kv2.get(6)) ? kv1 : kv2;
        }).map(t -> {
            return t._2;
        }).map(r -> {
            //work_time 毫秒转换为秒
            float seconds = Float.valueOf(Long.valueOf(r.get(5))) / 1000;
            return new UphLineInfoDay(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), String.valueOf(seconds), r.get(6), r.get(7), r.get(8), r.get(11), r.get(12));
        });

        System.out.println(lineInfoDay.count());

        //创建sql表
        SparkSession session = DPSparkApp.getSession();
        Dataset uphPartnoDayDs = session.createDataFrame(uphPartnoDay, UphPartnoOutput.class).where(new Column("level_code").contains("L10"));
        Dataset lineInfoDayDs = session.createDataFrame(lineInfoDay, UphLineInfoDay.class);
        uphPartnoDayDs.createOrReplaceTempView("uphPartnoDay");
        lineInfoDayDs.createOrReplaceTempView("lineInfoDay");

        session.sqlContext().udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        session.sqlContext().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
    }

    public void clearTempTable() {
        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        sqlContext.dropTempTable("uphPartnoDay");
        sqlContext.dropTempTable("lineInfoDay");
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
