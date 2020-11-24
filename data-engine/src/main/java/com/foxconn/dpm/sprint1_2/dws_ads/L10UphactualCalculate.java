package com.foxconn.dpm.sprint1_2.dws_ads;

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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;

/**
 * Description:  计算ADS的uph，只针对level_code=L10的数据
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/16
 */
public class L10UphactualCalculate extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter metaGetter = MetaGetter.getSql();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

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
        Dataset<Row> ds = sqlContext.sql(metaGetter.Get("l10_day_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            sqlContext.sql("select * from dpm_ods_production_target_values").show(500);
            sqlContext.sql("select * from uphPartnoDay").show(500);
            ds.show(50);
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_uph_adherence_day", ds.javaRDD(),
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_day",
                        DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                        DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_day"));
        clearTempTable();
    }

    public void calculateWeekUph() throws Exception {
        Dataset<Row> ds = sqlContext.sql(metaGetter.Get("l10_week_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.toDF().show(10);
        System.out.println("==============================>>>output End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_uph_adherence_week", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_week",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_week"));
        clearTempTable();
    }

    public void calculateMonthUph() throws Exception {
        Dataset<Row> ds = sqlContext.sql(metaGetter.Get("l10_month_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.toDF().show(10);
        System.out.println("==============================>>>output End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_uph_adherence_month", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_month",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_month"));
        clearTempTable();
    }

    public void calculateQuarterUph() throws Exception {
        Dataset<Row> ds = sqlContext.sql(metaGetter.Get("l10_quarter_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.toDF().show(10);
        System.out.println("==============================>>>output End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_uph_adherence_quarter", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_quarter",
                DataTypes.createStructField("ct_output_time", DataTypes.FloatType, true),
                DataTypes.createStructField("production_total_time", DataTypes.FloatType, true)

        ), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_uph_adherence_quarter"));
        clearTempTable();
    }

    public void calculateYearUph() throws Exception {
        Dataset<Row> ds = sqlContext.sql(metaGetter.Get("l10_year_uph_ads.sql").replace("$ETL_TIME$", etl_time));
        ds.toDF().show(10);
        System.out.println("==============================>>>output End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_uph_adherence_year", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_uph_adherence_year",
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
        System.out.println(yesterday + "_" + yesterdayStamp + "_" + tomorrow);
        JavaRDD<UphPartnoOutput> uphPartnoDay = DPHbase.saltRddRead("dpm_dws_production_partno_dd", yesterdayStamp, tomorrow, new Scan(), true).filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_PARTNO_DD", "site_code", "level_code", "work_dt", "output_qty", "update_dt")
                    &&
                    "line".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PARTNO_DD", "data_granularity"))
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
            return batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_PARTNO_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "output_qty", "ct", "update_dt", "update_by", "data_from", "area_code");
        }).map(r -> {
            //此处进行对应产灵标准ct相乘， 因为这里的ct只是该机种的单位ct
            return new UphPartnoOutput(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), String.valueOf(Float.valueOf(r.get(7)) * Float.valueOf(r.get(6))), r.get(8), r.get(9), r.get(10), r.get(11));
        }).cache();

        System.out.println(uphPartnoDay.count());
        System.out.println("==============================>>>uphPartnoDay End<<<==============================");


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
        }).cache();


        System.out.println(lineInfoDay.count());
        System.out.println("==============================>>>lineInfoDay End<<<==============================");

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
