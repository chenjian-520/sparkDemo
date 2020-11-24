package com.foxconn.dpm.sprint5.dws_ads;

import com.foxconn.dpm.DpMysql;
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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 *  @program: ehr->L10FpyCalculateSprintThree
 *  @description:
 *  ADS層dpm_ads_quality_fpy_day
 *  HP FPY =((PT input Qty-PT NG Qty)/PT input Qty）*（(RUN-IN input Qty-Runin NG Qty)/Runin input Qty）
 *  Lenovo_fpy =（Testing input qty  - Testing ng qty)/Testing input qty
 *  L10_fpy =(HP 產量*HP FPY+Lenovo 產量*Lenovo FPY)/(HP產量+ Lenovo 產量)
 *  <p>
 *  PT input Qty=dpm_dws_production_pass_station_day表中station_code（PRETEST）的total count
 *  PT NG Qty=dpm_dws_production_repair_station_day表中station_code（PRETEST）的total count
 *  HP 產量=dpm_dws_production_output_day表中customer=HP，output_qty的值
 *  <p>
 *  RUN-IN input Qty=dpm_dws_production_pass_station_day表中station_code（POST RUNIN）的total count
 *  Runin NG Qty=dpm_dws_production_repair_station_day表中station_code（POST RUNIN）的total count
 *  HP 產量=dpm_dws_production_output_day表中customer=HP，output_qty的值
 *  <p>
 *  Testing input Qty=dpm_dws_production_pass_station_day表中station_code（Testing）的total count
 *  Testing NG Qty=dpm_dws_production_repair_station_day表中station_code（Testing）的total count
 *  Lenovo 產量=dpm_dws_production_output_day表中customer=Lenovo，output_qty的值
 *  @author: cj
 *  @create: 2020-06-24
 *
 *  输入表： dpm_dws_production_repair_station_dd  dpm_dws_production_pass_station_dd
 *  输出表： dpm_ads_quality_fpy_day/week/month/quarter/year
 **/
public class L10FpyCalculateSprintFourToLine extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();


    @Override
    public void scheduling(Map<String, Object> map) throws Exception {


        LoadKpiTarget.loadProductionTarget();
        loadPreData(batchGetter.getStDateDayStampAdd(-1, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateL10DayFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        calculateL10WeekFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        calculateL10WeekFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        calculateL10MonthFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        calculateL10QuarterFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        calculateL10YearFpy();
    }


    public void calculateL10DayFpy() throws Exception {
        String sql = sqlGetter.Get("sprint_five_l10_fpy_day_ads.sql");
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);

        DPSparkApp.getSession().sql("SELECT\n" +
                "        site_code,\n" +
                "        level_code,\n" +
                "        line_code,\n" +
                "        work_dt,\n" +
                "        customer,\n" +
                "        fpy_mark,\n" +
                "\t\tinput_qty,\n" +
                "\t\tng_qty,\n" +
                "       ((nvl(input_qty, 0) - nvl(ng_qty, 0)) / nvl(input_qty, 0)) fpy_count\n" +
                "\n" +
                "      FROM\n" +
                "        (\n" +
                "          SELECT\n" +
                "            site_code,\n" +
                "            level_code,\n" +
                "            line_code,\n" +
                "            work_dt,\n" +
                "            customer,\n" +
                "            fpy_mark,\n" +
                "            sum(input_qty) input_qty,\n" +
                "            sum(ng_qty) ng_qty\n" +
                "\n" +
                "          FROM\n" +
                "            (\n" +
                "\n" +
                "              SELECT\n" +
                "                t2.site_code,\n" +
                "                t2.level_code,\n" +
                "                t2.line_code,\n" +
                "                t2.work_dt,\n" +
                "                t2.customer,\n" +
                "                CASE\n" +
                "                WHEN t2.customer_mark = 'T_1'\n" +
                "                  THEN\n" +
                "                    'hp_pt_fpy'\n" +
                "                WHEN t2.customer_mark = 'T_2'\n" +
                "                  THEN\n" +
                "                    'hp_rt_fpy'\n" +
                "                WHEN t2.customer_mark = 'T_3'\n" +
                "                  THEN\n" +
                "                    'lenovo_fpy'\n" +
                "                END                  fpy_mark,\n" +
                "                nvl(t2.input_qty, 0) input_qty,\n" +
                "                nvl(t3.ng_qty, 0)    ng_qty\n" +
                "\n" +
                "              FROM\n" +
                "                (\n" +
                "                  SELECT\n" +
                "                    site_code,\n" +
                "                    level_code,\n" +
                "                    line_code,\n" +
                "                    work_dt,\n" +
                "                    customer,\n" +
                "                    CASE\n" +
                "                    WHEN customer = 'HP' AND station_code = 'PRETEST'\n" +
                "                      THEN 'T_1'\n" +
                "                    WHEN customer = 'HP' AND station_code = 'POST RUNIN'\n" +
                "                      THEN 'T_2'\n" +
                "                    WHEN customer = 'LENOVO' AND station_code = 'Testing'\n" +
                "                      THEN 'T_3'\n" +
                "                    ELSE 'T_4'\n" +
                "                    END                      customer_mark,\n" +
                "                    sum(nvl(total_count, 0)) input_qty\n" +
                "                  FROM\n" +
                "                    fpyPassStationDay\n" +
                "                  WHERE level_code = 'L10' AND site_code = 'WH'\n" +
                "                  GROUP BY\n" +
                "                    site_code,\n" +
                "                    level_code,\n" +
                "                    line_code,\n" +
                "                    work_dt,\n" +
                "                    customer,\n" +
                "                    station_code\n" +
                "\n" +
                "                ) t2\n" +
                "                LEFT JOIN\n" +
                "                (\n" +
                "\n" +
                "                  SELECT\n" +
                "                    site_code,\n" +
                "                    level_code,\n" +
                "                    line_code,\n" +
                "                    work_dt,\n" +
                "                    customer,\n" +
                "                    CASE\n" +
                "                    WHEN customer = 'HP' AND fail_station = 'PRETEST'\n" +
                "                      THEN 'T_1'\n" +
                "                    WHEN customer = 'HP' AND fail_station = 'POST RUNIN'\n" +
                "                      THEN 'T_2'\n" +
                "                    WHEN customer = 'LENOVO' AND fail_station = 'Testing'\n" +
                "                      THEN 'T_3'\n" +
                "                    ELSE 'T_4'\n" +
                "                    END    customer_mark,\n" +
                "                    fail_station,\n" +
                "                    nvl(sum(total_count), 0) ng_qty\n" +
                "                  FROM\n" +
                "                    fpyRepairStationDay\n" +
                "                  WHERE level_code = 'L10' AND site_code = 'WH'\n" +
                "                  GROUP BY\n" +
                "                    site_code,\n" +
                "                    level_code,\n" +
                "                    line_code,\n" +
                "                    work_dt,\n" +
                "                    customer,\n" +
                "                    fail_station\n" +
                "\n" +
                "                ) t3\n" +
                "                  ON\n" +
                "                    t2.work_dt = t3.work_dt AND\n" +
                "                    t2.site_code = t3.site_code AND\n" +
                "                    t2.level_code = t3.level_code AND\n" +
                "                    t2.line_code = t3.line_code AND\n" +
                "                    t2.customer = t3.customer AND\n" +
                "                    t2.customer_mark = t3.customer_mark\n" +
                "            ) tt\n" +
                "          GROUP BY\n" +
                "            site_code,\n" +
                "            level_code,\n" +
                "            line_code,\n" +
                "            work_dt,\n" +
                "            customer,\n" +
                "            fpy_mark\n" +
                "        ) ttt").show(40);


        System.out.println("==============================>>>QA Log Start<<<==============================");

        System.out.println("==============================>>>QA Log Start<<<==============================");

        ds/*.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row value) throws Exception {
                return "WH".equals(value.getString(0));
            }
        })*/.show(50);

        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_day", ds.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(ds.schema().toSeq()).asJava(), SaveMode.Append);
        clearTempTable();
    }

    public void calculateL10WeekFpy() throws Exception {
        String sql = sqlGetter.Get("sprint_five_l10_fpy_week_ads.sql");
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_week", ds.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(ds.schema().toSeq()).asJava(), SaveMode.Append);
        clearTempTable();
    }

    public void calculateL10MonthFpy() throws Exception {
        String sql = sqlGetter.Get("sprint_five_l10_fpy_month_ads.sql");
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_month", ds.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(ds.schema().toSeq()).asJava(), SaveMode.Append);
        clearTempTable();
    }

    public void calculateL10QuarterFpy() throws Exception {
        String sql = sqlGetter.Get("sprint_five_l10_fpy_quarter_ads.sql");
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_quarter", ds.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(ds.schema().toSeq()).asJava(), SaveMode.Append);
        clearTempTable();
    }

    public void calculateL10YearFpy() throws Exception {
        String sql = sqlGetter.Get("sprint_five_l10_fpy_year_ads.sql");
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_year", ds.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(ds.schema().toSeq()).asJava(), SaveMode.Append);
        clearTempTable();
    }

    /**
     * ====================================================================
     * 描述:
     *      dpm_dws_production_pass_station_dd
     *      dpm_dws_production_repair_station_dd
     *      dpm_dws_production_output_dd
     * ====================================================================
     */
    public void loadPreData(String yesterdayStamp, String todayStamp) throws Exception {

        System.out.println(yesterdayStamp + "_" + todayStamp);

        JavaRDD<FpyPassStationDay> fpyPassStationDay = DPHbase.saltRddRead("dpm_dws_production_pass_station_dd", yesterdayStamp, todayStamp, new Scan(), true).filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "station_code", "total_count", "customer");
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
                    >Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "update_dt"))
                    ?kv1:kv2;
        }).map(t -> {
            return t._2;
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "station_code", "total_count", "customer", "factory_code");
        }).filter(r -> {
            return StringUtils.isNotEmpty(r.get(7))
                    &&
                    "L10".equals(r.get(1))
                    ;
        }).map(r -> {
            return new FpyPassStationDay(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7), r.get(8));
        });


        for (FpyPassStationDay passStationDay : fpyPassStationDay.take(10)) {
            System.out.println(passStationDay);
        };


        System.out.println("==============================>>>fpyPassStationDay End<<<==============================");

        JavaRDD<FpyRepairStationDay> fpyRepairStationDay = DPHbase.saltRddRead("dpm_dws_production_repair_station_dd", yesterdayStamp, todayStamp, new Scan(), true).filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "fail_station", "total_count", "customer", "update_dt");
        }).filter(r->{
            return "L10".equals(batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "level_code"));
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "N/A",
                    batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift", "sku", "part_no", "platform", "fail_station").toArray(new String[0])
            );
        }).reduceByKey((rv1, rv2) -> {
            return Long.valueOf(batchGetter.resultGetColumn(rv1, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "update_dt"))
                    >Long.valueOf(batchGetter.resultGetColumn(rv2, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD", "update_dt"))
                    ?rv1:rv2;
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


        try {
            for (FpyRepairStationDay repairStationDay : fpyRepairStationDay.take(10)) {
                System.out.println(repairStationDay);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>fpyRepairStationDay End<<<==============================");


        //创建sql表
        SparkSession session = DPSparkApp.getSession();
        Dataset fpyPassStationDayDs = session.createDataFrame(fpyPassStationDay, FpyPassStationDay.class);
        Dataset fpyRepairStationDayDs = session.createDataFrame(fpyRepairStationDay, FpyRepairStationDay.class);

        fpyPassStationDayDs.createOrReplaceTempView("fpyPassStationDay");
        fpyPassStationDayDs.show();
        System.out.println("==============================>>>fpyPassStationDay End<<<==============================");

        fpyRepairStationDayDs.createOrReplaceTempView("fpyRepairStationDay");
        fpyRepairStationDayDs.show();
        System.out.println("==============================>>>fpyRepairStationDay End<<<==============================");


        session.sqlContext().udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        session.sqlContext().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
    }

    public void clearTempTable() {
        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        sqlContext.dropTempTable("fpyRepairStationDay");
        sqlContext.dropTempTable("fpyPassStationDay");
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
