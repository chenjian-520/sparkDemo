package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.FpyOutPutDay;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Map;

/**
 * @version 1.0
 * @program: ehr->L10FpyCalculateSprintThree
 * @description: ADS層dpm_ads_quality_fpy_day
 * HP FPY =((PT input Qty-PT NG Qty)/PT input Qty）*（(RUN-IN input Qty-Runin NG Qty)/Runin input Qty）
 * Lenovo_fpy =（Testing input qty  - Testing ng qty)/Testing input qty
 * L10_fpy =(HP 產量*HP FPY+Lenovo 產量*Lenovo FPY)/(HP產量+ Lenovo 產量)
 * <p>
 * PT input Qty=dpm_dws_production_pass_station_day表中station_code（PRETEST）的total count
 * PT NG Qty=dpm_dws_production_repair_station_day表中station_code（PRETEST）的total count
 * HP 產量=dpm_dws_production_output_day表中customer=HP，output_qty的值
 * <p>
 * RUN-IN input Qty=dpm_dws_production_pass_station_day表中station_code（POST RUNIN）的total count
 * Runin NG Qty=dpm_dws_production_repair_station_day表中station_code（POST RUNIN）的total count
 * HP 產量=dpm_dws_production_output_day表中customer=HP，output_qty的值
 * <p>
 * Testing input Qty=dpm_dws_production_pass_station_day表中station_code（Testing）的total count
 * Testing NG Qty=dpm_dws_production_repair_station_day表中station_code（Testing）的total count
 * Lenovo 產量=dpm_dws_production_output_day表中customer=Lenovo，output_qty的值
 * @author: Axin
 * @create: 2020-01-16 08:12
 **/
public class L10FpyCalculate extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    String etl_time = String.valueOf(System.currentTimeMillis());

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {


//        String work_dt = ((String) map.get("work_dt"));
//        if (work_dt != null){
//            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//            LoadKpiTarget.loadProductionTarget(work_dt);
//            loadPreData(String.valueOf(format.parse(work_dt).getTime()), String.valueOf(format.parse(batchGetter.getStDateDayStrAdd(work_dt, 1, "-")).getTime()));
//            calculateL10DayFpy();
//            return;
//        }

        LoadKpiTarget.loadProductionTarget();
        loadPreData(batchGetter.getStDateDayStampAdd(-1, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateL10DayFpyDetail();

        LoadKpiTarget.loadProductionTarget();
        loadPreData(batchGetter.getStDateDayStampAdd(-1, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateL10DayFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        calculateL10WeekFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        calculateL10WeekFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1);
        calculateL10MonthFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        calculateL10MonthFpy();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        calculateL10QuarterFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        calculateL10QuarterFpy();


        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1);
        calculateL10YearFpy();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        calculateL10YearFpy();
    }

    public void calculateL10DayFpyDetail() throws Exception {
        String sql = sqlGetter.Get("l10_day_fpy_ads_detail.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_detail_day", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_detail_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_detail_day"));
        clearTempTable();
    }

    public void calculateL10DayFpy() throws Exception {
        //WH L10
        String sql = sqlGetter.Get("l10_day_fpy_ads.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            DPSparkApp.getSession().sqlContext().sql("select * from fpyPassStationDay").show(50);
            ds.show(50);
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_day", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_day"));


        //CQ L10
        String sqlCq = sqlGetter.Get("l10_day_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);

        String sqlLog = sqlGetter.Get("l10_day_fpy_ads_log_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsLog = DPSparkApp.getSession().sqlContext().sql(sqlLog);
        System.out.println("==============================>>>QA Log CQ Start<<<==============================");
        try{
            //查詢Hbase原始表數據
//            DPSparkApp.getSession().sqlContext().sql("select * from fpyRepairStationDay").show(5000);
//            DPSparkApp.getSession().sqlContext().sql("select * from fpyPassStationDay").show(50000);
            dsCq.show();
            dsLog.show(60);
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log CQ End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_day", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_day"));
        clearTempTable();
    }

    public void calculateL10WeekFpy() throws Exception {
        String sql = sqlGetter.Get("l10_week_fpy_ads.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_week", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_week"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_week"));

        //CQ L10
        String sqlCq = sqlGetter.Get("l10_week_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_week", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_week"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_week"));
        clearTempTable();
    }

    public void calculateL10MonthFpy() throws Exception {
        String sql = sqlGetter.Get("l10_month_fpy_ads.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_month", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_month"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_month"));

        //CQ L10
        String sqlCq = sqlGetter.Get("l10_month_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_month", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_month"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_month"));
        clearTempTable();
    }

    public void calculateL10QuarterFpy() throws Exception {
        String sql = sqlGetter.Get("l10_quarter_fpy_ads.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_quarter", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_quarter"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_quarter"));

        //CQ L10
        String sqlCq = sqlGetter.Get("l10_quarter_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_quarter", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_quarter"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_quarter"));
        clearTempTable();
    }

    public void calculateL10YearFpy() throws Exception {
        String sql = sqlGetter.Get("l10_year_fpy_ads.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> ds = DPSparkApp.getSession().sqlContext().sql(sql);
        ds.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_year", ds.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_year"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_year"));

        //CQ L10
        String sqlCq = sqlGetter.Get("l10_year_fpy_ads_cq.sql").replace("$ETL_TIME$", etl_time);
        Dataset<Row> dsCq = DPSparkApp.getSession().sqlContext().sql(sqlCq);
        dsCq.show();
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_year", dsCq.javaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_year"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_year"));
        clearTempTable();
    }

    /*
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
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "station_code", "total_count", "customer")
            &&
            "L10".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "level_code"))
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
        }).filter(r -> StringUtils.isNotEmpty(r.get(7))).map(r -> {
            return new FpyPassStationDay(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7), r.get(8));
        });

        JavaRDD<FpyRepairStationDay> fpyRepairStationDay = DPHbase.saltRddRead("dpm_dws_production_repair_station_dd", yesterdayStamp, todayStamp, new Scan(), true).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "N/A",
                    batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_REPAIR_STATION_DD",
                            "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift", "sku", "part_no", "platform", "fail_station"
                    ).toArray(new String[0])
            );
        }).filter(t->{
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

        JavaRDD<FpyOutPutDay> fpyOutPutDay = DPHbase.saltRddRead("dpm_dws_production_output_dd", yesterdayStamp, todayStamp, new Scan(), true).filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "output_qty", "customer")
                    &&
                    "line".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
                    &&
                    "L10".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                    ;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "site_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "line_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "platform"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "work_dt"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "work_shift"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "area_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "sku"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "workorder_type"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "part_no"),
                    batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "customer")

            );
        }).reduceByKey((kv1, kv2) -> {
            return
                    Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_DWS_PRODUCTION_OUTPUT_DD", "update_dt"))

                            >
                            Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWS_PRODUCTION_OUTPUT_DD", "update_dt"))
                            ?
                            kv1
                            :
                            kv2
                    ;
        }).map(t -> {
            return t._2;
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "site_code", "level_code", "line_code", "platform", "work_dt", "work_shift", "output_qty", "customer");
        }).filter(r -> StringUtils.isNotEmpty(r.get(7))).map(r -> {
            return new FpyOutPutDay(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7));
        });

        //创建sql表
        SparkSession session = DPSparkApp.getSession();
        Dataset fpyPassStationDayDs = session.createDataFrame(fpyPassStationDay, FpyPassStationDay.class);
        Dataset fpyRepairStationDayDs = session.createDataFrame(fpyRepairStationDay, FpyRepairStationDay.class);
        Dataset fpyOutPutDayDs = session.createDataFrame(fpyOutPutDay, FpyOutPutDay.class);

        fpyOutPutDayDs.createOrReplaceTempView("fpyOutPutDay");
        fpyPassStationDayDs.createOrReplaceTempView("fpyPassStationDay");
        fpyRepairStationDayDs.createOrReplaceTempView("fpyRepairStationDay");


        session.sqlContext().udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        session.sqlContext().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
    }

    public void clearTempTable() {
        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        sqlContext.dropTempTable("fpyOutPutDay");
        sqlContext.dropTempTable("fpyPassStationDay");
        sqlContext.dropTempTable("fpyRepairStationDay");
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
