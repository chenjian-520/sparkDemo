package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsProductionOeeEquipmentLineDD;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsProductionOutputDD;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className L5ScheduleAdherence
 * @description TODO
 * @date 2020/5/15 14:53
 */
public class L5ScheduleAdherence extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
    String etl_time = String.valueOf(System.currentTimeMillis());

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {


        registerUDF();
        LoadKpiTarget.loadProductionTarget();
        //day  upph
        getDWSProductionOutput(batchGetter.getStDateDayStampAdd(-4, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        getL5ProductionPlanDay(batchGetter.getStDateDayStampAdd(-4, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calcluateDaySA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekStampAdd(-1, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        calcluateWeekSA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekStampAdd(0, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        calcluateWeekSA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthStampAdd(-1, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1);
        calcluateMonthSA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthStampAdd(0, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        calcluateMonthSA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterStampAdd(-1, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        calcluateQuarterSA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        calcluateQuarterSA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearStampAdd(-1, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1);
        calcluateYearSA();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearStampAdd(0, "-")._1);
        getDWSProductionOutput(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        getL5ProductionPlanDay(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        calcluateYearSA();

    }

    public void calcluateDaySA() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_L5DaySvheduleAdherence.sql").replace("$ETL_TIME$", etl_time));
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            sqlContext.sql("select * from dpm_ods_production_target_values ").show(500);
            sqlContext.sql("select * from dpm_dws_production_output_dd").show(4000);
            sqlContext.sql("select * from dpm_ods_production_planning_day").show(4000);
            resultRows.show(500);
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_day", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_schedule_adherence_day"), resultRows.schema());
        clearTable();
    }

    public void calcluateWeekSA() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_L5WeekSvheduleAdherence.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_week", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_schedule_adherence_week"), resultRows.schema());
        clearTable();
    }

    public void calcluateMonthSA() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_L5MonthSvheduleAdherence.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_month", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_schedule_adherence_month"), resultRows.schema());
        clearTable();
    }

    public void calcluateQuarterSA() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_L5QuarterSvheduleAdherence.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_quarter", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_schedule_adherence_quarter"), resultRows.schema());
        clearTable();
    }

    public void calcluateYearSA() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_L5YearSvheduleAdherence.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_year", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_schedule_adherence_year"), resultRows.schema());
        clearTable();
    }


    /*
     * ====================================================================
     * 描述:
     *      获取指定时间段（天）的DSN日产量
     *      dpm_dws_production_output_dd        DPM_DWS_PRODUCTION_OUTPUT_DD
     * ====================================================================
     */

    public void getDWSProductionOutput(String yesterdayStamp, String todayStamp) throws Exception {

        System.out.println(yesterdayStamp + "_" + todayStamp);

        //读取日产量数据
        JavaRDD<Result> dpmDwsProductionOutputDD = DPHbase.saltRddRead("dpm_dws_production_output_dd", yesterdayStamp, todayStamp, new Scan(), true);

        if (dpmDwsProductionOutputDD == null) {
            System.out.println("==========>>>>>>表空或者无数据<<<<<<<============");
            return;
        }

        JavaRDD<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDJavaRDD1 = dpmDwsProductionOutputDD.filter(result -> {
            //必须字段过滤
            return
                    (
                            "line".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
                                    &&
                                    "L5".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                    )
                            ||
                            (
                                    "L5".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                                            &&
                                            (
                                                    "UpLoad_Painting".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                                            ||
                                                            "UpLoad_Molding".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                                            ||
                                                            "UpLoad_Stamping".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                                            ||
                                                            "UpLoad_Assy".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                            )
                                            &&
                                            "process".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
                            )
                    ;

        }).mapPartitions(batchP -> {
            //时间范围过滤
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDS = new ArrayList<>();
            while (batchP.hasNext()) {
                Result next = batchP.next();
                if (batchGetter.getFilterRangeTimeStampHBeans(next, "DPM_DWS_PRODUCTION_OUTPUT_DD", "work_dt", "yyyy-MM-dd", Long.valueOf(yesterdayStamp), Long.valueOf(todayStamp))) {
                    dpmDwsProductionOutputDDS.add(batchGetter.<DpmDwsProductionOutputDD>getBeanDeftInit(new DpmDwsProductionOutputDD(), beanGetter.resultGetConfDeftColumnsValues(next, "dpm_dws_production_output_dd", "DPM_DWS_PRODUCTION_OUTPUT_DD")));
                }
            }
            return dpmDwsProductionOutputDDS.iterator();

        });


        JavaRDD<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDJavaRDD = dpmDwsProductionOutputDDJavaRDD1.mapToPair(new PairFunction<DpmDwsProductionOutputDD, String, DpmDwsProductionOutputDD>() {
            @Override
            public Tuple2<String, DpmDwsProductionOutputDD> call(DpmDwsProductionOutputDD v) throws Exception {
                return new Tuple2<String, DpmDwsProductionOutputDD>(batchGetter.getStrArrayOrg("=", "N/A",
                        v.getWork_dt(), v.getSite_code(), v.getLevel_code(), v.getFactory_code(), v.getProcess_code(), v.getArea_code(), v.getLine_code(), v.getPart_no(), v.getSku(), v.getPlatform(), v.getWorkorder_type(), v.getCustomer(), v.getWork_shift()
                ), v);
            }
        }).reduceByKey((v1, v2) -> {
            return v1.getUpdate_dt() > v2.getUpdate_dt() ? v1 : v2;
        }).map(t -> {
            //process_code 4 site_code 1 data_granularity 14
            if ("process".equals(t._2.getData_granularity()) && "L5".equals(t._2.getLevel_code()) && t._2.getProcess_code().matches("^.+_.+$")) {
                t._2.setProcess_code(t._2.getProcess_code().split("_")[1]);
            }
            if ("line".equals(t._2.getData_granularity()) && "L5".equals(t._2.getLevel_code()) && "WH".equals(t._2.getSite_code())) {
                t._2.setProcess_code("Assy");
            }

            String processCode = t._2.getProcess_code();
            try {
                switch (processCode) {
                    case "塗裝":
                    case "涂装":
                        processCode = "Painting";
                        break;
                    case "成型":
                        processCode = "Molding";
                        break;
                    case "衝壓":
                    case "沖壓":
                        processCode = "Stamping";
                        break;
                    case "组装":
                    case "組裝":
                        processCode = "Assy";
                        break;
                }
            } catch (Exception e) {
                processCode = "N/A";
            }
            t._2.setProcess_code(processCode);

            return t._2;
        });


        try {
            System.out.println(dpmDwsProductionOutputDDJavaRDD.count());
        } catch (Exception e) {

        }
        System.out.println("==============================>>>dpmDwsProductionOutputDDJavaRDD End<<<==============================");
        sqlContext.createDataFrame(dpmDwsProductionOutputDDJavaRDD, DpmDwsProductionOutputDD.class).createOrReplaceTempView("dpm_dws_production_output_dd");
    }


    public void getL5ProductionPlanDay(String yesterdayStamp, String todayStamp) throws Exception {
        System.out.println(yesterdayStamp + "_" + todayStamp);

        JavaRDD<Result> filted = DPHbase.saltRddRead("dpm_ods_production_planning_day", yesterdayStamp, todayStamp, new Scan(), true).filter(r -> {
            return "L5".equals(batchGetter.resultGetColumn(r, "DPM_ODS_PRODUCTION_PLANNING_DAY", "level_code"));
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_PLANNING_DAY",
                            "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "work_dt", "work_shift", "customer", "key", "comment"
                    ).toArray(new String[0])
            );
        }).reduceByKey((kv1, kv2) -> {
            return
                    Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_ODS_PRODUCTION_PLANNING_DAY", "update_dt"))
                            >
                            Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_ODS_PRODUCTION_PLANNING_DAY", "update_dt"))
                            ?
                            kv1
                            :
                            kv2
                    ;
        }).map(t -> {
            return t._2;
        });

        try {
            System.out.println(filted.count());
        } catch (Exception e) {

        }
        System.out.println("==============================>>>getL5ProductionPlanDay End<<<==============================");

        JavaRDD<Row> rowJavaRDD = filted.mapPartitions(b -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (b.hasNext()) {
                ArrayList<String> r = beanGetter.resultGetConfDeftColumnsValues(b.next(), "dpm_ods_production_planning_day", "DPM_ODS_PRODUCTION_PLANNING_DAY");
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_planning_day", r));
            }
            return rows.iterator();
        });

        sqlContext.createDataFrame(rowJavaRDD, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_planning_day")).createOrReplaceTempView("dpm_ods_production_planning_day");

    }

    public void clearTable() {
        sqlContext.dropTempTable("dpm_ods_production_planning_day");
        sqlContext.dropTempTable("dpm_dws_production_output_dd");
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }

    public void registerUDF() {
        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);

    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
