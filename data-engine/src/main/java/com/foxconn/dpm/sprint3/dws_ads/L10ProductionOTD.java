package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.ProdPermissionManager;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.RDBConnetInfo;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author HS
 * @date 2020/5/20 12:19
 */
public class L10ProductionOTD extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        LoadKpiTarget.loadProductionTarget();
        loadPreData(batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(1, "-"));
        calculateL10DayProductionOTD();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateWeekAdd(-1, "-")._1, batchGetter.getStDateWeekAdd(0, "-")._1);
        calculateL10WeekProductionOTD();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateWeekAdd(0, "-")._1, batchGetter.getStDateWeekAdd(1, "-")._1);
        calculateL10WeekProductionOTD();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateMonthAdd(-1, "-")._1, batchGetter.getStDateMonthAdd(0, "-")._1);
        calculateL10MonthProductionOTD();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateMonthAdd(0, "-")._1, batchGetter.getStDateMonthAdd(1, "-")._1);
        calculateL10MonthProductionOTD();

        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateQuarterAdd(-1, "-")._1, batchGetter.getStDateQuarterAdd(0, "-")._1);
        calculateL10QuarterProductionOTD();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateQuarterAdd(0, "-")._1, batchGetter.getStDateQuarterAdd(1, "-")._1);
        calculateL10QuarterProductionOTD();


        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(-1, "-")._2);
        loadPreData(batchGetter.getStDateYearAdd(-1, "-")._1, batchGetter.getStDateYearAdd(0, "-")._1);
        calculateL10YearProductionOTD();
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(0, "-")._2);
        loadPreData(batchGetter.getStDateYearAdd(0, "-")._1, batchGetter.getStDateYearAdd(1, "-")._1);
        calculateL10YearProductionOTD();

    }

    public void calculateL10DayProductionOTD() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(
                "SELECT" +
                        "  concat(unix_timestamp(), '-', uuid()) id," +
                        "  work_dt                               work_date," +
                        "  site_code," +
                        "  level_code," +
                        "  'N/A'                                 factory_code," +
                        "  'N/A'                                 process_code," +
                        "  'N/A'                              customer_code," +
                        "  'N/A' plant_code," +
                        "  customer_pn                                  sku," +
                        "  cast(sum(nvl(ship_qty, 0)) as INTEGER)                ship_qty_actual," +
                        "  cast(sum(nvl(commit_ship_qty, 0)) as INTEGER)          ship_qty_target," +
                        "  cast(nvl(cast(sum(nvl(ship_qty, 0)) as FLOAT) / cast(sum(nvl(commit_ship_qty, 0)) as FLOAT) ,0) * 100 as FLOAT)  otd_actual," +
                        "  get_aim_target_by_key(concat_ws('=', 'D', site_code, level_code, 'all', 'all', 'all', 'all'), 13) * 100 otd_target, " +
                        "  cast(unix_timestamp() AS VARCHAR(32)) etl_time" +
                        "   FROM dpm_dws_production_otd  " +
                        "  GROUP BY  " +
                        "  work_dt," +
                        "  site_code," +
                        "  customer_pn," +
                        "  level_code"
        );
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            resultRows.toDF().show(100);
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_otd_day", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_otd_day"), resultRows.schema());
        clearTable();
    }

    public void calculateL10WeekProductionOTD() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(
                "SELECT" +
                        "  concat(unix_timestamp(), '-', uuid()) id," +
                        "  calculateYearWeek(work_dt) week_id," +
                        "  site_code," +
                        "  level_code," +
                        "  'N/A'                                 factory_code," +
                        "  'N/A'                                 process_code," +
                        "  'N/A'                              customer_code," +
                        "  'N/A'  plant_code," +
                        "  customer_pn                                 sku," +
                        "  cast(sum(nvl(ship_qty, 0)) as INTEGER)                ship_qty_actual," +
                        "  cast(sum(nvl(commit_ship_qty, 0)) as INTEGER)         ship_qty_target," +
                        "  cast(nvl(cast(sum(nvl(ship_qty, 0)) as FLOAT) / cast(sum(nvl(commit_ship_qty, 0)) as FLOAT) ,0) * 100 as FLOAT)  otd_actual," +
                        "  get_aim_target_by_key(concat_ws('=', 'D', site_code, level_code, 'all', 'all', 'all', 'all'), 13) * 100  otd_target, " +
                        "  cast(unix_timestamp() AS VARCHAR(32)) etl_time" +
                        "   FROM dpm_dws_production_otd   " +
                        "GROUP BY" +
                        "  calculateYearWeek(work_dt)," +
                        "  site_code," +
                        "  customer_pn," +
                        "  level_code"
        );
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_otd_week", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_otd_week"), resultRows.schema());
        clearTable();
    }

    public void calculateL10MonthProductionOTD() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(
                "SELECT" +
                        "  concat(unix_timestamp(), '-', uuid()) id," +
                        "  cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id," +
                        "  site_code," +
                        "  level_code," +
                        "  'N/A'                                 factory_code," +
                        "  'N/A'                                 process_code," +
                        "  'N/A'                              customer_code," +
                        "  'N/A' plant_code," +
                        "  customer_pn                                 sku," +
                        "  cast(sum(nvl(ship_qty, 0))  as INTEGER)               ship_qty_actual," +
                        "  cast(sum(nvl(commit_ship_qty, 0))  as INTEGER)        ship_qty_target," +
                        "  cast(nvl(cast(sum(nvl(ship_qty, 0)) as FLOAT) / cast(sum(nvl(commit_ship_qty, 0)) as FLOAT) ,0) * 100 as FLOAT)  otd_actual," +
                        "  get_aim_target_by_key(concat_ws('=', 'D', site_code, level_code, 'all', 'all', 'all', 'all'), 13) * 100  otd_target, " +
                        "  cast(unix_timestamp() AS VARCHAR(32)) etl_time" +
                        "   FROM dpm_dws_production_otd  " +
                        " GROUP BY  " +
                        "  cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER)," +
                        "  site_code," +
                        "  customer_pn," +
                        "  level_code"
        );
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_otd_month", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_otd_month"), resultRows.schema());
        clearTable();
    }

    public void calculateL10QuarterProductionOTD() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(
                "SELECT" +
                        "  concat(unix_timestamp(), '-', uuid()) id," +
                        "  cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) quarter_id," +
                        "  site_code," +
                        "  level_code," +
                        "  'N/A'                                 factory_code," +
                        "  'N/A'                                 process_code," +
                        "  'N/A'                              customer_code," +
                        "  'N/A' plant_code," +
                        "  customer_pn                                 sku," +
                        "  cast(sum(nvl(ship_qty, 0))     as INTEGER)            ship_qty_actual," +
                        "  cast(sum(nvl(commit_ship_qty, 0))  as INTEGER)        ship_qty_target," +
                        "  cast(nvl(cast(sum(nvl(ship_qty, 0)) as FLOAT) / cast(sum(nvl(commit_ship_qty, 0)) as FLOAT) ,0) * 100 as FLOAT)  otd_actual," +
                        "  get_aim_target_by_key(concat_ws('=', 'D', site_code, level_code, 'all', 'all', 'all', 'all'), 13) * 100  otd_target, " +
                        "  cast(unix_timestamp() AS VARCHAR(32)) etl_time" +
                        "   FROM dpm_dws_production_otd   " +
                        "GROUP BY" +
                        "  cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER)," +
                        "  site_code," +
                        "  customer_pn," +
                        "  level_code"
        );
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_otd_quarter", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_otd_quarter"), resultRows.schema());
        clearTable();
    }

    public void calculateL10YearProductionOTD() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(
                "SELECT" +
                        "  concat(unix_timestamp(), '-', uuid()) id," +
                        "  year(work_dt)                 year_id," +
                        "  site_code," +
                        "  level_code," +
                        "  'N/A'                                 factory_code," +
                        "  'N/A'                                 process_code," +
                        "  'N/A'                              customer_code," +
                        "  'N/A' plant_code," +
                        "  customer_pn                                 sku," +
                        "  cast(sum(nvl(ship_qty, 0))    as INTEGER)             ship_qty_actual," +
                        "  cast(sum(nvl(commit_ship_qty, 0))   as INTEGER)       ship_qty_target," +
                        "  cast(nvl(cast(sum(nvl(ship_qty, 0)) as FLOAT) / cast(sum(nvl(commit_ship_qty, 0)) as FLOAT) ,0) * 100 as FLOAT)  otd_actual," +
                        "  get_aim_target_by_key(concat_ws('=', 'D', site_code, level_code, 'all', 'all', 'all', 'all'), 13) * 100  otd_target, " +
                        "  cast(unix_timestamp() AS VARCHAR(32)) etl_time" +
                        "   FROM dpm_dws_production_otd" +
                        "  GROUP BY   " +
                        "  year(work_dt)                 ," +
                        "  site_code," +
                        "  customer_pn," +
                        "  level_code"
        );
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_otd_year", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_otd_year"), resultRows.schema());
        clearTable();
    }

    public void loadPreData(String startDay, String endDay) throws Exception {

        SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");
        String startDayStamp = String.valueOf(yyyy_MM_dd.parse(startDay).getTime());
        String endDayStamp = String.valueOf(yyyy_MM_dd.parse(endDay).getTime());

        JavaRDD<Row> dpm_dws_production_otds = DPHbase.saltRddRead("dpm_dws_production_otd", startDayStamp, endDayStamp, new Scan(), true).filter(r -> {

            return batchGetter.dateStrCompare(startDay
                    , batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OTD", "work_dt"), "yyyy-MM-dd", "<=")
                    &&
                    batchGetter.dateStrCompare(endDay
                            , batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OTD", "work_dt"), "yyyy-MM-dd", ">")

                    ;
        }).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_dws_production_otd", beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_dws_production_otd", "DPM_DWS_PRODUCTION_OTD")));
            }
            return rows.iterator();
        }).cache();

        sqlContext.createDataFrame(dpm_dws_production_otds, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_dws_production_otd")).createOrReplaceTempView("dpm_dws_production_otd");

        registerUDF();
    }

    public void registerUDF() {
        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);

    }

    public void clearTable() {
        sqlContext.dropTempTable("dpm_dws_production_otd");
        sqlContext.clearCache();
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
