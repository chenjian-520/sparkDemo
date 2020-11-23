package com.foxconn.dpm.dws_ads;

import com.foxconn.dpm.dws_ads.batchData.BatchGetterFY;
import com.foxconn.dpm.dws_ads.beans.ManualHour;
import com.foxconn.dpm.dws_ads.beans.ProductionOutput;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.HashMap;
import java.util.Map;

/**
 * @author HS
 * @className DSNUPPHCalculate
 * @description TODO
 * @date 2019/12/22 16:48
 */
public class DSNUPPHCalculate extends DPSparkBase {

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
     *                                                                           ************
     ********************************************************************************************** */
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        dpm_ads_production_site_kpi_day();
        //dpm_ads_production_site_kpi_week();
        //dpm_ads_production_site_kpi_month();
        //dpm_ads_production_site_kpi_quarter();
        //dpm_ads_production_site_kpi_year();

        DPSparkApp.stop();
    }

    public void dpm_ads_production_site_kpi_year() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetterFY batchGetterFY = BatchGetterFY.getInstance();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        Scan manual_hour_scan = new Scan();
        manual_hour_scan.withStartRow(batchGetterFY.getStDateYearAdd(0)._1.getBytes(), true);
        manual_hour_scan.withStopRow(batchGetterFY.getStDateYearAdd(0)._2.getBytes(), true);
        //读取日工时数据
        JavaRDD<Result> manual_hour_rdd = DPHbase.rddRead("dpm_ods_manual_manhour", manual_hour_scan, true);
        //读取dws的约当日产量
        JavaRDD<Result> pd_output_rdd = DPHbase.saltRddRead("dpm_dws_production_output_day", batchGetterFY.getStampDateYear(batchGetterFY.getNowYear() - 1)._1 + "", batchGetterFY.getStampDateYear(batchGetterFY.getNowYear())._2 + "", new Scan());

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Level", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    return new ManualHour(r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7),
                            Double.valueOf("".equals(r.get(8)) ? "0" : r.get(8).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(9)) ? "0" : r.get(9).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(10)) ? "0" : r.get(10).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(11)) ? "0" : r.get(11).replace(",", "").replace("-", "")));
                });

        /**
         * ===================================================================
         *
         * 清洗约当产量数据
         *
         * ===================================================================
         */
        JavaRDD<ProductionOutput> format_pd_output_rdd = pd_output_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "LevelCodeID", "LineCodeID", "WorkDT", "normalized_output_qty");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "SiteCodeID", "LevelCodeID", "PlantCodeID", "ProcessCodeID", "AreaCodeID", "LineCodeID", "MachineID", "PartNo", "Sku", "Plantform", "WorkorderType", "WorkDT", "WorkShifitClass", "normalized_output_qty");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    //不需要rowkey，所以从下标1开始取
                    return new ProductionOutput(
                            r.get(1),
                            r.get(2),
                            r.get(3),
                            r.get(4),
                            r.get(5),
                            r.get(6),
                            r.get(7),
                            r.get(8),
                            r.get(9),
                            r.get(10),
                            r.get(11),
                            r.get(12),
                            Integer.valueOf(r.get(13)),
                            Integer.valueOf(r.get(14)));
                });




        /*
         * ====================================================================
         * 描述:
         *      数据计算
         *     DL1_TTL_Manhour
         *     DL2_Fixed_Manhour
         *     DL2_Variable_Manhour
         * ====================================================================
         */
        SparkSession sqlContext = DPSparkApp.getSession();
        //sqlContext.setConf("spark.sql.crossJoin.enabled", "true");

        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> pd_output_dataFrame = sqlContext.createDataFrame(format_pd_output_rdd, ProductionOutput.class);

        pd_output_dataFrame.createOrReplaceTempView("dpm_dws_production_output_day");
        sqlContext.sql("select * from dpm_dws_production_output_day").show();
        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        sqlContext.sql("select * from dpm_ods_manual_manhour").show();

        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_year_kpi.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));
        rowDataset.show();


        /*
         * ====================================================================
         * 描述:
         * id
         * site_code_id
         * site_code
         * site_code_desc
         * level_code_id
         * level_code
         * level_code_desc
         * month_id
         * online_dl_upph_actual
         * online_dl_upph_target
         * offline_var_dl_upph_actual
         * offline_var_dl_upph_target
         * offline_fix_dl_headcount_actual
         * offline_fix_dl_headcount_target
         * etl_time
         * ====================================================================
         */
        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("year_id", DataTypes.createStructField("year_id", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        Class.forName("com.mysql.jdbc.Driver");
//        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_year", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());

    }

    public void dpm_ads_production_site_kpi_quarter() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetterFY batchGetterFY = BatchGetterFY.getInstance();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        Scan manual_hour_scan = new Scan();
        manual_hour_scan.withStartRow(batchGetterFY.getStDateQuarterAdd(0)._1.getBytes(), true);
        manual_hour_scan.withStopRow(batchGetterFY.getStDateQuarterAdd(0)._2.getBytes(), true);
        //读取日工时数据
        JavaRDD<Result> manual_hour_rdd = DPHbase.rddRead("dpm_ods_manual_manhour", manual_hour_scan, true);
        //读取dws的约当日产量
        JavaRDD<Result> pd_output_rdd = DPHbase.saltRddRead("dpm_dws_production_output_day", batchGetterFY.getStampDateQuarter(batchGetterFY.getNowQuarter() - 1)._1 + "", batchGetterFY.getStampDateQuarter(batchGetterFY.getNowQuarter())._2 + "", new Scan());

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Level", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    return new ManualHour(r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7),
                            Double.valueOf("".equals(r.get(8)) ? "0" : r.get(8).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(9)) ? "0" : r.get(9).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(10)) ? "0" : r.get(10).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(11)) ? "0" : r.get(11).replace(",", "").replace("-", "")));
                });

        /**
         * ===================================================================
         *
         * 清洗约当产量数据
         *
         * ===================================================================
         */
        JavaRDD<ProductionOutput> format_pd_output_rdd = pd_output_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "LevelCodeID", "LineCodeID", "WorkDT", "normalized_output_qty");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "SiteCodeID", "LevelCodeID", "PlantCodeID", "ProcessCodeID", "AreaCodeID", "LineCodeID", "MachineID", "PartNo", "Sku", "Plantform", "WorkorderType", "WorkDT", "WorkShifitClass", "normalized_output_qty");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    //不需要rowkey，所以从下标1开始取
                    return new ProductionOutput(
                            r.get(1),
                            r.get(2),
                            r.get(3),
                            r.get(4),
                            r.get(5),
                            r.get(6),
                            r.get(7),
                            r.get(8),
                            r.get(9),
                            r.get(10),
                            r.get(11),
                            r.get(12),
                            Integer.valueOf(r.get(13)),
                            Integer.valueOf(r.get(14)));
                });



        /*
         * ====================================================================
         * 描述:
         *      数据计算
         *     DL1_TTL_Manhour
         *     DL2_Fixed_Manhour
         *     DL2_Variable_Manhour
         * ====================================================================
         */
        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        //sqlContext.setConf("spark.sql.crossJoin.enabled", "true");

        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> pd_output_dataFrame = sqlContext.createDataFrame(format_pd_output_rdd, ProductionOutput.class);

        pd_output_dataFrame.createOrReplaceTempView("dpm_dws_production_output_day");
        sqlContext.sql("select * from dpm_dws_production_output_day").show();
        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour").show();

        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_quarter_kpi.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));
        rowDataset.show();


        /*
         * ====================================================================
         * 描述:
         * id
         * site_code_id
         * site_code
         * site_code_desc
         * level_code_id
         * level_code
         * level_code_desc
         * month_id
         * online_dl_upph_actual
         * online_dl_upph_target
         * offline_var_dl_upph_actual
         * offline_var_dl_upph_target
         * offline_fix_dl_headcount_actual
         * offline_fix_dl_headcount_target
         * etl_time
         * ====================================================================
         */
        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("quarter_id", DataTypes.createStructField("quarter_id", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        Class.forName("com.mysql.jdbc.Driver");
//        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_quarter", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());

    }

    public void dpm_ads_production_site_kpi_month() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetterFY batchGetterFY = BatchGetterFY.getInstance();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        Scan manual_hour_scan = new Scan();
        manual_hour_scan.withStartRow(batchGetterFY.getStDateMonthAdd(0)._1.getBytes(), true);
        manual_hour_scan.withStopRow(batchGetterFY.getStDateMonthAdd(0)._2.getBytes(), true);
        //读取日工时数据
        JavaRDD<Result> manual_hour_rdd = DPHbase.rddRead("dpm_ods_manual_manhour", manual_hour_scan, true);
        //读取dws的约当日产量
        JavaRDD<Result> pd_output_rdd = DPHbase.saltRddRead("dpm_dws_production_output_day", batchGetterFY.getStampDateMonth(batchGetterFY.getNowMonth() - 1)._1 + "", batchGetterFY.getStampDateMonth(batchGetterFY.getNowMonth() - 1)._2 + "", new Scan());

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Level", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    return new ManualHour(r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7),
                            Double.valueOf("".equals(r.get(8)) ? "0" : r.get(8).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(9)) ? "0" : r.get(9).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(10)) ? "0" : r.get(10).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(11)) ? "0" : r.get(11).replace(",", "").replace("-", "")));
                });

        /**
         * ===================================================================
         *
         * 清洗约当产量数据
         *
         * ===================================================================
         */
        JavaRDD<ProductionOutput> format_pd_output_rdd = pd_output_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "LevelCodeID", "LineCodeID", "WorkDT", "normalized_output_qty");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "SiteCodeID", "LevelCodeID", "PlantCodeID", "ProcessCodeID", "AreaCodeID", "LineCodeID", "MachineID", "PartNo", "Sku", "Plantform", "WorkorderType", "WorkDT", "WorkShifitClass", "normalized_output_qty");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    //不需要rowkey，所以从下标1开始取
                    return new ProductionOutput(
                            r.get(1),
                            r.get(2),
                            r.get(3),
                            r.get(4),
                            r.get(5),
                            r.get(6),
                            r.get(7),
                            r.get(8),
                            r.get(9),
                            r.get(10),
                            r.get(11),
                            r.get(12),
                            Integer.valueOf(r.get(13)),
                            Integer.valueOf(r.get(14)));
                });



        /*
         * ====================================================================
         * 描述:
         *      数据计算
         *     DL1_TTL_Manhour
         *     DL2_Fixed_Manhour
         *     DL2_Variable_Manhour
         * ====================================================================
         */
        SparkSession sqlContext = DPSparkApp.getSession();
        //sqlContext.setConf("spark.sql.crossJoin.enabled", "true");

        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> pd_output_dataFrame = sqlContext.createDataFrame(format_pd_output_rdd, ProductionOutput.class);

        pd_output_dataFrame.createOrReplaceTempView("dpm_dws_production_output_day");
        sqlContext.sql("select * from dpm_dws_production_output_day").show();
        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour").show();

        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_month_kpi.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));
        rowDataset.show();


        /*
         * ====================================================================
         * 描述:
         * id
         * site_code_id
         * site_code
         * site_code_desc
         * level_code_id
         * level_code
         * level_code_desc
         * month_id
         * online_dl_upph_actual
         * online_dl_upph_target
         * offline_var_dl_upph_actual
         * offline_var_dl_upph_target
         * offline_fix_dl_headcount_actual
         * offline_fix_dl_headcount_target
         * etl_time
         * ====================================================================
         */
        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("month_id", DataTypes.createStructField("month_id", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        Class.forName("com.mysql.jdbc.Driver");
//        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_month", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());

    }

    public void dpm_ads_production_site_kpi_week() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetterFY batchGetterFY = BatchGetterFY.getInstance();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        Scan manual_hour_scan = new Scan();
        manual_hour_scan.withStartRow(batchGetterFY.getStDateWeekAdd(0)._1.getBytes(), true);
        manual_hour_scan.withStopRow(batchGetterFY.getStDateWeekAdd(0)._2.getBytes(), true);
        //读取日工时数据
        JavaRDD<Result> manual_hour_rdd = DPHbase.rddRead("dpm_ods_manual_manhour", manual_hour_scan, true);
        //读取dws的约当日产量
        JavaRDD<Result> pd_output_rdd = DPHbase.saltRddRead("dpm_dws_production_output_day", batchGetterFY.getStampDateWeek(batchGetterFY.getNowWeek() - 1)._1 + "", batchGetterFY.getStampDateWeek(batchGetterFY.getNowWeek())._2 + 1 + "", new Scan());

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Level", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    return new ManualHour(r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7),
                            Double.valueOf("".equals(r.get(8)) ? "0" : r.get(8).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(9)) ? "0" : r.get(9).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(10)) ? "0" : r.get(10).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(11)) ? "0" : r.get(11).replace(",", "").replace("-", "")));
                });

        /**
         * ===================================================================
         *
         * 清洗约当产量数据
         *
         * ===================================================================
         */
        JavaRDD<ProductionOutput> format_pd_output_rdd = pd_output_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "LevelCodeID", "LineCodeID", "WorkDT", "normalized_output_qty");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "SiteCodeID", "LevelCodeID", "PlantCodeID", "ProcessCodeID", "AreaCodeID", "LineCodeID", "MachineID", "PartNo", "Sku", "Plantform", "WorkorderType", "WorkDT", "WorkShifitClass", "normalized_output_qty");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    //不需要rowkey，所以从下标1开始取
                    return new ProductionOutput(
                            r.get(1),
                            r.get(2),
                            r.get(3),
                            r.get(4),
                            r.get(5),
                            r.get(6),
                            r.get(7),
                            r.get(8),
                            r.get(9),
                            r.get(10),
                            r.get(11),
                            r.get(12),
                            Integer.valueOf(r.get(13)),
                            Integer.valueOf(r.get(14)));
                });
        /*
         * ====================================================================
         * 描述:
         *      数据计算
         *     DL1_TTL_Manhour
         *     DL2_Fixed_Manhour
         *     DL2_Variable_Manhour
         * ====================================================================
         */
        SparkSession sqlContext = DPSparkApp.getSession();
        //sqlContext.setConf("spark.sql.crossJoin.enabled", "true");

        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> pd_output_dataFrame = sqlContext.createDataFrame(format_pd_output_rdd, ProductionOutput.class);

        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        pd_output_dataFrame.createOrReplaceTempView("dpm_dws_production_output_day");
        sqlContext.sql("select * from dpm_dws_production_output_day").show();
        sqlContext.sql("select * from dpm_ods_manual_manhour").show();
        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_week_kpi.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));
        rowDataset.show();

        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("week_id", DataTypes.createStructField("week_id", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        Class.forName("com.mysql.jdbc.Driver");
//        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_week", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());

    }

    public void dpm_ads_production_site_kpi_day() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工聚类
         * ====================================================================
         */
        BatchGetterFY batchGetterFY = BatchGetterFY.getInstance();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        System.out.println(batchGetterFY.getStDateDayAdd(-100));
        System.out.println(batchGetterFY.getStDateDayAdd(0));
        Scan manual_hour_scan = new Scan();
        manual_hour_scan.withStartRow(batchGetterFY.getStDateDayAdd(-100).getBytes(), true);
        manual_hour_scan.withStopRow(batchGetterFY.getStDateDayAdd(0).getBytes(), true);
        //读取日工时数据
        JavaRDD<Result> manual_hour_rdd = DPHbase.rddRead("dpm_ods_manual_manhour", manual_hour_scan, true);
        //读取dws的约当日产量
        JavaRDD<Result> pd_output_rdd = DPHbase.saltRddRead("dpm_dws_production_output_day", batchGetterFY.getStampDateDayAdd(-1) + "", batchGetterFY.getStampDateDayAdd(0) + "", new Scan());

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Site", "Level", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "rowkey", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "DL2_Fixed_Manhour", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    return new ManualHour(r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7),
                            Double.valueOf("".equals(r.get(8)) ? "0" : r.get(8).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(9)) ? "0" : r.get(9).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(10)) ? "0" : r.get(10).replace(",", "").replace("-", "")),
                            Double.valueOf("".equals(r.get(11)) ? "0" : r.get(11).replace(",", "").replace("-", "")));
                });
        /**
         * ===================================================================
         *
         * 清洗约当产量数据
         *
         * ===================================================================
         */
        JavaRDD<ProductionOutput> format_pd_output_rdd = pd_output_rdd
                .filter(result -> {
                    return batchGetterFY.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "SiteCodeID", "LevelCodeID", "WorkDT", "normalized_output_qty");
                })
                .map(r -> {
                    return batchGetterFY.resultGetColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DAY", "rowkey", "SiteCodeID", "LevelCodeID", "PlantCodeID", "ProcessCodeID", "AreaCodeID", "LineCodeID", "MachineID", "PartNo", "Sku", "Plantform", "WorkorderType", "WorkDT", "WorkShifitClass", "normalized_output_qty");
                }).filter(r -> {
                    return r != null;
                }).map(r -> {
                    //不需要rowkey，所以从下标1开始取
                    return new ProductionOutput(
                            r.get(1),
                            r.get(2),
                            r.get(3),
                            r.get(4),
                            r.get(5),
                            r.get(6),
                            r.get(7),
                            r.get(8),
                            r.get(9),
                            r.get(10),
                            r.get(11),
                            r.get(12),
                            Integer.valueOf(r.get(13)),
                            Integer.valueOf(r.get(14)));
                });



        /*
         * ====================================================================
         * 描述:
         *      数据计算
         *     DL1_TTL_Manhour
         *     DL2_Fixed_Manhour
         *     DL2_Variable_Manhour
         * ====================================================================
         */
        SparkSession sqlContext = DPSparkApp.getSession();
        //sqlContext.setConf("spark.sql.crossJoin.enabled", "true");

        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> pd_output_dataFrame = sqlContext.createDataFrame(format_pd_output_rdd, ProductionOutput.class);
        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        pd_output_dataFrame.createOrReplaceTempView("dpm_dws_production_output_day");

        sqlContext.sql("select * from dpm_ods_manual_manhour").show();
        sqlContext.sql("select * from dpm_dws_production_output_day").show();
        String sql = sqlGetter.Get("calcute_day_kpi.sql").replace("${etl_time}", System.currentTimeMillis() + "");
        Dataset<Row> rowDataset = sqlContext.sql(sql);
        rowDataset.show();
        /*
         * ====================================================================
         * 描述:
         *      site_code_id String
         *      level_code_id String
         *      work_date String
         *      online_dl_upph_actual String
         *      --------------------------------------------------------------
         *      id String
         *      site_code_desc String
         *      level_code_id int
         *      level_code_desc String
         *      month_id int
         *      online_dl_upph_target
         *      offline_var_dl_upph_actual String
         *      offline_var_dl_upph_target String
         *      offline_fix_dl_headcount_actual String
         *      offline_fix_dl_headcount_target String
         *      etl_time String
         * ====================================================================
         */
        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("work_date", DataTypes.createStructField("work_date", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));
        Class.forName("com.mysql.jdbc.Driver");
        //DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_day", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {
    }
}
