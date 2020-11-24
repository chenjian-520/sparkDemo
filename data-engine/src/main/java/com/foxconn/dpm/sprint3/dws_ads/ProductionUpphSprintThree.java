package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsPersonnelEmpWorkhoursDD;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

/**
 * @author HS
 * @className ProductionUpphSprintThree
 * @description 由line级的产量做日产量汇总；人力工时重新计算factory级的
 * @date 2020/4/21 12:29
 */
public class ProductionUpphSprintThree extends DPSparkBase {

    String startWorkDay = null;
    String endWorkDay = null;
    long startStamp = -1;
    long endStamp = -1;

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

    String etl_time = String.valueOf(System.currentTimeMillis());

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //dpm_ods_production_target_values
        //day  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateDayAdd(-1, "-"));
        loadTimeRangeData(batchGetter.getStDateDayAdd(-8, "-"), batchGetter.getStDateDayAdd(1, "-"));
        //计算UPPH并写入MYSQL
        calculateDayDetailUpph();

        //day  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateDayAdd(-1, "-"));
        loadTimeRangeData(batchGetter.getStDateDayAdd(-8, "-"), batchGetter.getStDateDayAdd(1, "-"));
        //计算UPPH并写入MYSQL
        calculateDayUpph();

        //Week  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(-1, "-")._2);
        loadTimeRangeData(batchGetter.getStDateWeekAdd(-1, "-")._1, batchGetter.getStDateWeekAdd(0, "-")._1);
        //计算UPPH并写入MYSQL
        calculateWeekUpph();

        //Week  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateWeekAdd(0, "-")._2);
        loadTimeRangeData(batchGetter.getStDateWeekAdd(0, "-")._1, batchGetter.getStDateWeekAdd(1, "-")._1);
        //计算UPPH并写入MYSQL
        calculateWeekUpph();


        //Month  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(-1, "-")._2);
        loadTimeRangeData(batchGetter.getStDateMonthAdd(-1, "-")._1, batchGetter.getStDateMonthAdd(0, "-")._1);
        //计算UPPH并写入MYSQL
        calculateMonthUpph();

        //Month  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateMonthAdd(0, "-")._2);
        loadTimeRangeData(batchGetter.getStDateMonthAdd(0, "-")._1, batchGetter.getStDateMonthAdd(1, "-")._1);
        //计算UPPH并写入MYSQL
        calculateMonthUpph();

        //Quarter  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(-1, "-")._2);
        loadTimeRangeData(batchGetter.getStDateQuarterAdd(-1, "-")._1, batchGetter.getStDateQuarterAdd(0, "-")._1);
        //计算UPPH并写入MYSQL
        calculateQuarterUpph();

        //Quarter  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateQuarterAdd(0, "-")._2);
        loadTimeRangeData(batchGetter.getStDateQuarterAdd(0, "-")._1, batchGetter.getStDateQuarterAdd(1, "-")._1);
        //计算UPPH并写入MYSQL
        calculateQuarterUpph();

        //Year  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(-1, "-")._2);
        loadTimeRangeData(batchGetter.getStDateYearAdd(-1, "-")._1, batchGetter.getStDateYearAdd(0, "-")._1);
        //计算UPPH并写入MYSQL
        calculateYearUpph();

        //Year  upph
        LoadKpiTarget.loadProductionTarget(batchGetter.getStDateYearAdd(0, "-")._2);
        loadTimeRangeData(batchGetter.getStDateYearAdd(0, "-")._1, batchGetter.getStDateYearAdd(1, "-")._1);
        //计算UPPH并写入MYSQL
        calculateYearUpph();
    }


    public void loadTimeRangeData(String sDay, String eDay) throws Exception {
        //入口时间
        startWorkDay = sDay;
        endWorkDay = eDay;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        startStamp = simpleDateFormat.parse(startWorkDay).getTime();
        endStamp = simpleDateFormat.parse(endWorkDay).getTime();

        //初始化表和函数
        Dataset<Row> dwsProductionOutputDF = sqlContext.createDataFrame(getDWSProductionOutput(), DpmDwsProductionOutputDD.class);
        Dataset<Row> dwsPersonnelWorkHoursDF = sqlContext.createDataFrame(getDWSPersonnelWorkHours(), DpmDwsPersonnelEmpWorkhoursDD.class);
        dwsProductionOutputDF.createOrReplaceTempView("dwsProductionOutput");
        dwsPersonnelWorkHoursDF.createOrReplaceTempView("dwsPersonnelWorkHours");

        registerUDF();
    }

    public void clearTable() {
        sqlContext.dropTempTable("dwsProductionOutput");
        sqlContext.dropTempTable("dwsPersonnelWorkHours");
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }

    //Day UPPH
    public void calculateDayDetailUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("dpm_dsn_calcute_l5_6_10_day_detail_upph.sql").replace("$ETL_TIME$", etl_time));
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            resultRows.show();
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_detail", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_detail"), resultRows.schema());
        clearTable();
    }

    //Day UPPH
    public void calculateDayUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_dpm_dsn_calcute_l5_6_10_day_upph.sql").replace("$ETL_TIME$", etl_time));
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            resultRows.show(1000);
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_day", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_day"), resultRows.schema());

        JavaRDD<Row> l5LevelUPPH = calculateL5LevelUPPH(resultRows);
        System.out.println("==============================>>>L5 Level QA Log Start<<<==============================");
        try {
            for (Row row : l5LevelUPPH.collect()) {

                System.out.println(row.toString());
            }
            System.out.println(MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_day").toString());
            System.out.println(resultRows.schema().toString());
        } catch (Exception e) {

        }
        System.out.println("==============================>>>L5 Level QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dpm_ads_production_upph_day", l5LevelUPPH, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_day"), resultRows.schema());
        clearTable();
    }

    public void calculateWeekUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_dpm_dsn_calcute_l5_6_10_week_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_week", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_week"), resultRows.schema());

        JavaRDD<Row> l5LevelUPPH = calculateL5LevelUPPH(resultRows);
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_week", l5LevelUPPH, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_week"), resultRows.schema());
        System.out.println("==============================>>>week Log Start<<<==============================");
        try {
            for (Row row : l5LevelUPPH.collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>Log End<<<==============================");
        clearTable();
    }

    public void calculateMonthUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_dpm_dsn_calcute_l5_6_10_month_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_month", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_month"), resultRows.schema());


        JavaRDD<Row> l5LevelUPPH = calculateL5LevelUPPH(resultRows);
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_month", l5LevelUPPH, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_month"), resultRows.schema());
        System.out.println("==============================>>>month Log Start<<<==============================");
        try {
            for (Row row : l5LevelUPPH.collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>Log End<<<==============================");
        clearTable();
    }

    public void calculateQuarterUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_dpm_dsn_calcute_l5_6_10_quarter_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        System.out.println("==============================>>>resultRows End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_quarter", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_quarter"), resultRows.schema());


        JavaRDD<Row> l5LevelUPPH = calculateL5LevelUPPH(resultRows);
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_quarter", l5LevelUPPH, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_quarter"), resultRows.schema());
        System.out.println("==============================>>>quarter Log Start<<<==============================");
        try {
            for (Row row : l5LevelUPPH.collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>Log End<<<==============================");
        clearTable();
    }

    public void calculateYearUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_three_dpm_dsn_calcute_l5_6_10_year_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_year", resultRows.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_year"), resultRows.schema());

        JavaRDD<Row> l5LevelUPPH = calculateL5LevelUPPH(resultRows);
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_year", l5LevelUPPH, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_year"), resultRows.schema());
        System.out.println("==============================>>>year Log Start<<<==============================");
        try {
            for (Row row : l5LevelUPPH.collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>Log End<<<==============================");
        clearTable();
    }

    public void registerUDF() {
        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);

    }
    /*
     * ====================================================================
     * 描述:
     *      获取指定时间段（天）的DSN日产量
     *      dpm_dws_production_output_dd        DPM_DWS_PRODUCTION_OUTPUT_DD
     * ====================================================================
     */

    public JavaRDD<DpmDwsProductionOutputDD> getDWSProductionOutput() throws Exception {


        //读取日产量数据
        JavaRDD<Result> dpmDwsProductionOutputDD = DPHbase.saltRddRead("dpm_dws_production_output_dd", String.valueOf(startStamp), String.valueOf(endStamp), new Scan(), true);

        if (dpmDwsProductionOutputDD == null) {
            System.out.println("==========>>>>>>表空或者无数据<<<<<<<============");
            return null;
        }

        final JavaRDD<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDJavaRDD1 = dpmDwsProductionOutputDD.filter(result -> {
            //必须字段过滤
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "work_dt", "site_code", "level_code", "normalized_output_qty")
                    &&
                    (
                            (
                                    "L6".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                                            ||
                                            "L10".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                                            ||
                                            "L5".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                            )
                                    &&
                                    "line".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
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
                if (batchGetter.getFilterRangeTimeStampHBeans(next, "DPM_DWS_PRODUCTION_OUTPUT_DD", "work_dt", "yyyy-MM-dd", startStamp, endStamp)) {
                    ArrayList<String> r = beanGetter.resultGetConfDeftColumnsValues(next, "dpm_dws_production_output_dd", "DPM_DWS_PRODUCTION_OUTPUT_DD");
                    dpmDwsProductionOutputDDS.add(batchGetter.<DpmDwsProductionOutputDD>getBeanDeftInit(new DpmDwsProductionOutputDD(), r));
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
            return t._2;
        }).map(b -> {

            //process_code 4 site_code 1 data_granularity 14
            if ("process".equals(b.getData_granularity()) && "L5".equals(b.getLevel_code()) && b.getProcess_code().matches("^.+_.+$")) {
                b.setProcess_code(b.getProcess_code().split("_")[1]);
            }
            if (b.getFactory_code() == null || b.getFactory_code().equals("") || "N/A".equals(b.getFactory_code())) {
                b.setFactory_code("NULL");
            }
            return b;
        });

        return dpmDwsProductionOutputDDJavaRDD;
    }

    /*
     * ====================================================================
     * 描述:
     *      获取指定时间段（天）的 E_HR人力工时
     *      dpm_dws_personnel_emp_workhours_dd        DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD
     * ====================================================================
     */
    public JavaRDD<DpmDwsPersonnelEmpWorkhoursDD> getDWSPersonnelWorkHours() throws Exception {
        JavaRDD<Result> personnel_emp_workhours_dd = DPHbase.saltRddRead("dpm_dws_personnel_emp_workhours_dd", String.valueOf(startStamp), String.valueOf(endStamp), new Scan(), true);

        final JavaRDD<Result> filter = personnel_emp_workhours_dd.filter(result -> {
            //必须字段过滤
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "work_dt", "site_code", "level_code", "humresource_type", "work_shift", "update_dt", "update_by", "data_from")
                    &&
                    !"".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "level_code"))
                    &&
                    !"".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "humresource_type"))
                    &&
                    "factory".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "data_granularity"))
                    ;

        });
        JavaRDD<DpmDwsPersonnelEmpWorkhoursDD> dpmDwsPersonnelEmpWorkhoursDDJavaRDD1 = filter.mapPartitions(batchP -> {
            //时间范围过滤
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<DpmDwsPersonnelEmpWorkhoursDD> dpmDwsPersonnelEmpWorkhoursDD = new ArrayList<>();
            while (batchP.hasNext()) {
                Result next = batchP.next();
                if (batchGetter.getFilterRangeTimeStampHBeans(next, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "work_dt", "yyyy-MM-dd", startStamp, endStamp)) {
                    dpmDwsPersonnelEmpWorkhoursDD.add(batchGetter.<DpmDwsPersonnelEmpWorkhoursDD>getBeanDeftInit(new DpmDwsPersonnelEmpWorkhoursDD(), beanGetter.resultGetConfDeftColumnsValues(next, "dpm_dws_personnel_emp_workhours_dd", "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD")));
                }
            }
            return dpmDwsPersonnelEmpWorkhoursDD.iterator();
        });


        JavaRDD<DpmDwsPersonnelEmpWorkhoursDD> dpmDwsPersonnelEmpWorkhoursDDJavaRDD = dpmDwsPersonnelEmpWorkhoursDDJavaRDD1.mapToPair(new PairFunction<DpmDwsPersonnelEmpWorkhoursDD, String, DpmDwsPersonnelEmpWorkhoursDD>() {
            @Override
            public Tuple2<String, DpmDwsPersonnelEmpWorkhoursDD> call(DpmDwsPersonnelEmpWorkhoursDD v) throws Exception {
                v.setFactory_code(v.getFactory_code() == null || "".equals(v.getFactory_code()) ? "N/A" : v.getFactory_code());
                return new Tuple2<String, DpmDwsPersonnelEmpWorkhoursDD>(batchGetter.getStrArrayOrg("=", "-",
                        v.getWork_dt(), v.getSite_code(), v.getLevel_code(), v.getFactory_code(), v.getWork_shift(), v.getHumresource_type()
                ), v);
            }
        }).reduceByKey((v1, v2) -> {
            return v1.getUpdate_dt() > v2.getUpdate_dt() ? v1 : v2;
        }).map(t -> {
            String humresource_type = t._2.getHumresource_type();

            //DL1 DL2V DL2F IDL

            //DL1V
            //DL2V
            //DL2F
            // IDL1F IDL1V IDL2F IDL2V

            switch (humresource_type) {
                case "DL1V":
                    humresource_type = "DL1";
                    break;
                case "IDL1F":
                case "IDL1V":
                case "IDL2F":
                case "IDL2V":
                    humresource_type = "IDL";
                    break;

            }

            t._2.setHumresource_type(humresource_type);


            if (t._2.getFactory_code() == null || t._2.getFactory_code().equals("") || "N/A".equals(t._2.getFactory_code())) {
                t._2.setFactory_code("NULL");
            }

            return t._2;
        });

        return dpmDwsPersonnelEmpWorkhoursDDJavaRDD;
    }

    public JavaRDD<Row> calculateL5LevelUPPH(Dataset<Row> resultRows) {
        final StructType schema = resultRows.schema();
        JavaRDD<Row> l5FactoryUPPH = resultRows.where("(level_code='L5' and factory_code in ('DT1', 'DT2') and site_code = 'WH') or (level_code='L5'  and site_code = 'CQ') and emp_humman_resource_code in ('DL1', 'DL2V')").javaRDD();
        return l5FactoryUPPH.keyBy(r -> {
            //work_date site_code level_code emp_humman_resource_code
            return String.valueOf(r.get(1)).concat(r.getString(2)).concat(r.getString(3)).concat(r.getString(8));
        }).reduceByKey((v1, v2) -> {
            ArrayList<Object> v1Obj = new ArrayList<Object>();
            v1Obj.addAll(JavaConverters.seqAsJavaListConverter(v1.toSeq()).asJava());
            ArrayList<Object> v2Obj = new ArrayList<Object>();
            v2Obj.addAll(JavaConverters.seqAsJavaListConverter(v2.toSeq()).asJava());

            /**
             *  id,
             *  work_date,
             *  site_code,
             *  level_code,
             *  factory_code,
             *  process_code,
             *  'N/A'        line_code,
             *  'N/A'        block_code,
             *  emp_humman_resource_code,
             *  upph_actual,
             *  upph_target,
             *  '$ETL_TIME$' etl_time,
             *  humresource_type,
             *  normalized_output_qty,
             *  act_attendance_workhours,
             *  attendance_qty,
             *  output_qty
             */
            //new id
            v1Obj.set(0, String.valueOf(System.currentTimeMillis()).concat(UUID.randomUUID().toString().replace("-", "")));
            v1Obj.set(4, "N/A");//new Factory code
            /*----------------------------------------------------------------------------------------------*/
            /**
             * ====================================================================
             * 描述:
             *      DT1    A UPPH 实际值 B UPPH目标值  E  约当产量 G 人力工时
             *      DT2    C UPPH 实际值 D UPPH目标值  F  约当产量 H 人力工时
             *      Level upph target  (B * A + D * C) / (A + C)
             *      Level upph actual  (E + F) / (G + H)
             * ====================================================================
             */
            v1Obj.set(9, batchGetter.formatFloat(
                    (batchGetter.formatFloat(v1Obj.get(13)) + batchGetter.formatFloat(v2Obj.get(13)))//noroutput
                            /
                            (batchGetter.formatFloat(v1Obj.get(14)) + batchGetter.formatFloat(v2Obj.get(14)))//hours
            ));// upph actual
            v1Obj.set(10,
                    batchGetter.formatFloat((
                                    (batchGetter.formatFloat(v1Obj.get(9)) * batchGetter.formatFloat(v1Obj.get(10)))  // dt1 q
                                            +
                                            (batchGetter.formatFloat(v2Obj.get(9)) * batchGetter.formatFloat(v2Obj.get(10))) // dt2 q

                            )
                                    /
                                    (batchGetter.formatFloat(v1Obj.get(9)) + batchGetter.formatFloat(v2Obj.get(9))) // dt1 ac + dt2 ac)
                    ));// upph target
            /*----------------------------------------------------------------------------------------------*/
            v1Obj.set(13, (batchGetter.formatDouble(v1Obj.get(13)) + batchGetter.formatDouble(v2Obj.get(13))));//normalized_output_qty
            v1Obj.set(14, (batchGetter.formatDouble(v1Obj.get(14)) + batchGetter.formatDouble(v2Obj.get(14))));//act_attendance_workhours
            v1Obj.set(15, (batchGetter.formatLong(v1Obj.get(15)) + batchGetter.formatLong(v2Obj.get(15))));//attendance_qty
            v1Obj.set(16, (batchGetter.formatDouble(v1Obj.get(16)) + batchGetter.formatDouble(16)));//output_qty

            return new GenericRowWithSchema(v1Obj.toArray(new Object[0]), schema);
        }).map(t -> {
            ArrayList<Object> v1Obj = new ArrayList<Object>();
            v1Obj.addAll(JavaConverters.seqAsJavaListConverter(t._2.toSeq()).asJava());
            ;
            v1Obj.set(4, "N/A");


            return new GenericRowWithSchema(v1Obj.toArray(new Object[0]), schema);
        });
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}



