package com.foxconn.dpm.sprint4.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsPersonnelEmpWorkhoursDD;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsProductionOeeEquipmentLineDD;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsProductionOutputDD;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.ProdPermissionManager;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.RDBConnetInfo;
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
import scala.Tuple4;
import scala.Tuple5;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author HS
 * @className L10ANDL5ProductionUpphSprintFour
 * @TODO 差人力工时 和 L5 产量
 * @date 2020/4/21 12:29
 */
public class L10ANDL5ProductionUpphSprintFour extends DPSparkBase {

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
        //       System.out.println(startWorkDay + "_" + endWorkDay);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        startStamp = simpleDateFormat.parse(startWorkDay).getTime();
        endStamp = simpleDateFormat.parse(endWorkDay).getTime();
        //         System.out.println(startStamp + "_" + endStamp);

        //初始化表和函数
        getDWSProductionOutput();
        Dataset<Row> dwsPersonnelWorkHoursDF = sqlContext.createDataFrame(getDWSPersonnelWorkHours(), DpmDwsPersonnelEmpWorkhoursDD.class);
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
//        sqlContext.sql("select * from dwsPersonnelWorkHours where factory_code = 'DT1' or factory_code = 'DT2'").show();
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_four_dpm_dsn_calcute_l5_6_10_day_detail_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();


        System.out.println("==============================>>>resultRows End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_detail", resultRows.toJavaRDD(),
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_detail",
                        DataTypes.createStructField("customer_code", DataTypes.StringType, true)
                ),
                resultRows.schema());
        clearTable();
    }

    //Day UPPH
    public void calculateDayUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_four_dpm_dsn_calcute_l5_10_day_upph.sql").replace("$ETL_TIME$", etl_time));
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            sqlContext.sql("select * from dwsProductionOutput where work_dt='2020-07-02' and level_code='L5' and site_code='WH'").show(2000);
            sqlContext.sql("select * from dwsPersonnelWorkHours where work_dt='2020-07-02' and level_code='L5' and site_code='WH' ").show(2000);
            sqlContext.sql("select * from dpm_ods_production_target_values  ").show(2000);
            resultRows.show(500);
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_day", resultRows.toJavaRDD(),
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_day",
                        DataTypes.createStructField("customer_code", DataTypes.StringType, true)
                ),
                resultRows.schema()
        );
        clearTable();
    }

    public void calculateWeekUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_four_dpm_dsn_calcute_l5_10_week_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_week", resultRows.toJavaRDD(),
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_week",
                        DataTypes.createStructField("customer_code", DataTypes.StringType, true)
                ),
                resultRows.schema());
        clearTable();
    }

    public void calculateMonthUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_four_dpm_dsn_calcute_l5_10_month_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_month", resultRows.toJavaRDD(),
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_month",
                        DataTypes.createStructField("customer_code", DataTypes.StringType, true)
                ),
                resultRows.schema());
        clearTable();
    }

    public void calculateQuarterUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_four_dpm_dsn_calcute_l5_10_quarter_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_quarter", resultRows.toJavaRDD(),
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_quarter",
                        DataTypes.createStructField("customer_code", DataTypes.StringType, true)
                ),
                resultRows.schema());
        clearTable();
    }

    public void calculateYearUpph() throws Exception {
        Dataset<Row> resultRows = sqlContext.sql(sqlGetter.Get("sprint_four_dpm_dsn_calcute_l5_10_year_upph.sql").replace("$ETL_TIME$", etl_time));
        resultRows.show();
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_upph_year", resultRows.toJavaRDD(),
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_upph_year",
                        DataTypes.createStructField("customer_code", DataTypes.StringType, true)
                ),
                resultRows.schema());
        clearTable();
    }

    public void registerUDF() {
        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);

    }
    /*
     * ====================================================================
     * 描述:
     *      获取上传产量
     * ====================================================================
     */

    public void getDWSProductionOutput() throws Exception {


        //读取日产量数据
        JavaRDD<Result> dpmDwsProductionOutputDD = DPHbase.saltRddRead("dpm_dws_production_output_dd", String.valueOf(startStamp), String.valueOf(endStamp), new Scan(), true);

        if (dpmDwsProductionOutputDD == null) {
            System.out.println("==========>>>>>>表空或者无数据<<<<<<<============");
            return;
        }
        /*
         * Molding
         * Stamping
         * Painting
         * Assy
         */
        JavaRDD<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDJavaRDD1 = dpmDwsProductionOutputDD.filter(result -> {
            //必须字段过滤
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "work_dt", "site_code", "level_code", "normalized_output_qty")
                    &&
                    (
                            (
                                    "process".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
                                            &&
                                            "ASSEMBLY1".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                            &&
                                            "L10".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                            )
                                    ||
                                    (
                                            "process".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
                                                    &&
                                                    (
                                                            "L5".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))

                                                    )

                                                    &&
                                                    (
                                                            "UpLoad_Molding".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                                                    ||
                                                                    "UpLoad_Stamping".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                                                    ||
                                                                    "UpLoad_Painting".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
                                                                    ||
                                                                    "UpLoad_Assy".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))


                                                    )
                                    )
                                    ||
                                    (
                                            "line".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))

                                                    &&
                                                    (
                                                            "L10".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                                                                    ||
                                                                    "L5".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                                                    )

                                    )

                    )
                    ;

        }).mapPartitions(batchP -> {
            //时间范围过滤
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDS = new ArrayList<>();
            while (batchP.hasNext()) {
                Result next = batchP.next();
                ArrayList<String> r = beanGetter.resultGetConfDeftColumnsValues(next, "dpm_dws_production_output_dd", "DPM_DWS_PRODUCTION_OUTPUT_DD");
                dpmDwsProductionOutputDDS.add(batchGetter.<DpmDwsProductionOutputDD>getBeanDeftInit(new DpmDwsProductionOutputDD(), r));
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
            //调整正确的工段码
            if ("L5".equals(t._2.getLevel_code())) {
                t._2.setCustomer("NULL");
            }
            if ("L5".equals(t._2.getLevel_code()) && "WH".equals(t._2.getSite_code()) && "line".equals(t._2.getData_granularity())){
                t._2.setProcess_code("Assy");
            }
            if ("L5".equals(t._2.getLevel_code()) && "process".equals(t._2.getData_granularity()) && t._2.getProcess_code().matches("^.+_.+$")) {
                t._2.setProcess_code(t._2.getProcess_code().split("_")[1]);
            } else if ("L10".equals(t._2.getLevel_code())) {
                if ("ASSEMBLY1".equals(t._2.getProcess_code()) && "process".equals(t._2.getData_granularity())) {
                    t._2.setProcess_code("Assy");
                } else if ("line".equals(t._2.getData_granularity())) {
                    t._2.setProcess_code("Packing");
                }
                t._2.setFactory_code("NULL");
            }
            return t._2;
        });

        sqlContext.createDataFrame(dpmDwsProductionOutputDDJavaRDD, DpmDwsProductionOutputDD.class).createOrReplaceTempView("dwsProductionOutput");
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

        JavaRDD<Result> filter = personnel_emp_workhours_dd.filter(result -> {
            //必须字段过滤
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "work_dt", "site_code", "level_code", "humresource_type", "work_shift", "update_dt", "update_by", "data_from")
                    &&
                    (
                            (
                                    (
                                            "L10".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "level_code"))
                                    )
                                            &&
                                            "line".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "data_granularity"))
                            )
                                    ||
                                    (
                                            (
                                                    "L5".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "level_code"))
                                            )
                                                    &&
                                                    "process".equals(batchGetter.resultGetColumn(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD", "data_granularity"))
                                    )
                    )
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
                        v.getWork_dt(), v.getSite_code(), v.getLevel_code(), v.getFactory_code(), v.getProcess_code(), v.getWork_shift(), v.getHumresource_type()
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
            return t._2;
        });

        return dpmDwsPersonnelEmpWorkhoursDDJavaRDD;
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}



