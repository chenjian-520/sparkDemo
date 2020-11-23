package com.foxconn.dpm.temporary;

import com.foxconn.dpm.dwd_dws.beans.EHR_MAN_HOUR;
import com.foxconn.dpm.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.dwd_dws.udf.FormatFloatNumber;
import com.foxconn.dpm.temporary.beans.DayDop;
import com.foxconn.dpm.temporary.beans.ManualHour;
import com.foxconn.dpm.temporary.beans.ManualNormalization;
import com.foxconn.dpm.temporary.beans.ManualStandardValue;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.hbaseread.HGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author HS
 * @className LevelDayOutPut
 * @description TODO
 * @date 2019/12/27 17:23
 */
public class LevelDayOutPut extends DPSparkBase {
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        String job_switch = (String) map.get("JOB_SWITCH");

        /*switch (job_switch){
            case "DAY_WEEK_MONTH":
                dayUPPH();
                weekUPPH();
                monthUPPH();
                break;
            case "QUARTER_YEAR":
                quarterUPPH();
                yearUPPH();
                break;
        }*/

        dayUPPHTest();
        DPSparkApp.stop();
    }

    public void dayUPPHTest() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工具类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        HGetter hGetter = MetaGetter.getHGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        System.out.println(batchGetter.getStDateDayAdd(-3));
        System.out.println(batchGetter.getStDateDayAdd(-2));
        //读取日工时数据
        //JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1), batchGetter.getStDateDayAdd(0), true);
        JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-4).replace("-", ""), batchGetter.getStDateDayAdd(-3).replace("-", ""), true);

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualHour(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), batchGetter.formatDouble(r.get(7)), batchGetter.formatDouble(r.get(8)), batchGetter.formatDouble(r.get(9)), batchGetter.formatDouble(r.get(10)));
        }).distinct();

        System.out.println("manualHour========================>>>");


        System.out.println(batchGetter.getStDateDayAdd(-4, "-"));
        System.out.println(batchGetter.getStDateDayAdd(-3, "-"));
        //读取日产量数据
        //00:2019-12-24:BU1008:90HM:BTO
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"), true, ":", 20);
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(1, "-"), true, ":", 20);
        JavaRDD<Result> day_dop_rdd = hGetter.commonRead("dpm_dws_if_m_dop", "00:" + batchGetter.getStDateDayAdd(-4, "-"), "99:" + batchGetter.getStDateDayAdd(-3, "-"), true);

        JavaRDD<DayDop> format_day_dop_rdd = day_dop_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "QTY");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "PartNo", "ModelNo", "QTY", "Platform", "WOType");
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateDayAdd(-4);
            String stDateDayEnd = batchGetter.getStDateDayAdd(-3);
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            String levelCode = "";
            String modelNo = "";
            switch (r.get(4).trim()) {
                case "BU1001":
                    levelCode = "L5";
                    modelNo = r.get(6);
                    break;
                case "BU1061":
                    levelCode = "L6";
                    modelNo = r.get(6);
                    break;
                case "BU1008":
                    levelCode = "L10";
                    modelNo = r.get(8).replace("/", "");
                    break;
                default:
                    return null;
            }
            String BGCode = "";
            if (r.get(3).trim().equals("D次")) {
                BGCode = "D";
            } else {
                return null;
            }
            return new DayDop(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), levelCode, r.get(5), modelNo, batchGetter.formatDouble(r.get(7)), r.get(8), r.get(9));
        }).distinct();

        System.out.println("dayDop========================>>>");


        /*
         * ====================================================================
         * 描述:
         * String Key
         * String Level
         * Double Normalization
         * Double Normalization_BTO
         * Double Normalization_CTO
         * ====================================================================
         */
        JavaRDD<Result> manual_normalization_Rdd = hGetter.commonRead("dpm_ods_manual_normalization", "0", "z", true, "L");
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualNormalization(r.get(0).trim(), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)));
        }).distinct();

        System.out.println("manualNormalization========================>>>");

        /*
         * ====================================================================
         * 描述:
         *
         *      E_HR人力工时
         *
         * ====================================================================
         */
        System.out.println(batchGetter.getStDateDayStampAdd(-3));
        System.out.println(batchGetter.getStDateDayStampAdd(-2));
        //JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.saltRead("dpm_dws_personnel_emp_workhours_day", batchGetter.getStDateDayStampAdd(-1), batchGetter.getStDateDayStampAdd(1), true, ":", 20);
        JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.commonRead("dpm_dws_personnel_emp_workhours_day", "0", "z", true);
        JavaRDD<EHR_MAN_HOUR> ehr_ma_hour_rdd = personnel_emp_workhours_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "workshift", "update_dt");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "attendance_qty", "act_attendance_workhours", "workshift", "update_dt");
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateWeekAdd(-1)._1;
            String stDateDayEnd = batchGetter.getStDateWeekAdd(0)._2;
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",",
                    r.get(0),
                    r.get(1),
                    r.get(2),
                    r.get(3),
                    r.get(6)
            );
        }).reduceByKey((ArrayList<String> r1v, ArrayList<String> r2v) -> {
//            Long v1long = Long.valueOf(r1v.get(7));
//            Long v2long = Long.valueOf(r2v.get(7));
            try {
                if (r1v == null) {
                    if (r2v != null) {
                        return r2v;
                    }
                } else if (r2v == null) {
                    if (r1v != null) {
                        return r1v;
                    }
                } else if (r1v == null && r2v == null) {
                    return null;
                }


                Double v1qty = batchGetter.formatDouble(r1v.get(4));
                Double v2qty = batchGetter.formatDouble(r2v.get(4));
                Double v1hours = batchGetter.formatDouble(r1v.get(5));
                Double v2hours = batchGetter.formatDouble(r2v.get(5));


                switch (r1v.get(3).trim()) {
                    case "DL1":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2V":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2F":
                        return v1qty >= v2qty ? r1v : r2v;
                    default:
                        return null;
                }
            } catch (Exception e) {
                return null;
            }

        }).filter(r -> {
            return r != null;
        }).map(t -> {
            return t._2;
        }).filter(r -> {
            return r != null;
        }).distinct().mapToPair(new PairFunction<ArrayList<String>, String, Tuple6<String, Double, Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>> call(ArrayList<String> r) throws Exception {
                //SiteCodeID
                //LevelCodeID
                //WorkDT
                //WorkShifitClass  --->>去掉
                //HumresourceCode  --->>去掉
                //AttendanceQTY    --->>并排 DL2F 只统计数量
                //AttendanceWorkhours --->>并排 DL1  DL2V 只统计工时
                //"WorkDT", "SiteCodeID", "LevelCodeID", "HumresourceCode", "AttendanceQTY", "AttendanceWorkhours"
                //这样就得到了leve级别的某种人力的人数和工时
                return new Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>>(
                        batchGetter.getStrArrayOrg(",", r.get(0).replace("-", ""), r.get(1), r.get(2)),
                        new Tuple6<String, Double, Double, Double, Double, Integer>(
                                //类型        人数          工时
                                r.get(3), batchGetter.formatDouble(r.get(4)), batchGetter.formatDouble(r.get(5)),
                                0d,
                                0d,
                                0
                        )
                );

            }
        }).reduceByKey((t6v1, t6v2) -> {
            String manType1 = "";
            Integer headCount1 = 0;
            Double manHour1 = 0d;
            if (t6v1 != null) {
                manType1 = t6v1._1();
                headCount1 = t6v1._2().intValue();
                manHour1 = t6v1._3();
            }
            String manType2 = "";
            Integer headCount2 = 0;
            Double manHour2 = 0d;
            if (t6v2 != null) {
                manType2 = t6v2._1();
                headCount2 = t6v2._2().intValue();
                manHour2 = t6v2._3();
            }

            Double dl1 = t6v1._4() + t6v2._4();
            Double dl2v = t6v1._5() + t6v2._5();
            Integer dl2f = t6v1._6() + t6v2._6();

            //DL1/DL2V /DL2F
            switch (manType1) {
                case "DL1":
                    dl1 += manHour1;
                    break;
                case "DL2V":
                    dl2v += manHour1;
                    break;
                case "DL2F":
                    dl2f += headCount1;
                    break;
            }


            switch (manType2) {
                case "DL1":
                    dl1 += manHour2;
                    break;
                case "DL2V":
                    dl2v += manHour2;
                    break;
                case "DL2F":
                    dl2f += headCount2;
                    break;
            }
            //把左数据置0，这样数据左数据的基本值只会被计算一次
            //使用余项存储每次左数据和右数据的汇总数据
            return new Tuple6<String, Double, Double, Double, Double, Integer>("", 0d, 0d, dl1, dl2v, dl2f);
        }).map(r -> {
            //提取有用的信息
            String[] splitOrgs = r._1.split(",");
            return new EHR_MAN_HOUR(
                    splitOrgs[0], splitOrgs[1], splitOrgs[2], r._2._4(), r._2._5(), r._2._6()
            );
        });


        System.out.println("dpm_dws_personnel_emp_workhours_day========================>>>");


        //dpm_ods_manual_manhour
        //{"rowKey":"20191213:D:WH:L6:DT1:TZ:NA","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_MANHOUR",
        // "columnList":[{"columnName":"BU","columnValue":"DT1"},{"columnName":"DL1_TTL_Manhour","columnValue":"4,547.0"},{"columnName":"DL2_Fixed_Manhour","columnValue":"0"},{"columnName":"DL2_Variable_Manhour","columnValue":"16.0"},{"columnName":"Date","columnValue":"20191213"},{"columnName":"Factory","columnValue":"涂装"},{"columnName":"Group","columnValue":"D"},{"columnName":"IDL_headcount","columnValue":"0"},{"columnName":"Level","columnValue":"L6"},{"columnName":"Line","columnValue":"NA"},{"columnName":"Offline_DL_fixed_headcount","columnValue":"0"},{"columnName":"Output","columnValue":"5000"},{"columnName":"Site","columnValue":"WH"}]}]}
        //dpm_dws_if_m_dop
        //{"rowKey":"05:20191225:BU1061:5YDCW:D9.5-MFF3","salt":null,
        // "columnFamily":[{"columnFamilyName":"DPM_DWS_IF_M_DOP","columnList":[{"columnName":"BGCode","columnValue":"D次"},{"columnName":"BUCode","columnValue":"BU1061"},{"columnName":"FactoryCode","columnValue":"WH"},{"columnName":"ModelNo","columnValue":"D9.5-MFF3"},{"columnName":"PartNo","columnValue":"5YDCW"},{"columnName":"QTY","columnValue":"4166.0"},{"columnName":"SBGCode","columnValue":"DTSA"},{"columnName":"WorkDate","columnValue":"20191225"}]}]}
        //dpm_ods_manual_normalization
        //{"rowKey":"L5:1A42AJ200-600-G","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_NORMALIZATION","columnList":[{"columnName":"Key","columnValue":"1A42AJ200-600-G"},{"columnName":"Level","columnValue":"L5"},{"columnName":"Normalization","columnValue":"0.082007825"},{"columnName":"Normalization_BTO","columnValue":""},{"columnName":"Normalization_CTO","columnValue":""}]}]}


        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();


        /*
         * ====================================================================
         * 描述:
         *
         *      注册方法区
         *
         * ====================================================================
         */
        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);



        /*
         * ====================================================================
         * 描述:
         *
         *      注册DataFrame
         * ====================================================================
         */
        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(format_day_dop_rdd, DayDop.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);
        Dataset<Row> ehr_man_hour_dataframe = sqlContext.createDataFrame(ehr_ma_hour_rdd, EHR_MAN_HOUR.class);

        /*
         * ====================================================================
         * 描述:
         *
         *      注册表
         * ====================================================================
         */
        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_if_m_dop");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_ods_manual_normalization");
        ehr_man_hour_dataframe.createOrReplaceTempView("ehr_man_hour");


        /*
         * ====================================================================
         * 描述:
         *
         *      预览表调试
         * ====================================================================
         */
        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour ").show();
        day_dop_dataFrame.sqlContext().sql("select * from dpm_dws_if_m_dop").show();
        manual_normalization_dataFrame.sqlContext().sql("select * from dpm_ods_manual_normalization ").show();
        ehr_man_hour_dataframe.sqlContext().sql("select * from ehr_man_hour ").show();


        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_l10_day_output.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));
        //结果查看
        List<Row> collect = rowDataset.toJavaRDD().collect();
        for (Row row : collect) {
            System.out.println(row);
        }


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    public void dayUPPH() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工具类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        HGetter hGetter = MetaGetter.getHGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        System.out.println(batchGetter.getStDateDayAdd(-1));
        System.out.println(batchGetter.getStDateDayAdd(1));
        //读取日工时数据
        //JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1), batchGetter.getStDateDayAdd(0), true);
        JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1).replace("-", ""), batchGetter.getStDateDayAdd(1).replace("-", ""), true);

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualHour(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), batchGetter.formatDouble(r.get(7)), batchGetter.formatDouble(r.get(8)), batchGetter.formatDouble(r.get(9)), batchGetter.formatDouble(r.get(10)));
        }).distinct();

        System.out.println("manualHour========================>>>");


        System.out.println(batchGetter.getStDateDayAdd(-1, "-"));
        System.out.println(batchGetter.getStDateDayAdd(0, "-"));
        //读取日产量数据
        //00:2019-12-24:BU1008:90HM:BTO
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"), true, ":", 20);
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(1, "-"), true, ":", 20);
        JavaRDD<Result> day_dop_rdd = hGetter.commonRead("dpm_dws_if_m_dop", "00:" + batchGetter.getStDateDayAdd(-1, "-"), "99:" + batchGetter.getStDateDayAdd(1, "-"), true);

        JavaRDD<DayDop> format_day_dop_rdd = day_dop_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "QTY");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "PartNo", "ModelNo", "QTY", "Platform", "WOType");
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateDayAdd(-1);
            String stDateDayEnd = batchGetter.getStDateDayAdd(0);
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            String levelCode = "";
            String modelNo = "";
            switch (r.get(4).trim()) {
                case "BU1001":
                    levelCode = "L5";
                    modelNo = r.get(6);
                    break;
                case "BU1061":
                    levelCode = "L6";
                    modelNo = r.get(6);
                    break;
                case "BU1008":
                    levelCode = "L10";
                    modelNo = r.get(8).replace("/", "");
                    break;
                default:
                    return null;
            }
            String BGCode = "";
            if (r.get(3).trim().equals("D次")) {
                BGCode = "D";
            } else {
                return null;
            }
            return new DayDop(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), levelCode, r.get(5), modelNo, batchGetter.formatDouble(r.get(7)), r.get(8), r.get(9));
        }).distinct();

        System.out.println("dayDop========================>>>");


        /*
         * ====================================================================
         * 描述:
         * String Key
         * String Level
         * Double Normalization
         * Double Normalization_BTO
         * Double Normalization_CTO
         * ====================================================================
         */
        JavaRDD<Result> manual_normalization_Rdd = hGetter.commonRead("dpm_ods_manual_normalization", "0", "z", true, "L");
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualNormalization(r.get(0).trim(), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)));
        }).distinct();

        System.out.println("manualNormalization========================>>>");

        /*
         * ====================================================================
         * 描述:
         *
         *      E_HR人力工时
         *
         * ====================================================================
         */
        System.out.println(batchGetter.getStDateDayStampAdd(-1));
        System.out.println(batchGetter.getStDateDayStampAdd(0));
        //JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.saltRead("dpm_dws_personnel_emp_workhours_day", batchGetter.getStDateDayStampAdd(-1), batchGetter.getStDateDayStampAdd(1), true, ":", 20);
        JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.commonRead("dpm_dws_personnel_emp_workhours_day", "0", "z", true);
        JavaRDD<EHR_MAN_HOUR> ehr_ma_hour_rdd = personnel_emp_workhours_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "workshift", "update_dt", "update_by", "data_from");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "attendance_qty", "act_attendance_workhours", "workshift", "update_dt", "update_by", "data_from");
        }).filter(r -> {
            try {
                long dataTime = batchGetter.unSepSimpleDateFormat.parse(r.get(0).replace("-", "")).getTime();
                long beginTime = batchGetter.unSepSimpleDateFormat.parse("20200105").getTime();
                if (dataTime >= beginTime) {
                    return "HS".equals(r.get(8)) && "DWD".equals(r.get(9));
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateDayAdd(-1);
            String stDateDayEnd = batchGetter.getStDateDayAdd(0);
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",",
                    r.get(0),
                    r.get(1),
                    r.get(2),
                    r.get(3),
                    r.get(6)
            );
        }).reduceByKey((ArrayList<String> r1v, ArrayList<String> r2v) -> {
//            Long v1long = Long.valueOf(r1v.get(7));
//            Long v2long = Long.valueOf(r2v.get(7));
            try {
                if (r1v == null) {
                    if (r2v != null) {
                        return r2v;
                    }
                } else if (r2v == null) {
                    if (r1v != null) {
                        return r1v;
                    }
                } else if (r1v == null && r2v == null) {
                    return null;
                }


                Double v1qty = batchGetter.formatDouble(r1v.get(4));
                Double v2qty = batchGetter.formatDouble(r2v.get(4));
                Double v1hours = batchGetter.formatDouble(r1v.get(5));
                Double v2hours = batchGetter.formatDouble(r2v.get(5));


                switch (r1v.get(3).trim()) {
                    case "DL1":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2V":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2F":
                        return v1qty >= v2qty ? r1v : r2v;
                    default:
                        return null;
                }
            } catch (Exception e) {
                return null;
            }

        }).filter(r -> {
            return r != null;
        }).map(t -> {
            return t._2;
        }).filter(r -> {
            return r != null;
        }).distinct().mapToPair(new PairFunction<ArrayList<String>, String, Tuple6<String, Double, Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>> call(ArrayList<String> r) throws Exception {
                //SiteCodeID
                //LevelCodeID
                //WorkDT
                //WorkShifitClass  --->>去掉
                //HumresourceCode  --->>去掉
                //AttendanceQTY    --->>并排 DL2F 只统计数量
                //AttendanceWorkhours --->>并排 DL1  DL2V 只统计工时
                //"WorkDT", "SiteCodeID", "LevelCodeID", "HumresourceCode", "AttendanceQTY", "AttendanceWorkhours"
                //这样就得到了leve级别的某种人力的人数和工时
                return new Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>>(
                        batchGetter.getStrArrayOrg(",", r.get(0).replace("-", ""), r.get(1), r.get(2)),
                        new Tuple6<String, Double, Double, Double, Double, Integer>(
                                //类型        人数          工时
                                r.get(3), batchGetter.formatDouble(r.get(4)), batchGetter.formatDouble(r.get(5)),
                                0d,
                                0d,
                                0
                        )
                );

            }
        }).reduceByKey((t6v1, t6v2) -> {
            String manType1 = "";
            Integer headCount1 = 0;
            Double manHour1 = 0d;
            if (t6v1 != null) {
                manType1 = t6v1._1();
                headCount1 = t6v1._2().intValue();
                manHour1 = t6v1._3();
            }
            String manType2 = "";
            Integer headCount2 = 0;
            Double manHour2 = 0d;
            if (t6v2 != null) {
                manType2 = t6v2._1();
                headCount2 = t6v2._2().intValue();
                manHour2 = t6v2._3();
            }

            Double dl1 = t6v1._4() + t6v2._4();
            Double dl2v = t6v1._5() + t6v2._5();
            Integer dl2f = t6v1._6() + t6v2._6();

            //DL1/DL2V /DL2F
            switch (manType1) {
                case "DL1":
                    dl1 += manHour1;
                    break;
                case "DL2V":
                    dl2v += manHour1;
                    break;
                case "DL2F":
                    dl2f += headCount1;
                    break;
            }


            switch (manType2) {
                case "DL1":
                    dl1 += manHour2;
                    break;
                case "DL2V":
                    dl2v += manHour2;
                    break;
                case "DL2F":
                    dl2f += headCount2;
                    break;
            }
            //把左数据置0，这样数据左数据的基本值只会被计算一次
            //使用余项存储每次左数据和右数据的汇总数据
            return new Tuple6<String, Double, Double, Double, Double, Integer>("", 0d, 0d, dl1, dl2v, dl2f);
        }).map(r -> {
            //提取有用的信息
            String[] splitOrgs = r._1.split(",");
            return new EHR_MAN_HOUR(splitOrgs[0], splitOrgs[1], splitOrgs[2], r._2._4(), r._2._5(), r._2._6()
            );
        });


        System.out.println("dpm_dws_personnel_emp_workhours_day========================>>>");


      /*  System.out.println(batchGetter.getStDateDayStampAdd(-1));
        System.out.println(batchGetter.getStDateDayStampAdd(0));
        //JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.saltRead("dpm_dws_personnel_emp_workhours_day", batchGetter.getStDateDayStampAdd(-1), batchGetter.getStDateDayStampAdd(1), true, ":", 20);
        JavaRDD<Result> manual_standard_value_result_rdd = hGetter.commonRead("dpm_ods_manual_standard_value", "0", "z", true);
        JavaRDD<ManualStandardValue> manual_standard_value_rdd = manual_standard_value_result_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_STANDARD_VALUE", "Plant", "site_code", "level_code", "humresource_type", "workshift", "update_dt", "update_by", "data_from");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_STANDARD_VALUE", "work_dt", "site_code", "level_code", "humresource_type", "attendance_qty", "act_attendance_workhours", "workshift", "update_dt", "update_by", "data_from");
        }).filter(r -> {
            try {
                long dataTime = batchGetter.unSepSimpleDateFormat.parse(r.get(0).replace("-", "")).getTime();
                long beginTime = batchGetter.unSepSimpleDateFormat.parse("20200105").getTime();
                if (dataTime >= beginTime) {
                    String replace = r.get(0).replace("-", "");
                    return "HS".equals(r.get(8)) && "DWD".equals(r.get(9));
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateDayAdd(-1);
            String stDateDayEnd = batchGetter.getStDateDayAdd(0);
        })


        System.out.println("dpm_ods_manual_standard_value=============================>>>");

*/
        //dpm_ods_manual_manhour
        //{"rowKey":"20191213:D:WH:L6:DT1:TZ:NA","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_MANHOUR",
        // "columnList":[{"columnName":"BU","columnValue":"DT1"},{"columnName":"DL1_TTL_Manhour","columnValue":"4,547.0"},{"columnName":"DL2_Fixed_Manhour","columnValue":"0"},{"columnName":"DL2_Variable_Manhour","columnValue":"16.0"},{"columnName":"Date","columnValue":"20191213"},{"columnName":"Factory","columnValue":"涂装"},{"columnName":"Group","columnValue":"D"},{"columnName":"IDL_headcount","columnValue":"0"},{"columnName":"Level","columnValue":"L6"},{"columnName":"Line","columnValue":"NA"},{"columnName":"Offline_DL_fixed_headcount","columnValue":"0"},{"columnName":"Output","columnValue":"5000"},{"columnName":"Site","columnValue":"WH"}]}]}
        //dpm_dws_if_m_dop
        //{"rowKey":"05:20191225:BU1061:5YDCW:D9.5-MFF3","salt":null,
        // "columnFamily":[{"columnFamilyName":"DPM_DWS_IF_M_DOP","columnList":[{"columnName":"BGCode","columnValue":"D次"},{"columnName":"BUCode","columnValue":"BU1061"},{"columnName":"FactoryCode","columnValue":"WH"},{"columnName":"ModelNo","columnValue":"D9.5-MFF3"},{"columnName":"PartNo","columnValue":"5YDCW"},{"columnName":"QTY","columnValue":"4166.0"},{"columnName":"SBGCode","columnValue":"DTSA"},{"columnName":"WorkDate","columnValue":"20191225"}]}]}
        //dpm_ods_manual_normalization
        //{"rowKey":"L5:1A42AJ200-600-G","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_NORMALIZATION","columnList":[{"columnName":"Key","columnValue":"1A42AJ200-600-G"},{"columnName":"Level","columnValue":"L5"},{"columnName":"Normalization","columnValue":"0.082007825"},{"columnName":"Normalization_BTO","columnValue":""},{"columnName":"Normalization_CTO","columnValue":""}]}]}


        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();


        /*
         * ====================================================================
         * 描述:
         *
         *      注册方法区
         *
         * ====================================================================
         */
        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);



        /*
         * ====================================================================
         * 描述:
         *
         *      注册DataFrame
         * ====================================================================
         */
        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(format_day_dop_rdd, DayDop.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);
        Dataset<Row> ehr_man_hour_dataframe = sqlContext.createDataFrame(ehr_ma_hour_rdd, EHR_MAN_HOUR.class);

        /*
         * ====================================================================
         * 描述:
         *
         *      注册表
         * ====================================================================
         */
        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_if_m_dop");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_ods_manual_normalization");
        ehr_man_hour_dataframe.createOrReplaceTempView("ehr_man_hour");


        /*
         * ====================================================================
         * 描述:
         *
         *      预览表调试
         * ====================================================================
         */
        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour ").show();
        day_dop_dataFrame.sqlContext().sql("select * from dpm_dws_if_m_dop").show();
        manual_normalization_dataFrame.sqlContext().sql("select * from dpm_ods_manual_normalization ").show();
        ehr_man_hour_dataframe.sqlContext().sql("select * from ehr_man_hour ").show();

        Dataset<Row> ehr_count = ehr_man_hour_dataframe.sqlContext().sql("select * from ehr_man_hour ");

        List<Row> collect = ehr_count.toJavaRDD().collect();
        for (Row row : collect) {
            System.out.println(row);
        }


        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_l5_6_10_day_upph.sql").replace("${etl_time}", System.currentTimeMillis() + ""));
        //结果查看
        rowDataset.show();


        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("work_date", DataTypes.createStructField("work_date", DataTypes.StringType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("site_code_desc", DataTypes.createStructField("site_code_desc", DataTypes.StringType, true));
        schemaList.put("level_code_desc", DataTypes.createStructField("level_code_desc", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));
        Class.forName("com.mysql.jdbc.Driver");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_day", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    public void weekUPPH() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工具类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        HGetter hGetter = MetaGetter.getHGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        System.out.println(batchGetter.getStDateWeekAdd(-1));
        System.out.println(batchGetter.getStDateWeekAdd(1));
        //读取日工时数据
        //JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1), batchGetter.getStDateDayAdd(0), true);
        JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateWeekAdd(-1)._1.replace("-", ""), batchGetter.getStDateWeekAdd(1)._2.replace("-", ""), true);

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualHour(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), batchGetter.formatDouble(r.get(7)), batchGetter.formatDouble(r.get(8)), batchGetter.formatDouble(r.get(9)), batchGetter.formatDouble(r.get(10)));
        }).distinct();

        System.out.println("manualHour========================>>>");


        System.out.println(batchGetter.getStDateWeekAdd(-1));
        System.out.println(batchGetter.getStDateWeekAdd(0));

        //读取日产量数据
        //00:2019-12-24:BU1008:90HM:BTO
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"), true, ":", 20);
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateWeekAdd(-1, "-")._1, batchGetter.getStDateWeekAdd(1, "-")._2, true, ":", 20);
        JavaRDD<Result> day_dop_rdd = hGetter.commonRead("dpm_dws_if_m_dop", "00:" + batchGetter.getStDateWeekAdd(-1, "-")._1, "99:" + batchGetter.getStDateWeekAdd(1, "-")._2, true);
        JavaRDD<DayDop> format_day_dop_rdd = day_dop_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "QTY");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "PartNo", "ModelNo", "QTY", "Platform", "WOType");
        }).filter(r -> {
            Tuple2<String, String> stDateWeekStart = batchGetter.getStDateWeekAdd(-1);
            Tuple2<String, String> stDateWeekEnd = batchGetter.getStDateWeekAdd(0);
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateWeekStart._1.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateWeekEnd._2.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            String levelCode = "";
            String modelNo = "";
            switch (r.get(4).trim()) {
                case "BU1001":
                    levelCode = "L5";
                    modelNo = r.get(6);
                    break;
                case "BU1061":
                    levelCode = "L6";
                    modelNo = r.get(6);
                    break;
                case "BU1008":
                    levelCode = "L10";
                    modelNo = r.get(8).replace("/", "");
                    break;
                default:
                    return null;
            }
            String BGCode = "";
            if (r.get(3).trim().equals("D次")) {
                BGCode = "D";
            } else {
                return null;
            }
            return new DayDop(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), levelCode, r.get(5), modelNo, batchGetter.formatDouble(r.get(7)), r.get(8), r.get(9));
        }).distinct();

        System.out.println("dayDop========================>>>");


        /*
         * ====================================================================
         * 描述:
         * String Key
         * String Level
         * Double Normalization
         * Double Normalization_BTO
         * Double Normalization_CTO
         * ====================================================================
         */
        JavaRDD<Result> manual_normalization_Rdd = hGetter.commonRead("dpm_ods_manual_normalization", "0", "z", true, "L");
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualNormalization(r.get(0), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)));
        }).distinct();

        System.out.println("manualNormalization========================>>>");


        /*
         * ====================================================================
         * 描述:
         *
         *      E_HR人力工时
         *
         * ====================================================================
         */
        System.out.println(batchGetter.getStDateWeekStampAdd(-1));
        System.out.println(batchGetter.getStDateWeekStampAdd(1));
        //JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.saltRead("dpm_dws_personnel_emp_workhours_day", batchGetter.getStDateWeekStampAdd(-1)._1, batchGetter.getStDateWeekStampAdd(1)._2, true, ":", 20);
        JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.commonRead("dpm_dws_personnel_emp_workhours_day", "0", "z", true);
        JavaRDD<EHR_MAN_HOUR> ehr_ma_hour_rdd = personnel_emp_workhours_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "workshift", "update_dt", "update_by", "data_from");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "attendance_qty", "act_attendance_workhours", "workshift", "update_dt", "update_by", "data_from");
        }).filter(r -> {
            try {
                long dataTime = batchGetter.unSepSimpleDateFormat.parse(r.get(0).replace("-", "")).getTime();
                long beginTime = batchGetter.unSepSimpleDateFormat.parse("20200106").getTime();
                if (dataTime >= beginTime) {
                    return "HS".equals(r.get(8)) && "DWD".equals(r.get(9));
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateWeekAdd(-1)._1;
            String stDateDayEnd = batchGetter.getStDateWeekAdd(0)._2;
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",",
                    r.get(0),
                    r.get(1),
                    r.get(2),
                    r.get(3),
                    r.get(6)
            );
        }).reduceByKey((ArrayList<String> r1v, ArrayList<String> r2v) -> {
//            Long v1long = Long.valueOf(r1v.get(7));
//            Long v2long = Long.valueOf(r2v.get(7));
            try {
                if (r1v == null) {
                    if (r2v != null) {
                        return r2v;
                    }
                } else if (r2v == null) {
                    if (r1v != null) {
                        return r1v;
                    }
                } else if (r1v == null && r2v == null) {
                    return null;
                }


                Double v1qty = batchGetter.formatDouble(r1v.get(4));
                Double v2qty = batchGetter.formatDouble(r2v.get(4));
                Double v1hours = batchGetter.formatDouble(r1v.get(5));
                Double v2hours = batchGetter.formatDouble(r2v.get(5));


                switch (r1v.get(3).trim()) {
                    case "DL1":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2V":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2F":
                        return v1qty >= v2qty ? r1v : r2v;
                    default:
                        return null;
                }
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(t -> {
            return t._2;
        }).filter(r -> {
            return r != null;
        }).distinct().mapToPair(new PairFunction<ArrayList<String>, String, Tuple6<String, Double, Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>> call(ArrayList<String> r) throws Exception {
                //SiteCodeID
                //LevelCodeID
                //WorkDT
                //WorkShifitClass  --->>去掉
                //HumresourceCode  --->>去掉
                //AttendanceQTY    --->>并排 DL2F 只统计数量
                //AttendanceWorkhours --->>并排 DL1  DL2V 只统计工时
                //"WorkDT", "SiteCodeID", "LevelCodeID", "HumresourceCode", "AttendanceQTY", "AttendanceWorkhours"
                //这样就得到了leve级别的某种人力的人数和工时
                return new Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>>(
                        batchGetter.getStrArrayOrg(",", r.get(0).replace("-", ""), r.get(1), r.get(2)),
                        new Tuple6<String, Double, Double, Double, Double, Integer>(
                                //类型        人数          工时
                                r.get(3), batchGetter.formatDouble(r.get(4)), batchGetter.formatDouble(r.get(5)),
                                0d,
                                0d,
                                0
                        )
                );
            }
        }).reduceByKey((t6v1, t6v2) -> {
            String manType1 = "";
            Integer headCount1 = 0;
            Double manHour1 = 0d;
            if (t6v1 != null) {
                manType1 = t6v1._1();
                headCount1 = t6v1._2().intValue();
                manHour1 = t6v1._3();
            }
            String manType2 = "";
            Integer headCount2 = 0;
            Double manHour2 = 0d;
            if (t6v2 != null) {
                manType2 = t6v2._1();
                headCount2 = t6v2._2().intValue();
                manHour2 = t6v2._3();
            }

            Double dl1 = t6v1._4() + t6v2._4();
            Double dl2v = t6v1._5() + t6v2._5();
            Integer dl2f = t6v1._6() + t6v2._6();

            //DL1/DL2V /DL2F
            switch (manType1) {
                case "DL1":
                    dl1 += manHour1;
                    break;
                case "DL2V":
                    dl2v += manHour1;
                    break;
                case "DL2F":
                    dl2f += headCount1;
                    break;
            }


            switch (manType2) {
                case "DL1":
                    dl1 += manHour2;
                    break;
                case "DL2V":
                    dl2v += manHour2;
                    break;
                case "DL2F":
                    dl2f += headCount2;
                    break;
            }
            //把左数据置0，这样数据左数据的基本值只会被计算一次
            //使用余项存储每次左数据和右数据的汇总数据
            return new Tuple6<String, Double, Double, Double, Double, Integer>("", 0d, 0d, dl1, dl2v, dl2f);
        }).map(r -> {
            //提取有用的信息
            String[] splitOrgs = r._1.split(",");
            return new EHR_MAN_HOUR(
                    splitOrgs[0], splitOrgs[1], splitOrgs[2], r._2._4(), r._2._5(), r._2._6()
            );
        });


        System.out.println("dpm_dws_personnel_emp_workhours_day========================>>>");


        //dpm_ods_manual_manhour
        //{"rowKey":"20191213:D:WH:L6:DT1:TZ:NA","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_MANHOUR",
        // "columnList":[{"columnName":"BU","columnValue":"DT1"},{"columnName":"DL1_TTL_Manhour","columnValue":"4,547.0"},{"columnName":"DL2_Fixed_Manhour","columnValue":"0"},{"columnName":"DL2_Variable_Manhour","columnValue":"16.0"},{"columnName":"Date","columnValue":"20191213"},{"columnName":"Factory","columnValue":"涂装"},{"columnName":"Group","columnValue":"D"},{"columnName":"IDL_headcount","columnValue":"0"},{"columnName":"Level","columnValue":"L6"},{"columnName":"Line","columnValue":"NA"},{"columnName":"Offline_DL_fixed_headcount","columnValue":"0"},{"columnName":"Output","columnValue":"5000"},{"columnName":"Site","columnValue":"WH"}]}]}
        //dpm_dws_if_m_dop
        //{"rowKey":"05:20191225:BU1061:5YDCW:D9.5-MFF3","salt":null,
        // "columnFamily":[{"columnFamilyName":"DPM_DWS_IF_M_DOP","columnList":[{"columnName":"BGCode","columnValue":"D次"},{"columnName":"BUCode","columnValue":"BU1061"},{"columnName":"FactoryCode","columnValue":"WH"},{"columnName":"ModelNo","columnValue":"D9.5-MFF3"},{"columnName":"PartNo","columnValue":"5YDCW"},{"columnName":"QTY","columnValue":"4166.0"},{"columnName":"SBGCode","columnValue":"DTSA"},{"columnName":"WorkDate","columnValue":"20191225"}]}]}
        //dpm_ods_manual_normalization
        //{"rowKey":"L5:1A42AJ200-600-G","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_NORMALIZATION","columnList":[{"columnName":"Key","columnValue":"1A42AJ200-600-G"},{"columnName":"Level","columnValue":"L5"},{"columnName":"Normalization","columnValue":"0.082007825"},{"columnName":"Normalization_BTO","columnValue":""},{"columnName":"Normalization_CTO","columnValue":""}]}]}

        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);


        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(format_day_dop_rdd, DayDop.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);
        Dataset<Row> ehr_man_hour_dataframe = sqlContext.createDataFrame(ehr_ma_hour_rdd, EHR_MAN_HOUR.class);

        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_if_m_dop");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_ods_manual_normalization");
        ehr_man_hour_dataframe.createOrReplaceTempView("ehr_man_hour");


        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour where dpm_ods_manual_manhour.Date = '20200102'").show();
        day_dop_dataFrame.sqlContext().sql("select * from dpm_dws_if_m_dop where WorkDate = '20200102'").show();
        manual_normalization_dataFrame.sqlContext().sql("select * from dpm_ods_manual_normalization limit 10").show();
        ehr_man_hour_dataframe.sqlContext().sql("select * from ehr_man_hour limit 10").show();

//        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("test.sql")
//                .replace("${etl_time}", System.currentTimeMillis() + "")
//                .replace("${etl_by}", "HS"));
        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_l5_6_10_week_upph.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));

        rowDataset.show();
        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("week_id", DataTypes.createStructField("work_date", DataTypes.IntegerType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("site_code_desc", DataTypes.createStructField("site_code_desc", DataTypes.StringType, true));
        schemaList.put("level_code_desc", DataTypes.createStructField("level_code_desc", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        Class.forName("com.mysql.jdbc.Driver");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_week", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    public void monthUPPH() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工具类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        HGetter hGetter = MetaGetter.getHGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        System.out.println(batchGetter.getStDateMonthAdd(-1));
        System.out.println(batchGetter.getStDateMonthAdd(0));
        //读取日工时数据
        //JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1), batchGetter.getStDateDayAdd(0), true);
        JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateMonthAdd(-1)._1.replace("-", ""), batchGetter.getStDateMonthAdd(1)._2.replace("-", ""), true);

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualHour(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), batchGetter.formatDouble(r.get(7)), batchGetter.formatDouble(r.get(8)), batchGetter.formatDouble(r.get(9)), batchGetter.formatDouble(r.get(10)));
        }).distinct();

        System.out.println("manualHour========================>>>");


        System.out.println(batchGetter.getStDateMonthAdd(-1));
        System.out.println(batchGetter.getStDateMonthAdd(0));
        //读取日产量数据
        //00:2019-12-24:BU1008:90HM:BTO
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"), true, ":", 20);
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateMonthAdd(-1, "-")._1, batchGetter.getStDateMonthAdd(1, "-")._2, true, ":", 20);
        JavaRDD<Result> day_dop_rdd = hGetter.commonRead("dpm_dws_if_m_dop", "00:" + batchGetter.getStDateMonthAdd(-1, "-")._1, "99:" + batchGetter.getStDateMonthAdd(1, "-")._2, true);
        JavaRDD<DayDop> format_day_dop_rdd = day_dop_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "QTY");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "PartNo", "ModelNo", "QTY", "Platform", "WOType");
        }).filter(r -> {
            return r != null;
        }).filter(r -> {
            Tuple2<String, String> stDateMonthStartAdd = batchGetter.getStDateMonthAdd(-1);
            Tuple2<String, String> stDateMonthEndAdd = batchGetter.getStDateMonthAdd(0);
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateMonthStartAdd._1.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateMonthEndAdd._2.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).map(r -> {
            String levelCode = "";
            String modelNo = "";
            switch (r.get(4).trim()) {
                case "BU1001":
                    levelCode = "L5";
                    modelNo = r.get(6);
                    break;
                case "BU1061":
                    levelCode = "L6";
                    modelNo = r.get(6);
                    break;
                case "BU1008":
                    levelCode = "L10";
                    modelNo = r.get(8).replace("/", "");
                    break;
                default:
                    return null;
            }
            String BGCode = "";
            if (r.get(3).trim().equals("D次")) {
                BGCode = "D";
            } else {
                return null;
            }
            return new DayDop(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), levelCode, r.get(5), modelNo, batchGetter.formatDouble(r.get(7)), r.get(8), r.get(9));
        }).distinct();

        System.out.println("dayDop========================>>>");


        /*
         * ====================================================================
         * 描述:
         * String Key
         * String Level
         * Double Normalization
         * Double Normalization_BTO
         * Double Normalization_CTO
         * ====================================================================
         */
        JavaRDD<Result> manual_normalization_Rdd = hGetter.commonRead("dpm_ods_manual_normalization", "0", "z", true, "L");
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualNormalization(r.get(0), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)));
        }).distinct();

        System.out.println("manualNormalization========================>>>");




        /*
         * ====================================================================
         * 描述:
         *
         *      E_HR人力工时
         *
         * ====================================================================
         */
        System.out.println(batchGetter.getStDateDayStampAdd(-1));
        System.out.println(batchGetter.getStDateDayStampAdd(1));
        //JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.saltRead("dpm_dws_personnel_emp_workhours_day", batchGetter.getStDateDayStampAdd(-1), batchGetter.getStDateDayStampAdd(1), true, ":", 20);
        JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.commonRead("dpm_dws_personnel_emp_workhours_day", "0", "z", true);
        JavaRDD<EHR_MAN_HOUR> ehr_ma_hour_rdd = personnel_emp_workhours_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "workshift", "update_dt", "update_by", "data_from");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "attendance_qty", "act_attendance_workhours", "workshift", "update_dt", "update_by", "data_from");
        }).filter(r -> {
            try {
                long dataTime = batchGetter.unSepSimpleDateFormat.parse(r.get(0).replace("-", "")).getTime();
                long beginTime = batchGetter.unSepSimpleDateFormat.parse("20200106").getTime();
                if (dataTime >= beginTime) {
                    return "HS".equals(r.get(8)) && "DWD".equals(r.get(9));
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateMonthAdd(-1)._1;
            String stDateDayEnd = batchGetter.getStDateMonthAdd(0)._2;
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",",
                    r.get(0),
                    r.get(1),
                    r.get(2),
                    r.get(3),
                    r.get(6)
            );
        }).reduceByKey((ArrayList<String> r1v, ArrayList<String> r2v) -> {
//            Long v1long = Long.valueOf(r1v.get(7));
//            Long v2long = Long.valueOf(r2v.get(7));
            try {
                if (r1v == null) {
                    if (r2v != null) {
                        return r2v;
                    }
                } else if (r2v == null) {
                    if (r1v != null) {
                        return r1v;
                    }
                } else if (r1v == null && r2v == null) {
                    return null;
                }


                Double v1qty = batchGetter.formatDouble(r1v.get(4));
                Double v2qty = batchGetter.formatDouble(r2v.get(4));
                Double v1hours = batchGetter.formatDouble(r1v.get(5));
                Double v2hours = batchGetter.formatDouble(r2v.get(5));


                switch (r1v.get(3).trim()) {
                    case "DL1":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2V":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2F":
                        return v1qty >= v2qty ? r1v : r2v;
                    default:
                        return null;
                }
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(t -> {
            return t._2;
        }).filter(r -> {
            return r != null;
        }).distinct().mapToPair(new PairFunction<ArrayList<String>, String, Tuple6<String, Double, Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>> call(ArrayList<String> r) throws Exception {
                //SiteCodeID
                //LevelCodeID
                //WorkDT
                //WorkShifitClass  --->>去掉
                //HumresourceCode  --->>去掉
                //AttendanceQTY    --->>并排 DL2F 只统计数量
                //AttendanceWorkhours --->>并排 DL1  DL2V 只统计工时
                //"WorkDT", "SiteCodeID", "LevelCodeID", "HumresourceCode", "AttendanceQTY", "AttendanceWorkhours"
                //这样就得到了leve级别的某种人力的人数和工时
                return new Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>>(
                        batchGetter.getStrArrayOrg(",", r.get(0).replace("-", ""), r.get(1), r.get(2)),
                        new Tuple6<String, Double, Double, Double, Double, Integer>(
                                //类型        人数          工时
                                r.get(3), batchGetter.formatDouble(r.get(4)), batchGetter.formatDouble(r.get(5)),
                                0d,
                                0d,
                                0
                        )
                );
            }
        }).reduceByKey((t6v1, t6v2) -> {
            String manType1 = "";
            Integer headCount1 = 0;
            Double manHour1 = 0d;
            if (t6v1 != null) {
                manType1 = t6v1._1();
                headCount1 = t6v1._2().intValue();
                manHour1 = t6v1._3();
            }
            String manType2 = "";
            Integer headCount2 = 0;
            Double manHour2 = 0d;
            if (t6v2 != null) {
                manType2 = t6v2._1();
                headCount2 = t6v2._2().intValue();
                manHour2 = t6v2._3();
            }

            Double dl1 = t6v1._4() + t6v2._4();
            Double dl2v = t6v1._5() + t6v2._5();
            Integer dl2f = t6v1._6() + t6v2._6();

            //DL1/DL2V /DL2F
            switch (manType1) {
                case "DL1":
                    dl1 += manHour1;
                    break;
                case "DL2V":
                    dl2v += manHour1;
                    break;
                case "DL2F":
                    dl2f += headCount1;
                    break;
            }


            switch (manType2) {
                case "DL1":
                    dl1 += manHour2;
                    break;
                case "DL2V":
                    dl2v += manHour2;
                    break;
                case "DL2F":
                    dl2f += headCount2;
                    break;
            }
            //把左数据置0，这样数据左数据的基本值只会被计算一次
            //使用余项存储每次左数据和右数据的汇总数据
            return new Tuple6<String, Double, Double, Double, Double, Integer>("", 0d, 0d, dl1, dl2v, dl2f);
        }).map(r -> {
            //提取有用的信息
            String[] splitOrgs = r._1.split(",");
            return new EHR_MAN_HOUR(
                    splitOrgs[0], splitOrgs[1], splitOrgs[2], r._2._4(), r._2._5(), r._2._6()
            );
        });


        //dpm_ods_manual_manhour
        //{"rowKey":"20191213:D:WH:L6:DT1:TZ:NA","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_MANHOUR",
        // "columnList":[{"columnName":"BU","columnValue":"DT1"},{"columnName":"DL1_TTL_Manhour","columnValue":"4,547.0"},{"columnName":"DL2_Fixed_Manhour","columnValue":"0"},{"columnName":"DL2_Variable_Manhour","columnValue":"16.0"},{"columnName":"Date","columnValue":"20191213"},{"columnName":"Factory","columnValue":"涂装"},{"columnName":"Group","columnValue":"D"},{"columnName":"IDL_headcount","columnValue":"0"},{"columnName":"Level","columnValue":"L6"},{"columnName":"Line","columnValue":"NA"},{"columnName":"Offline_DL_fixed_headcount","columnValue":"0"},{"columnName":"Output","columnValue":"5000"},{"columnName":"Site","columnValue":"WH"}]}]}
        //dpm_dws_if_m_dop
        //{"rowKey":"05:20191225:BU1061:5YDCW:D9.5-MFF3","salt":null,
        // "columnFamily":[{"columnFamilyName":"DPM_DWS_IF_M_DOP","columnList":[{"columnName":"BGCode","columnValue":"D次"},{"columnName":"BUCode","columnValue":"BU1061"},{"columnName":"FactoryCode","columnValue":"WH"},{"columnName":"ModelNo","columnValue":"D9.5-MFF3"},{"columnName":"PartNo","columnValue":"5YDCW"},{"columnName":"QTY","columnValue":"4166.0"},{"columnName":"SBGCode","columnValue":"DTSA"},{"columnName":"WorkDate","columnValue":"20191225"}]}]}
        //dpm_ods_manual_normalization
        //{"rowKey":"L5:1A42AJ200-600-G","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_NORMALIZATION","columnList":[{"columnName":"Key","columnValue":"1A42AJ200-600-G"},{"columnName":"Level","columnValue":"L5"},{"columnName":"Normalization","columnValue":"0.082007825"},{"columnName":"Normalization_BTO","columnValue":""},{"columnName":"Normalization_CTO","columnValue":""}]}]}

        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);

        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(format_day_dop_rdd, DayDop.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);
        Dataset<Row> ehr_man_hour_dataframe = sqlContext.createDataFrame(ehr_ma_hour_rdd, EHR_MAN_HOUR.class);

        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_if_m_dop");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_ods_manual_normalization");
        ehr_man_hour_dataframe.createOrReplaceTempView("ehr_man_hour");

        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour limit 10").show();
        day_dop_dataFrame.sqlContext().sql("select * from dpm_dws_if_m_dop limit 10").show();
        manual_normalization_dataFrame.sqlContext().sql("select * from dpm_ods_manual_normalization limit 10").show();
        ehr_man_hour_dataframe.sqlContext().sql("select * from ehr_man_hour limit 10").show();

//        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("test.sql")
//                .replace("${etl_time}", System.currentTimeMillis() + "")
//                .replace("${etl_by}", "HS"));
        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_l5_6_10_month_upph.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));

        rowDataset.show();

        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("month_id", DataTypes.createStructField("month_id", DataTypes.IntegerType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("site_code_desc", DataTypes.createStructField("site_code_desc", DataTypes.StringType, true));
        schemaList.put("level_code_desc", DataTypes.createStructField("level_code_desc", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        Class.forName("com.mysql.jdbc.Driver");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_month", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    public void quarterUPPH() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工具类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        HGetter hGetter = MetaGetter.getHGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        System.out.println(batchGetter.getStDateQuarterAdd(-1));
        System.out.println(batchGetter.getStDateQuarterAdd(1));
        //读取日工时数据
        //JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1), batchGetter.getStDateDayAdd(0), true);
        JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateQuarterAdd(-1)._1.replace("-", ""), batchGetter.getStDateQuarterAdd(1)._2.replace("-", ""), true);

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualHour(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), batchGetter.formatDouble(r.get(7)), batchGetter.formatDouble(r.get(8)), batchGetter.formatDouble(r.get(9)), batchGetter.formatDouble(r.get(10)));
        }).distinct();

        System.out.println("manualHour========================>>>");


        System.out.println(batchGetter.getStDateQuarterAdd(-1, "-")._1);
        System.out.println(batchGetter.getStDateQuarterAdd(1, "-")._2);
        //读取日产量数据
        //00:2019-12-24:BU1008:90HM:BTO
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"), true, ":", 20);
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateQuarterAdd(-1, "-")._1, batchGetter.getStDateQuarterAdd(1, "-")._2, true, ":", 20);
        JavaRDD<Result> day_dop_rdd = hGetter.commonRead("dpm_dws_if_m_dop", "00:" + batchGetter.getStDateQuarterAdd(-1, "-")._1, "99:" + batchGetter.getStDateQuarterAdd(1, "-")._2, true);
        JavaRDD<DayDop> format_day_dop_rdd = day_dop_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "QTY");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "PartNo", "ModelNo", "QTY", "Platform", "WOType");
        }).filter(r -> {
            Tuple2<String, String> stDateQuarterStartAdd = batchGetter.getStDateQuarterAdd(-1);
            Tuple2<String, String> stDateQuarterEndAdd = batchGetter.getStDateQuarterAdd(1);
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateQuarterStartAdd._1.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateQuarterEndAdd._2.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            String levelCode = "";
            String modelNo = "";
            switch (r.get(4).trim()) {
                case "BU1001":
                    levelCode = "L5";
                    modelNo = r.get(6);
                    break;
                case "BU1061":
                    levelCode = "L6";
                    modelNo = r.get(6);
                    break;
                case "BU1008":
                    levelCode = "L10";
                    modelNo = r.get(8).replace("/", "");
                    break;
                default:
                    return null;
            }
            String BGCode = "";
            if (r.get(3).trim().equals("D次")) {
                BGCode = "D";
            } else {
                return null;
            }
            return new DayDop(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), levelCode, r.get(5), modelNo, batchGetter.formatDouble(r.get(7)), r.get(8), r.get(9));
        }).distinct();

        System.out.println("dayDop========================>>>");


        /*
         * ====================================================================
         * 描述:
         * String Key
         * String Level
         * Double Normalization
         * Double Normalization_BTO
         * Double Normalization_CTO
         * ====================================================================
         */
        JavaRDD<Result> manual_normalization_Rdd = hGetter.commonRead("dpm_ods_manual_normalization", "0", "z", true, "L");
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualNormalization(r.get(0), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)));
        }).distinct();

        System.out.println("manualNormalization========================>>>");
        /*
         * ====================================================================
         * 描述:
         *
         *      E_HR人力工时
         *
         * ====================================================================
         */
        System.out.println(batchGetter.getStDateQuarterStampAdd(-1));
        System.out.println(batchGetter.getStDateQuarterStampAdd(1));
        //JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.saltRead("dpm_dws_personnel_emp_workhours_day", batchGetter.getStDateQuarterStampAdd(-1)._1, batchGetter.getStDateQuarterStampAdd(1)._2, true, ":", 20);
        JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.commonRead("dpm_dws_personnel_emp_workhours_day", "0", "z", true);
        JavaRDD<EHR_MAN_HOUR> ehr_ma_hour_rdd = personnel_emp_workhours_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "workshift", "update_dt", "update_by", "data_from");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "attendance_qty", "act_attendance_workhours", "workshift", "update_dt", "update_by", "data_from");
        }).filter(r -> {
            try {
                long dataTime = batchGetter.unSepSimpleDateFormat.parse(r.get(0).replace("-", "")).getTime();
                long beginTime = batchGetter.unSepSimpleDateFormat.parse("20200106").getTime();
                if (dataTime >= beginTime) {
                    return "HS".equals(r.get(8)) && "DWD".equals(r.get(9));
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateQuarterAdd(-1)._1;
            String stDateDayEnd = batchGetter.getStDateQuarterAdd(0)._2;
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",",
                    r.get(0),
                    r.get(1),
                    r.get(2),
                    r.get(3),
                    r.get(6)
            );
        }).reduceByKey((ArrayList<String> r1v, ArrayList<String> r2v) -> {
//            Long v1long = Long.valueOf(r1v.get(7));
//            Long v2long = Long.valueOf(r2v.get(7));
            try {
                if (r1v == null) {
                    if (r2v != null) {
                        return r2v;
                    }
                } else if (r2v == null) {
                    if (r1v != null) {
                        return r1v;
                    }
                } else if (r1v == null && r2v == null) {
                    return null;
                }


                Double v1qty = batchGetter.formatDouble(r1v.get(4));
                Double v2qty = batchGetter.formatDouble(r2v.get(4));
                Double v1hours = batchGetter.formatDouble(r1v.get(5));
                Double v2hours = batchGetter.formatDouble(r2v.get(5));


                switch (r1v.get(3).trim()) {
                    case "DL1":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2V":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2F":
                        return v1qty >= v2qty ? r1v : r2v;
                    default:
                        return null;
                }
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(t -> {
            return t._2;
        }).filter(r -> {
            return r != null;
        }).distinct().mapToPair(new PairFunction<ArrayList<String>, String, Tuple6<String, Double, Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>> call(ArrayList<String> r) throws Exception {
                //SiteCodeID
                //LevelCodeID
                //WorkDT
                //WorkShifitClass  --->>去掉
                //HumresourceCode  --->>去掉
                //AttendanceQTY    --->>并排 DL2F 只统计数量
                //AttendanceWorkhours --->>并排 DL1  DL2V 只统计工时
                //"WorkDT", "SiteCodeID", "LevelCodeID", "HumresourceCode", "AttendanceQTY", "AttendanceWorkhours"
                //这样就得到了leve级别的某种人力的人数和工时
                return new Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>>(
                        batchGetter.getStrArrayOrg(",", r.get(0).replace("-", ""), r.get(1), r.get(2)),
                        new Tuple6<String, Double, Double, Double, Double, Integer>(
                                //类型        人数          工时
                                r.get(3), batchGetter.formatDouble(r.get(4)), batchGetter.formatDouble(r.get(5)),
                                0d,
                                0d,
                                0
                        )
                );
            }
        }).reduceByKey((t6v1, t6v2) -> {
            String manType1 = "";
            Integer headCount1 = 0;
            Double manHour1 = 0d;
            if (t6v1 != null) {
                manType1 = t6v1._1();
                headCount1 = t6v1._2().intValue();
                manHour1 = t6v1._3();
            }
            String manType2 = "";
            Integer headCount2 = 0;
            Double manHour2 = 0d;
            if (t6v2 != null) {
                manType2 = t6v2._1();
                headCount2 = t6v2._2().intValue();
                manHour2 = t6v2._3();
            }

            Double dl1 = t6v1._4() + t6v2._4();
            Double dl2v = t6v1._5() + t6v2._5();
            Integer dl2f = t6v1._6() + t6v2._6();

            //DL1/DL2V /DL2F
            switch (manType1) {
                case "DL1":
                    dl1 += manHour1;
                    break;
                case "DL2V":
                    dl2v += manHour1;
                    break;
                case "DL2F":
                    dl2f += headCount1;
                    break;
            }


            switch (manType2) {
                case "DL1":
                    dl1 += manHour2;
                    break;
                case "DL2V":
                    dl2v += manHour2;
                    break;
                case "DL2F":
                    dl2f += headCount2;
                    break;
            }
            //把左数据置0，这样数据左数据的基本值只会被计算一次
            //使用余项存储每次左数据和右数据的汇总数据
            return new Tuple6<String, Double, Double, Double, Double, Integer>("", 0d, 0d, dl1, dl2v, dl2f);
        }).map(r -> {
            //提取有用的信息
            String[] splitOrgs = r._1.split(",");
            return new EHR_MAN_HOUR(
                    splitOrgs[0], splitOrgs[1], splitOrgs[2], r._2._4(), r._2._5(), r._2._6()
            );
        });

        //dpm_ods_manual_manhour
        //{"rowKey":"20191213:D:WH:L6:DT1:TZ:NA","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_MANHOUR",
        // "columnList":[{"columnName":"BU","columnValue":"DT1"},{"columnName":"DL1_TTL_Manhour","columnValue":"4,547.0"},{"columnName":"DL2_Fixed_Manhour","columnValue":"0"},{"columnName":"DL2_Variable_Manhour","columnValue":"16.0"},{"columnName":"Date","columnValue":"20191213"},{"columnName":"Factory","columnValue":"涂装"},{"columnName":"Group","columnValue":"D"},{"columnName":"IDL_headcount","columnValue":"0"},{"columnName":"Level","columnValue":"L6"},{"columnName":"Line","columnValue":"NA"},{"columnName":"Offline_DL_fixed_headcount","columnValue":"0"},{"columnName":"Output","columnValue":"5000"},{"columnName":"Site","columnValue":"WH"}]}]}
        //dpm_dws_if_m_dop
        //{"rowKey":"05:20191225:BU1061:5YDCW:D9.5-MFF3","salt":null,
        // "columnFamily":[{"columnFamilyName":"DPM_DWS_IF_M_DOP","columnList":[{"columnName":"BGCode","columnValue":"D次"},{"columnName":"BUCode","columnValue":"BU1061"},{"columnName":"FactoryCode","columnValue":"WH"},{"columnName":"ModelNo","columnValue":"D9.5-MFF3"},{"columnName":"PartNo","columnValue":"5YDCW"},{"columnName":"QTY","columnValue":"4166.0"},{"columnName":"SBGCode","columnValue":"DTSA"},{"columnName":"WorkDate","columnValue":"20191225"}]}]}
        //dpm_ods_manual_normalization
        //{"rowKey":"L5:1A42AJ200-600-G","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_NORMALIZATION","columnList":[{"columnName":"Key","columnValue":"1A42AJ200-600-G"},{"columnName":"Level","columnValue":"L5"},{"columnName":"Normalization","columnValue":"0.082007825"},{"columnName":"Normalization_BTO","columnValue":""},{"columnName":"Normalization_CTO","columnValue":""}]}]}

        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);

        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(format_day_dop_rdd, DayDop.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);
        Dataset<Row> ehr_man_hour_dataframe = sqlContext.createDataFrame(ehr_ma_hour_rdd, EHR_MAN_HOUR.class);

        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_if_m_dop");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_ods_manual_normalization");
        ehr_man_hour_dataframe.createOrReplaceTempView("ehr_man_hour");

        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour limit 10").show();
        day_dop_dataFrame.sqlContext().sql("select * from dpm_dws_if_m_dop limit 10").show();
        manual_normalization_dataFrame.sqlContext().sql("select * from dpm_ods_manual_normalization limit 10").show();
        ehr_man_hour_dataframe.sqlContext().sql("select * from ehr_man_hour limit 10").show();

        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_l5_6_10_quarter_upph.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));

        rowDataset.show();

        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("quarter_id", DataTypes.createStructField("quarter_id", DataTypes.IntegerType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("site_code_desc", DataTypes.createStructField("site_code_desc", DataTypes.StringType, true));
        schemaList.put("level_code_desc", DataTypes.createStructField("level_code_desc", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));
        Class.forName("com.mysql.jdbc.Driver");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_quarter", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    public void yearUPPH() throws Exception {
        /*
         * ====================================================================
         * 描述:
         *      初始化工具类
         * ====================================================================
         */
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        HGetter hGetter = MetaGetter.getHGetter();
        SqlGetter sqlGetter = MetaGetter.getSql();


        /*
         * ====================================================================
         * 描述:
         *      获取数据源
         * ====================================================================
         */
        //人力工时
        System.out.println(batchGetter.getStDateYearAdd(-1));
        System.out.println(batchGetter.getStDateYearAdd(0));
        //读取日工时数据
        //JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateDayAdd(-1), batchGetter.getStDateDayAdd(0), true);
        JavaRDD<Result> manual_hour_rdd = hGetter.commonRead("dpm_ods_manual_manhour", batchGetter.getStDateYearAdd(-1)._1.replace("-", ""), batchGetter.getStDateYearAdd(0)._2.replace("-", ""), true);

        /*
         * ====================================================================
         * 描述:
         *      数据清洗
         * ====================================================================
         */
        JavaRDD<ManualHour> format_manual_hour_rdd = manual_hour_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_MANHOUR", "Date", "Group", "Site", "Level", "BU", "Factory", "Line", "DL1_TTL_Manhour", "Output", "DL2_Variable_Manhour", "Offline_DL_fixed_headcount");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualHour(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), batchGetter.formatDouble(r.get(7)), batchGetter.formatDouble(r.get(8)), batchGetter.formatDouble(r.get(9)), batchGetter.formatDouble(r.get(10)));
        }).distinct();

        System.out.println("manualHour========================>>>");


        System.out.println(batchGetter.getStDateYearAdd(-1));
        System.out.println(batchGetter.getStDateYearAdd(0));
        //读取日产量数据
        //00:2019-12-24:BU1008:90HM:BTO
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"), true, ":", 20);
        //JavaRDD<Result> day_dop_rdd = hGetter.saltRead("dpm_dws_if_m_dop", batchGetter.getStDateYearAdd(-1, "-")._1, batchGetter.getStDateYearAdd(0, "-")._2, true, ":", 20);
        JavaRDD<Result> day_dop_rdd = hGetter.commonRead("dpm_dws_if_m_dop", "00:" + batchGetter.getStDateYearAdd(-1, "-")._1, "99:" + batchGetter.getStDateYearAdd(0, "-")._2, true);
        JavaRDD<DayDop> format_day_dop_rdd = day_dop_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "QTY");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_IF_M_DOP", "WorkDate", "FactoryCode", "SBGCode", "BGCode", "BUCode", "PartNo", "ModelNo", "QTY", "Platform", "WOType");
        }).filter(r -> {
            Tuple2<String, String> stDateYearStartAdd = batchGetter.getStDateYearAdd(-1);
            Tuple2<String, String> stDateYearEndAdd = batchGetter.getStDateYearAdd(0);
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateYearStartAdd._1.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateYearEndAdd._2.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            String levelCode = "";
            String modelNo = "";
            switch (r.get(4).trim()) {
                case "BU1001":
                    levelCode = "L5";
                    modelNo = r.get(6);
                    break;
                case "BU1061":
                    levelCode = "L6";
                    modelNo = r.get(6);
                    break;
                case "BU1008":
                    levelCode = "L10";
                    modelNo = r.get(8).replace("/", "");
                    break;
                default:
                    return null;
            }
            String BGCode = "";
            if (r.get(3).trim().equals("D次")) {
                BGCode = "D";
            } else {
                return null;
            }
            return new DayDop(r.get(0).replace("-", ""), r.get(1), r.get(2), r.get(3), levelCode, r.get(5), modelNo, batchGetter.formatDouble(r.get(7)), r.get(8), r.get(9));
        }).distinct();

        System.out.println("dayDop========================>>>");


        /*
         * ====================================================================
         * 描述:
         * String Key
         * String Level
         * Double Normalization
         * Double Normalization_BTO
         * Double Normalization_CTO
         * ====================================================================
         */
        JavaRDD<Result> manual_normalization_Rdd = hGetter.commonRead("dpm_ods_manual_normalization", "0", "z", true, "L");
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_MANUAL_NORMALIZATION", "Key", "Level", "Normalization", "Normalization_BTO", "Normalization_CTO");
        }).filter(r -> {
            return r != null;
        }).map(r -> {
            return new ManualNormalization(r.get(0), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)));
        }).distinct();

        System.out.println("manualNormalization========================>>>");

        /*
         * ====================================================================
         * 描述:
         *
         *      E_HR人力工时
         *
         * ====================================================================
         */
        System.out.println(batchGetter.getStDateDayStampAdd(-1));
        System.out.println(batchGetter.getStDateDayStampAdd(0));
        //JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.saltRead("dpm_dws_personnel_emp_workhours_day", batchGetter.getStDateYearStampAdd(-1)._1, batchGetter.getStDateYearStampAdd(0)._2, true, ":", 20);
        JavaRDD<Result> personnel_emp_workhours_rdd = hGetter.commonRead("dpm_dws_personnel_emp_workhours_day", "0", "z", true);
        JavaRDD<EHR_MAN_HOUR> ehr_ma_hour_rdd = personnel_emp_workhours_rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "workshift", "update_dt", "update_by", "data_from");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY", "work_dt", "site_code", "level_code", "humresource_type", "attendance_qty", "act_attendance_workhours", "workshift", "update_dt", "update_by", "data_from");
        }).filter(r -> {
            try {
                long dataTime = batchGetter.unSepSimpleDateFormat.parse(r.get(0).replace("-", "")).getTime();
                long beginTime = batchGetter.unSepSimpleDateFormat.parse("20200106").getTime();
                if (dataTime >= beginTime) {
                    return "HS".equals(r.get(8)) && "DWD".equals(r.get(9));
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }).filter(r -> {
            String stDateDayStart = batchGetter.getStDateYearAdd(-1)._1;
            String stDateDayEnd = batchGetter.getStDateYearAdd(0)._2;
            try {

                return
                        batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayStart.replace("-", ""), "yyyyMMdd", ">=")
                                &&
                                batchGetter.dateStrCompare(r.get(0).replace("-", ""), stDateDayEnd.replace("-", ""), "yyyyMMdd", "<=")
                        ;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",",
                    r.get(0),
                    r.get(1),
                    r.get(2),
                    r.get(3),
                    r.get(6)
            );
        }).reduceByKey((ArrayList<String> r1v, ArrayList<String> r2v) -> {
//            Long v1long = Long.valueOf(r1v.get(7));
//            Long v2long = Long.valueOf(r2v.get(7));
            try {
                if (r1v == null) {
                    if (r2v != null) {
                        return r2v;
                    }
                } else if (r2v == null) {
                    if (r1v != null) {
                        return r1v;
                    }
                } else if (r1v == null && r2v == null) {
                    return null;
                }


                Double v1qty = batchGetter.formatDouble(r1v.get(4));
                Double v2qty = batchGetter.formatDouble(r2v.get(4));
                Double v1hours = batchGetter.formatDouble(r1v.get(5));
                Double v2hours = batchGetter.formatDouble(r2v.get(5));


                switch (r1v.get(3).trim()) {
                    case "DL1":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2V":
                        return v1hours >= v2hours ? r1v : r2v;
                    case "DL2F":
                        return v1qty >= v2qty ? r1v : r2v;
                    default:
                        return null;
                }
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null;
        }).map(t -> {
            return t._2;
        }).filter(r -> {
            return r != null;
        }).distinct().mapToPair(new PairFunction<ArrayList<String>, String, Tuple6<String, Double, Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>> call(ArrayList<String> r) throws Exception {
                //SiteCodeID
                //LevelCodeID
                //WorkDT
                //WorkShifitClass  --->>去掉
                //HumresourceCode  --->>去掉
                //AttendanceQTY    --->>并排 DL2F 只统计数量
                //AttendanceWorkhours --->>并排 DL1  DL2V 只统计工时
                //"WorkDT", "SiteCodeID", "LevelCodeID", "HumresourceCode", "AttendanceQTY", "AttendanceWorkhours"
                //这样就得到了leve级别的某种人力的人数和工时
                return new Tuple2<String, Tuple6<String, Double, Double, Double, Double, Integer>>(
                        batchGetter.getStrArrayOrg(",", r.get(0).replace("-", ""), r.get(1), r.get(2)),
                        new Tuple6<String, Double, Double, Double, Double, Integer>(
                                //类型        人数          工时
                                r.get(3), batchGetter.formatDouble(r.get(4)), batchGetter.formatDouble(r.get(5)),
                                0d,
                                0d,
                                0
                        )
                );
            }
        }).reduceByKey((t6v1, t6v2) -> {
            String manType1 = "";
            Integer headCount1 = 0;
            Double manHour1 = 0d;
            if (t6v1 != null) {
                manType1 = t6v1._1();
                headCount1 = t6v1._2().intValue();
                manHour1 = t6v1._3();
            }
            String manType2 = "";
            Integer headCount2 = 0;
            Double manHour2 = 0d;
            if (t6v2 != null) {
                manType2 = t6v2._1();
                headCount2 = t6v2._2().intValue();
                manHour2 = t6v2._3();
            }

            Double dl1 = t6v1._4() + t6v2._4();
            Double dl2v = t6v1._5() + t6v2._5();
            Integer dl2f = t6v1._6() + t6v2._6();

            //DL1/DL2V /DL2F
            switch (manType1) {
                case "DL1":
                    dl1 += manHour1;
                    break;
                case "DL2V":
                    dl2v += manHour1;
                    break;
                case "DL2F":
                    dl2f += headCount1;
                    break;
            }


            switch (manType2) {
                case "DL1":
                    dl1 += manHour2;
                    break;
                case "DL2V":
                    dl2v += manHour2;
                    break;
                case "DL2F":
                    dl2f += headCount2;
                    break;
            }
            //把左数据置0，这样数据左数据的基本值只会被计算一次
            //使用余项存储每次左数据和右数据的汇总数据
            return new Tuple6<String, Double, Double, Double, Double, Integer>("", 0d, 0d, dl1, dl2v, dl2f);
        }).map(r -> {
            //提取有用的信息
            String[] splitOrgs = r._1.split(",");
            return new EHR_MAN_HOUR(
                    splitOrgs[0], splitOrgs[1], splitOrgs[2], r._2._4(), r._2._5(), r._2._6()
            );
        });
        //dpm_ods_manual_manhour
        //{"rowKey":"20191213:D:WH:L6:DT1:TZ:NA","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_MANHOUR",
        // "columnList":[{"columnName":"BU","columnValue":"DT1"},{"columnName":"DL1_TTL_Manhour","columnValue":"4,547.0"},{"columnName":"DL2_Fixed_Manhour","columnValue":"0"},{"columnName":"DL2_Variable_Manhour","columnValue":"16.0"},{"columnName":"Date","columnValue":"20191213"},{"columnName":"Factory","columnValue":"涂装"},{"columnName":"Group","columnValue":"D"},{"columnName":"IDL_headcount","columnValue":"0"},{"columnName":"Level","columnValue":"L6"},{"columnName":"Line","columnValue":"NA"},{"columnName":"Offline_DL_fixed_headcount","columnValue":"0"},{"columnName":"Output","columnValue":"5000"},{"columnName":"Site","columnValue":"WH"}]}]}
        //dpm_dws_if_m_dop
        //{"rowKey":"05:20191225:BU1061:5YDCW:D9.5-MFF3","salt":null,
        // "columnFamily":[{"columnFamilyName":"DPM_DWS_IF_M_DOP","columnList":[{"columnName":"BGCode","columnValue":"D次"},{"columnName":"BUCode","columnValue":"BU1061"},{"columnName":"FactoryCode","columnValue":"WH"},{"columnName":"ModelNo","columnValue":"D9.5-MFF3"},{"columnName":"PartNo","columnValue":"5YDCW"},{"columnName":"QTY","columnValue":"4166.0"},{"columnName":"SBGCode","columnValue":"DTSA"},{"columnName":"WorkDate","columnValue":"20191225"}]}]}
        //dpm_ods_manual_normalization
        //{"rowKey":"L5:1A42AJ200-600-G","salt":null,"columnFamily":[{"columnFamilyName":"DPM_MANUAL_NORMALIZATION","columnList":[{"columnName":"Key","columnValue":"1A42AJ200-600-G"},{"columnName":"Level","columnValue":"L5"},{"columnName":"Normalization","columnValue":"0.082007825"},{"columnName":"Normalization_BTO","columnValue":""},{"columnName":"Normalization_CTO","columnValue":""}]}]}

        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        sqlContext.udf().register("formatNumber", new FormatFloatNumber(), DataTypes.StringType);


        Dataset<Row> manual_manhour_dataFrame = sqlContext.createDataFrame(format_manual_hour_rdd, ManualHour.class);
        Dataset<Row> day_dop_dataFrame = sqlContext.createDataFrame(format_day_dop_rdd, DayDop.class);
        Dataset<Row> manual_normalization_dataFrame = sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class);
        Dataset<Row> ehr_man_hour_dataframe = sqlContext.createDataFrame(ehr_ma_hour_rdd, EHR_MAN_HOUR.class);

        manual_manhour_dataFrame.createOrReplaceTempView("dpm_ods_manual_manhour");
        day_dop_dataFrame.createOrReplaceTempView("dpm_dws_if_m_dop");
        manual_normalization_dataFrame.createOrReplaceTempView("dpm_ods_manual_normalization");
        ehr_man_hour_dataframe.createOrReplaceTempView("ehr_man_hour");

        manual_manhour_dataFrame.sqlContext().sql("select * from dpm_ods_manual_manhour limit 10").show();
        day_dop_dataFrame.sqlContext().sql("select * from dpm_dws_if_m_dop limit 10").show();
        manual_normalization_dataFrame.sqlContext().sql("select * from dpm_ods_manual_normalization limit 10").show();
        ehr_man_hour_dataframe.sqlContext().sql("select * from ehr_man_hour limit 10").show();

//        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("test.sql")
//                .replace("${etl_time}", System.currentTimeMillis() + "")
//                .replace("${etl_by}", "HS"));
        Dataset<Row> rowDataset = sqlContext.sql(sqlGetter.Get("calcute_l5_6_10_year_upph.sql")
                .replace("${etl_time}", System.currentTimeMillis() + ""));

        rowDataset.show();


        HashMap<String, StructField> schemaList = new HashMap<String, StructField>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("year_id", DataTypes.createStructField("year_id", DataTypes.IntegerType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_actual", DataTypes.createStructField("online_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_actual", DataTypes.createStructField("offline_var_dl_upph_actual", DataTypes.StringType, true));
        schemaList.put("level_code_id", DataTypes.createStructField("level_code_id", DataTypes.IntegerType, true));
        schemaList.put("site_code_id", DataTypes.createStructField("site_code_id", DataTypes.IntegerType, true));
        schemaList.put("site_code_desc", DataTypes.createStructField("site_code_desc", DataTypes.StringType, true));
        schemaList.put("level_code_desc", DataTypes.createStructField("level_code_desc", DataTypes.StringType, true));
        schemaList.put("online_dl_upph_target", DataTypes.createStructField("online_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_var_dl_upph_target", DataTypes.createStructField("offline_var_dl_upph_target", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_actual", DataTypes.createStructField("offline_fix_dl_headcount_actual", DataTypes.StringType, true));
        schemaList.put("offline_fix_dl_headcount_target", DataTypes.createStructField("offline_fix_dl_headcount_target", DataTypes.StringType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        Class.forName("com.mysql.jdbc.Driver");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_production_site_kpi_year", rowDataset.toJavaRDD(), schemaList, rowDataset.schema());


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
