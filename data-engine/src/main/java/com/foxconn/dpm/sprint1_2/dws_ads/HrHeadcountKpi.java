package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.TurnoverBean;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.HrHumanResourceBean;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.GetMonthTime;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.Tuple6;
import scala.Tuple7;
import scala.collection.JavaConverters;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Description:  計算DWS表dpm_dws_personnel_emp_workhours_dd
 * 至
 * ADS表dpm_ads_personel_headcount_kpi_day
 * 直接取值到對應的字段：site_code，level_code，work_dt，work_shift，humresource_type对应人力资源，attendance_qty对应人头数
 *
 * headcount人数
 * 输入表：
 *   dpm_dws_personnel_overview_dd  人力概要状态表(出勤率和离职率)
 * 输出表：
 * dpm_ads_personel_headcount_kpi_day/week/month/quarter/year  人头数KPI
 *
 * @author FL cj
 * @version 1.0
 * @timestamp 2020/4/22
 */
public class HrHeadcountKpi extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;
        String formatYesterday = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-1))));

        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1);
            today = batchGetter.getStDateDayAdd(0);
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
            todayStamp = batchGetter.getStDateDayStampAdd(0);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }

        String L5DL = "";
        String L6DL = "";
        String L10DL = "";
        String L5IDL = "";
        String L6IDL = "";
        String L10IDL = "";

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        //获取 出勤率 目标值  淘汰逻辑  用获取目标值函数替代
//        List<Tuple7<String, String, String, String, String, String, String>> collect = DPHbase.rddRead("dpm_ods_production_target_values", new Scan(), true).map(r -> {
//            return new Tuple7<>(
//                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("site_code"))),
//                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("bu_code"))),
//                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("machine_id"))),
//                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("day_id"))),
//                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("update_dt"))),
//                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("offline_fix_dl_headcount"))),
//                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("idl")))
//            );
//        }).filter(r -> ("WH".equals(r._1()) || "CQ".equals(r._1())) && r._6() != null && r._6() != "" && r._7() != null && r._7() != "")
//                .filter(r -> GetMonthTime.getStartTimeInMillis() <= simpleDateFormat.parse(r._4()).getTime() && simpleDateFormat.parse(r._4()).getTime() <= GetMonthTime.getEndTimeInMillis())
//                .collect();
//
//        for (Tuple7<String, String, String, String, String, String, String> a : collect) {
//            if ("WH".equals(a._1())) {
//                if ("L5".equals(a._2())) {
//                    L5DL = a._6();
//                    L5IDL = a._7();
//                } else if ("L6".equals(a._2())) {
//                    L6DL = a._6();
//                    L6IDL = a._7();
//                } else {
//                    L10DL = a._6();
//                    L10IDL = a._7();
//                }
//            }
//            System.out.println(a);
//        }


        //读取加盐表dpm_dws_personnel_emp_workhours_dd
        final Long etlTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(batchGetter.getStDateDayStampAdd(-10)));
        scan.withStopRow(Bytes.toBytes(todayStamp));
        System.out.println(yesterdayStamp + "--" + todayStamp);
        long timeMillis = System.currentTimeMillis();

//        String finalL5IDL = L5IDL;
//        String finalL5DL = L5DL;
//        String finalL6IDL = L6IDL;
//        String finalL6DL = L6DL;
//        String finalL10IDL = L10IDL;
//        String finalL10DL = L10DL;
        JavaRDD<HrHumanResourceBean> humanResourceBean = DPHbase.rddRead("dpm_dws_personnel_overview_dd", scan, true)
                .map(r -> {
                    HrHumanResourceBean hrHumanResourceBean = new HrHumanResourceBean();

                    String level_code = Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("level_code")));
                    if (level_code == "") {
                        hrHumanResourceBean.setLevel_code("all");
                    } else {
                        hrHumanResourceBean.setLevel_code(level_code);
                    }
                    String process_code = Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("process_code")));

                    hrHumanResourceBean.setId(UUID.randomUUID().toString())
                            .setFactory_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("factory_code"))))
                            .setSite_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("site_code"))))
                            .setProcess_code(process_code)
                            .setWork_date(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("work_dt"))))
                            .setEmp_humman_resource_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("humresource_type"))))
                            .setOnjob_headcount(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("ttl_incumbents_qty"))))
                            .setEtl_time(String.valueOf(timeMillis))
                            .setUpdate_dt(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("update_dt"))));

//                    String code = hrHumanResourceBean.getEmp_humman_resource_code();
//                    if (code == null && code.equals("")) {
//                        code = "XXX";
//                    }
//                    if ("L5".equals(hrHumanResourceBean.getLevel_code()) && code.matches("IDL.*")) {
//                        hrHumanResourceBean.setHeadcount_target(finalL5IDL);
//                    } else if ("L5".equals(hrHumanResourceBean.getLevel_code()) && code.matches("DL.*")) {
//                        hrHumanResourceBean.setHeadcount_target(finalL5DL);
//                    } else if ("L6".equals(hrHumanResourceBean.getLevel_code()) && code.matches("IDL.*")) {
//                        hrHumanResourceBean.setHeadcount_target(finalL6IDL);
//                    } else if ("L6".equals(hrHumanResourceBean.getLevel_code()) && code.matches("DL.*")) {
//                        hrHumanResourceBean.setHeadcount_target(finalL6DL);
//                    } else if ("L10".equals(hrHumanResourceBean.getLevel_code()) && code.matches("IDL.*")) {
//                        hrHumanResourceBean.setHeadcount_target(finalL10IDL);
//                    } else if ("L10".equals(hrHumanResourceBean.getLevel_code()) && code.matches("DL.*")) {
//                        hrHumanResourceBean.setHeadcount_target(finalL10DL);
//                    }
                    return hrHumanResourceBean;
                }).filter(r -> "WH".equals(r.getSite_code()) || "CQ".equals(r.getSite_code()))
                .keyBy(r -> {
                    //work_dt+site_code+level_code+factory_code+process_code+humresource_type
                    return new Tuple6<>(r.getWork_date(),r.getSite_code(),r.getLevel_code(),r.getFactory_code(),r.getProcess_code(),r.getEmp_humman_resource_code());
                }).coalesce(10, false).reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return Long.valueOf(v1.getUpdate_dt()) >= Long.valueOf(v2.getUpdate_dt()) ? v1 : v2;
                }).map(r -> {
                    //还原RDD
                    return r._2();
                });

//        humanResourceBean.filter(r->"CQ".equals(r.getSite_code())).take(10).forEach(r-> System.out.println(r));
//        humanResourceBean.take(5).forEach(r -> System.out.println(r));
        
        Dataset<Row> humanResource = DPSparkApp.getSession().createDataFrame(humanResourceBean, HrHumanResourceBean.class);
        humanResource.createOrReplaceTempView("humanResourceView");
        humanResource.show();
//        long count = humanResourceBean.count();
//        System.out.println(count);

        // 注册目标值函数 展示目标值表
        LoadKpiTarget.loadProductionTarget();
        DPSparkApp.getSession().sql("select * from dpm_ods_production_target_values").show(50);


        System.out.println("/******************************* by day hc ********************************************/");
        Dataset<Row> datasetDay = DPSparkApp.getSession().sql("select  emp_humman_resource_code,etl_time,if(factory_code='','all',factory_code) factory_code, cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,if(level_code='','all',level_code),if(factory_code='','all',factory_code),if(process_code='','all',process_code), 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target ,uuid() id, if(level_code='','all',level_code) level_code,onjob_headcount, if(process_code='','all',process_code) process_code,site_code,work_date from humanResourceView where work_date = '" + formatYesterday + "' ");

        datasetDay.createOrReplaceTempView("hcbyProcessday");
        datasetDay.show();
//        datasetDay.filter(new FilterFunction<Row>() {
//            @Override
//            public boolean call(Row value) throws Exception {
//                return value.getInt(3)!=0;
//            }
//        }).show(50);

        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_day", datasetDay.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetDay.schema().toSeq()).asJava(), SaveMode.Append);

        //by site and level and emp_humman_resource_code
        Dataset<Row> levelDay = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,emp_humman_resource_code,work_date,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,'all','all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessday group by level_code,site_code,emp_humman_resource_code,work_date");
        levelDay.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_day", levelDay.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(levelDay.schema().toSeq()).asJava(), SaveMode.Append);

        //by site and level and factory and emp_humman_resource_code
        Dataset<Row> factoryDay = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,factory_code,emp_humman_resource_code,work_date,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,factory_code,'all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessday group by level_code,site_code,factory_code,emp_humman_resource_code,work_date");
        factoryDay.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_day", factoryDay.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(factoryDay.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by week hc ********************************************/");

        //注册week的 udf函数
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
        Dataset<Row> datasetWeek = DPSparkApp.getSession().sql("select emp_humman_resource_code,etl_time,if(factory_code='','all',factory_code) factory_code,if(process_code='','all',process_code) process_code,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,if(level_code='','all',level_code),if(factory_code='','all',factory_code),if(process_code='','all',process_code), 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target,uuid() id,if(level_code='','all',level_code) level_code,onjob_headcount,site_code,cast(calculateYearWeek(work_date) AS INTEGER) week_id from (select * from humanResourceView where work_date = '" + formatYesterday + "' ) ");
        datasetWeek.createOrReplaceTempView("hcbyProcessweek");
        datasetWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_week", datasetWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetWeek.schema().toSeq()).asJava(), SaveMode.Append);


        //by site and level and factory and emp_humman_resource_code
        Dataset<Row> factoryWeek = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,factory_code,emp_humman_resource_code,cast(week_id AS INTEGER) week_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,factory_code,'all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessweek group by level_code,site_code,factory_code,emp_humman_resource_code,week_id");
        factoryWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_week", factoryWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(factoryWeek.schema().toSeq()).asJava(), SaveMode.Append);


        //by site and level and emp_humman_resource_code
        Dataset<Row> levelWeek = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,emp_humman_resource_code,cast(week_id AS INTEGER) week_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,'all','all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessweek group by level_code,site_code,emp_humman_resource_code,week_id");
        levelWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_week", levelWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(levelWeek.schema().toSeq()).asJava(), SaveMode.Append);




        System.out.println("/******************************* by Last week hc ********************************************/");
        String lastweekday = batchGetter.getStDateWeekAdd(-1, "-")._2;
        Dataset<Row> datasetLastWeek = DPSparkApp.getSession().sql("select emp_humman_resource_code,unix_timestamp() etl_time,if(factory_code='','all',factory_code) factory_code,if(process_code='','all',process_code) process_code,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,if(level_code='','all',level_code),if(factory_code='','all',factory_code),if(process_code='','all',process_code), 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target,uuid() id,if(level_code='','all',level_code) level_code,onjob_headcount,site_code,cast(calculateYearWeek(work_date) AS INTEGER) week_id from (select emp_humman_resource_code,factory_code,process_code,headcount_target,id,level_code,onjob_headcount,site_code,work_date  from humanResourceView where work_date = '" + lastweekday + "' ) ");
        datasetLastWeek.createOrReplaceTempView("hcProcessLastWeek");
        datasetLastWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_week", datasetLastWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetLastWeek.schema().toSeq()).asJava(), SaveMode.Append);

        //by site and level and factory and emp_humman_resource_code
        Dataset<Row> factoryLastWeek = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,factory_code,emp_humman_resource_code,cast(week_id AS INTEGER) week_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,factory_code,'all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcProcessLastWeek group by level_code,site_code,factory_code,emp_humman_resource_code,week_id");
        factoryLastWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_week", factoryLastWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(factoryLastWeek.schema().toSeq()).asJava(), SaveMode.Append);


        //by site and level and emp_humman_resource_code
        Dataset<Row> levelLastWeek = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,emp_humman_resource_code,cast(week_id AS INTEGER) week_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,'all','all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcProcessLastWeek group by level_code,site_code,emp_humman_resource_code,week_id");
        levelLastWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_week", levelLastWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(levelLastWeek.schema().toSeq()).asJava(), SaveMode.Append);




        System.out.println("/******************************* by month hc  ********************************************/");
        Dataset<Row> datasetMonth = DPSparkApp.getSession().sql("select emp_humman_resource_code,etl_time,if(factory_code='','all',factory_code) factory_code,if(process_code='','all',process_code) process_code,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,if(level_code='','all',level_code),if(factory_code='','all',factory_code),if(process_code='','all',process_code), 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target,uuid() id,if(level_code='','all',level_code) level_code,onjob_headcount,site_code,cast(from_unixtime(to_unix_timestamp(work_date, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id from (select * from humanResourceView where work_date = '" + formatYesterday + "' ) ");
        datasetMonth.createOrReplaceTempView("hcbyProcessmonth");
        datasetMonth.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_month", datasetMonth.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetMonth.schema().toSeq()).asJava(), SaveMode.Append);


        //by site and level and emp_humman_resource_code
        Dataset<Row> levelMonth = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,emp_humman_resource_code,month_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,'all','all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessmonth group by level_code,site_code,emp_humman_resource_code,month_id");
        levelMonth.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_month", levelMonth.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(levelMonth.schema().toSeq()).asJava(), SaveMode.Append);

        //by site and level and factory and emp_humman_resource_code
        Dataset<Row> factoryMonth = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,factory_code,emp_humman_resource_code,month_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,factory_code,'all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessmonth group by level_code,site_code,factory_code,emp_humman_resource_code,month_id");
        factoryMonth.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_month", factoryMonth.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(factoryMonth.schema().toSeq()).asJava(), SaveMode.Append);



        System.out.println("/******************************* by quarter hc  ********************************************/");
        Dataset<Row> datasetQuarter = DPSparkApp.getSession().sql("select emp_humman_resource_code,etl_time,if(factory_code='','all',factory_code) factory_code,if(process_code='','all',process_code) process_code,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,if(level_code='','all',level_code),if(factory_code='','all',factory_code),if(process_code='','all',process_code), 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target,uuid() id,if(level_code='','all',level_code) level_code,onjob_headcount,site_code,cast(concat(year(work_date), quarter(work_date)) AS INTEGER) quarter_id from (select * from humanResourceView where work_date = '" + formatYesterday + "' ) ");
        datasetQuarter.createOrReplaceTempView("hcbyProcessQuarter");
        datasetQuarter.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_quarter", datasetQuarter.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetQuarter.schema().toSeq()).asJava(), SaveMode.Append);

        //by site and level and emp_humman_resource_code
        Dataset<Row> levelQuarter = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,emp_humman_resource_code,quarter_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,'all','all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessQuarter group by level_code,site_code,emp_humman_resource_code,quarter_id");
        levelQuarter.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_quarter", levelQuarter.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(levelQuarter.schema().toSeq()).asJava(), SaveMode.Append);

        //by site and level and factory and emp_humman_resource_code
        Dataset<Row> factoryQuarter = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,factory_code,emp_humman_resource_code,quarter_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,factory_code,'all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessQuarter group by level_code,site_code,factory_code,emp_humman_resource_code,quarter_id");
        factoryQuarter.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_quarter", factoryQuarter.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(factoryQuarter.schema().toSeq()).asJava(), SaveMode.Append);



        System.out.println("/******************************* by year hc  ********************************************/");
        Dataset<Row> datasetYear = DPSparkApp.getSession().sql("select emp_humman_resource_code,etl_time,if(factory_code='','all',factory_code) factory_code,if(process_code='','all',process_code) process_code,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,if(level_code='','all',level_code),if(factory_code='','all',factory_code),if(process_code='','all',process_code), 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target,uuid() id,if(level_code='','all',level_code) level_code,onjob_headcount,site_code,year(work_date) year_id from (select * from humanResourceView where work_date = '" + formatYesterday + "' ) ");
        datasetYear.createOrReplaceTempView("hcbyProcessYear");
        datasetYear.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_year", datasetYear.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetYear.schema().toSeq()).asJava(), SaveMode.Append);



        //by site and level and emp_humman_resource_code
        Dataset<Row> levelYear = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,emp_humman_resource_code,year_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,'all','all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessYear group by level_code,site_code,emp_humman_resource_code,year_id");
        levelYear.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_year", levelYear.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(levelYear.schema().toSeq()).asJava(), SaveMode.Append);

        //by site and level and factory and emp_humman_resource_code
        Dataset<Row> factoryYear = DPSparkApp.getSession().sql("select uuid() id,site_code,level_code,factory_code,emp_humman_resource_code,year_id,sum(onjob_headcount) onjob_headcount,cast(\n" +
                "    get_aim_target_by_key(\n" +
                "    concat_ws('=','D',site_code,level_code,factory_code,'all', 'all', 'all'),\n" +
                "    case when emp_humman_resource_code='DL1V' then 8\n" +
                "    when emp_humman_resource_code='DL2V' then 9\n" +
                "    when emp_humman_resource_code='DL2F' then 10\n" +
                "     when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11\n" +
                "    end\n" +
                "  ) AS INTEGER) headcount_target, unix_timestamp() etl_time from hcbyProcessYear group by level_code,site_code,factory_code,emp_humman_resource_code,year_id");
        factoryYear.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_headcount_kpi_year", factoryYear.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(factoryYear.schema().toSeq()).asJava(), SaveMode.Append);


    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
