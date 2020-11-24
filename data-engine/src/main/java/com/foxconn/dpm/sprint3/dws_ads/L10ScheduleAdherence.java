package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint3.dws_ads.bean.L6SASiteLineBean;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.ScanTableDto;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple9;
import scala.collection.JavaConverters;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-05-26
 *   完成  L10SA 指标遗弃， 无用代码， 计算逻辑到线，到厂，可参考
 */
public class L10ScheduleAdherence extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter metaGetter = MetaGetter.getSql();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //获取传入的时间
//        String yesterday = null;
//        String today = null;
//        String yesterdayStamp = null;
//        String todayStamp = null;
//        String formatYesterday = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-1))));
//
//        if (map.get("workDate") == null) {
//            //初始化时间
//            yesterday = batchGetter.getStDateDayAdd(-1);
//            today = batchGetter.getStDateDayAdd(0);
//            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
//            todayStamp = batchGetter.getStDateDayStampAdd(0);
//        } else {
//            yesterday = map.get("workDate").toString();
//            today = map.get("workDate").toString();
//            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
//            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
//        }
//
//        Calendar c = Calendar.getInstance();
//        c.setTimeInMillis(Long.valueOf(todayStamp));
//        c.add(Calendar.YEAR, -1);
//        c.add(Calendar.MONTH, -1);
//        String oldYearStamp = String.valueOf(c.getTime().getTime());
//
//        //L6，L10分子都是一个表 用的对象相同
//        JavaRDD<L6SASiteLineBean> l10SASiteLineBeanJavaRDD = DPHbase.saltRddRead("dpm_dws_production_output_dd", oldYearStamp, todayStamp, new Scan(), true)
//                .filter(r -> "WH".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("site_code")))) &&
//                        "process".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("data_granularity"))))&&
//                        "ASSEMBLY1".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("process_code"))))&&
//                        "L10".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("level_code")))))
//                .map(r -> {
//                    return new L6SASiteLineBean(
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("site_code"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("level_code"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("factory_code"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("process_code"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("area_code"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("line_code"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("part_no"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("sku"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("platform"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("workorder_type"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("customer"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("work_dt"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("work_shift"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("output_qty"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("normalized_output_qty"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("data_granularity"))),
//                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("update_dt")))
//                    );
//                }).keyBy(r-> new Tuple9<>(r.getSite_code(),r.getLevel_code(),r.getFactory_code(),r.getProcess_code(),r.getArea_code(),r.getLine_code(),r.getPart_no(),r.getWork_dt(),r.getWork_shift()))
//                .reduceByKey((v1,v2)->  Long.valueOf(v1.getUpdate_dt())>=Long.valueOf(v2.getUpdate_dt())?v1:v2)
//                .map(r->r._2);
//        DPSparkApp.getSession().createDataFrame(l10SASiteLineBeanJavaRDD,L6SASiteLineBean.class).createOrReplaceTempView("l10SASiteLineView");
//
//        ScanTableDto empTurnover = new ScanTableDto();
//        empTurnover.setBySalt(true);
//        empTurnover.setTableName("dpm_ods_production_planning_day");
//        empTurnover.setViewTabelName("productionPlanView");
//        empTurnover.setStartRowKey(oldYearStamp);
//        empTurnover.setEndRowKey(todayStamp);
//        empTurnover.setScan(new Scan());
//        DPHbase.loadDatasets(new ArrayList() {{
//            add(empTurnover);
//        }});
//
//        //注册目标值函数
////        LoadKpiTarget.loadProductionTarget();
//        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
//
//        System.out.println("/******************************* by day ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetDay1 =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherenceCustomer_day.sql"));
//        datasetDay1.show();
//        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_day", datasetDay1.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetDay1.schema().toSeq()).asJava(), SaveMode.Append);
//
//        System.out.println("/******************************* by week and lastWeek ScheduleAdherence ********************************************/");
//
//        Dataset<Row> datasetWeek1 =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherenceCustomer_week.sql"));
//        datasetWeek1.show();
//        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_week", datasetWeek1.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetWeek1.schema().toSeq()).asJava(), SaveMode.Append);
//
//        System.out.println("/******************************* by month ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetMonth1 = DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherenceCustomer_month.sql"));
//        datasetMonth1.show();
//        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_month", datasetMonth1.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetMonth1.schema().toSeq()).asJava(), SaveMode.Append);
//
//        System.out.println("/******************************* by quarter ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetQuarter1 =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherenceCustomer_quarter.sql"));
//        datasetQuarter1.show();
//        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_quarter", datasetQuarter1.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetQuarter1.schema().toSeq()).asJava(), SaveMode.Append);
//
//        System.out.println("/******************************* by year ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetYear1 =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherenceCustomer_year.sql"));
//        datasetYear1.show();
//        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_schedule_adherence_year", datasetYear1.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetYear1.schema().toSeq()).asJava(), SaveMode.Append);

        // L10Sa 到线
//        System.out.println("/******************************* by day ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetDay =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherence_day.sql"));
//        datasetDay.show();
//
//        System.out.println("/******************************* by week and lastWeek ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetWeek =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherence_week.sql"));
//        datasetWeek.show();
//
//        System.out.println("/******************************* by month ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetMonth = DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherence_month.sql"));
//        datasetMonth.show();
//
//        System.out.println("/******************************* by quarter ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetQuarter =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherence_quarter.sql"));
//        datasetQuarter.show();
//
//        System.out.println("/******************************* by year ScheduleAdherence ********************************************/");
//        Dataset<Row> datasetYear =DPSparkApp.getSession().sql(metaGetter.Get("L10ScheduleAdherence_year.sql"));
//        datasetYear.show();
        LoadKpiTarget.loadProductionTarget();
        DPSparkApp.getSession().sql("select * from dpm_ods_production_target_values").show(50);



    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
