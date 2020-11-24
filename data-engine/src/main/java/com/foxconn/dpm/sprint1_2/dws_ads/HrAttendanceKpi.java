package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.sprint1_2.dws_ads.beans.HrAttendanceOutput;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.HrTurnoverOutput;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple10;
import scala.Tuple11;
import scala.Tuple12;
import scala.Tuple13;
import scala.collection.JavaConverters;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Description:  計算DWS表dpm_dws_personnel_overview_dd
 * 至
 * ADS表dpm_ads_personel_emp_attendance_day
 * 直接取值到對應的字段：目標人力，在職人力，離職人力，出勤人力
 *
 * 考勤人事
 * 输入表：
 *   dpm_dws_personnel_overview_dd  人力概要状态表(出勤率和离职率)
 * 输出表：
 * dpm_ads_personel_emp_attendance_day/week/month/quarter/year  人员出勤KPI
 *
 * @author FL cj
 * @version 1.0
 * @timestamp 2020/4/22
 */
public class HrAttendanceKpi extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter metaGetter = MetaGetter.getSql();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;
        String formatYesterday = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-1))));
        String formattoday = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(0))));

        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1);
            today = batchGetter.getStDateDayAdd(0);
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
            todayStamp = batchGetter.getStDateDayStampAdd(1);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(Long.valueOf(todayStamp));
        c.add(Calendar.YEAR, -1);
        c.add(Calendar.MONTH, -1);
        String oldYearStamp = String.valueOf(c.getTime().getTime());

        final Long etlTime = System.currentTimeMillis();
        System.out.println(etlTime);
        String today1 = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String yesterday1 = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-1))));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        //获取 出勤率 目标值
        LoadKpiTarget.loadProductionTarget();
        DPSparkApp.getSession().sql("select * from dpm_ods_production_target_values").show(50);


        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(oldYearStamp));
        scan.withStopRow(Bytes.toBytes(todayStamp));
        
        JavaRDD<HrAttendanceOutput> hrAttendanceOutput = DPHbase.rddRead("dpm_dws_personnel_overview_dd", scan).map(r -> {
            HrAttendanceOutput hrTurnoverOutput = new HrAttendanceOutput();
            hrTurnoverOutput.setSite_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("site_code"))))
                    .setLevel_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("level_code"))))
                    .setWork_date(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("work_dt"))))
                    .setAttendance_headcount(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("act_attendance_qty"))))
                    .setOnjob_headcount(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("ttl_incumbents_qty"))))
                    .setPlan_attendance_qty(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("plan_attendance_qty"))));
            return hrTurnoverOutput;
        }).filter(r->"WH".equals(r.getSite_code()) || "CQ".equals(r.getSite_code())).persist(StorageLevel.MEMORY_AND_DISK());

        String timeMillis = String.valueOf(System.currentTimeMillis());

        Dataset<Row> dataFrame = DPSparkApp.getSession().createDataFrame(hrAttendanceOutput, HrAttendanceOutput.class);
        dataFrame.createTempView("attendance_day");


        Dataset<Row> temp2 = DPSparkApp.getSession().sql("select * from (select uuid() id , site_code,if(level_code=='','N/A',level_code) level_code,work_date,sum(attendance_headcount) attendance_headcount,sum(plan_attendance_qty) onjob_headcount,(sum(attendance_headcount)/sum(plan_attendance_qty)) attendance_rate_actual , cast(get_aim_target_by_key(concat_ws('=','D',site_code,level_code,'all', 'all', 'all', 'all'),19) AS FLOAT) attendance_rate_target ,"+timeMillis+" etl_time from attendance_day group by site_code , level_code,work_date ) as b where b.work_date = '"+ formatYesterday +"' ");
        temp2.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_attendance_day", temp2.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(temp2.schema().toSeq()).asJava(), SaveMode.Append);


        timeMillis=String.valueOf(System.currentTimeMillis());
        Dataset<Row> temp1 = DPSparkApp.getSession().sql("select * from (select uuid() id , site_code,if(level_code=='','N/A',level_code) level_code,work_date,sum(attendance_headcount) attendance_headcount,sum(plan_attendance_qty) onjob_headcount,(sum(attendance_headcount)/sum(plan_attendance_qty)) attendance_rate_actual , cast(get_aim_target_by_key(concat_ws('=','D',site_code,level_code,'all', 'all', 'all', 'all'),19) AS FLOAT) attendance_rate_target ,"+timeMillis+" etl_time from attendance_day group by site_code , level_code,work_date ) as b where b.work_date = '" + formattoday + "' ");
        temp1.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_attendance_day", temp1.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(temp1.schema().toSeq()).asJava(), SaveMode.Append);




        System.out.println("/******************************* by week Attendance ********************************************/");
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
        String date1 = batchGetter.getStDateWeekAdd(0,"-")._2();
        String stDateDayAdd1 = batchGetter.getStDateDayStrAdd(date1, -1,"-");

        Dataset<Row> datasetWeek = DPSparkApp.getSession().sql(metaGetter.Get("Attendance_week.sql").replace("###",date1).replace("##",stDateDayAdd1).replace("$ETL_TIME$",timeMillis));
        datasetWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_attendance_week", datasetWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetWeek.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by lastWeek Attendance ********************************************/");

        String date = batchGetter.getStDateWeekAdd(-1,"-")._2();
        String stDateDayAdd = batchGetter.getStDateDayStrAdd(date, -1,"-");

        Dataset<Row> datasetLastWeek = DPSparkApp.getSession().sql(metaGetter.Get("Attendance_lastWeek.sql").replace("###",date).replace("##",stDateDayAdd).replace("$ETL_TIME$",timeMillis));
        datasetLastWeek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_attendance_week", datasetLastWeek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetLastWeek.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by month Attendance ********************************************/");
        Dataset<Row> datasetMonth = DPSparkApp.getSession().sql(metaGetter.Get("Attendance_month.sql").replace("$ETL_TIME$",timeMillis));
        datasetMonth.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_attendance_month", datasetMonth.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetMonth.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by quarter Attendance ********************************************/");
        Dataset<Row> datasetQuarter = DPSparkApp.getSession().sql(metaGetter.Get("Attendance_quarter.sql").replace("$ETL_TIME$",timeMillis));
        datasetQuarter.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_attendance_quarter", datasetQuarter.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetQuarter.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by year Attendance ********************************************/");
        Dataset<Row> datasetYear = DPSparkApp.getSession().sql(metaGetter.Get("Attendance_year.sql").replace("$ETL_TIME$",timeMillis));
        datasetYear.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_attendance_year", datasetYear.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetYear.schema().toSeq()).asJava(), SaveMode.Append);


    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
