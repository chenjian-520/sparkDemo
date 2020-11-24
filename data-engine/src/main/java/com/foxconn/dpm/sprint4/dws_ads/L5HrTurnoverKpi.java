package com.foxconn.dpm.sprint4.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint4.dws_ads.bean.HrTurnoverOutput;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple6;
import scala.collection.JavaConverters;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author cj
 * @version 1.0
 * @timestamp 2020/6/1
 *
 * dpm_dws_personnel_overview_dd
 *
 * dpm_ads_personel_emp_turnover_day/week/month/quarter/year
 *
 */
public class L5HrTurnoverKpi extends DPSparkBase {

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

        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1);
            today = batchGetter.getStDateDayAdd(0);
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-7);
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


        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(oldYearStamp));
        scan.withStopRow(Bytes.toBytes(todayStamp));


        JavaRDD<HrTurnoverOutput> HrTurnoverOutputRDD = DPHbase.rddRead("dpm_dws_personnel_overview_dd", scan).map(r -> {
           HrTurnoverOutput hrTurnoverOutput = new HrTurnoverOutput();

            String update_dt = batchGetter.resultGetColumn(r,"DPM_DWS_PERSONNEL_OVERVIEW_DD","update_dt");

            hrTurnoverOutput.setSite_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("site_code"))))
                    .setLevel_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("level_code"))))
                    .setFactory_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("factory_code"))))
                    .setProcess_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("process_code"))))
                    .setHumresource_type(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("humresource_type"))))
                    .setWork_date(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("work_dt"))))
                    .setTurnover_headcount(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("separation_qty"))))
                    .setOnjob_headcount(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PERSONNEL_OVERVIEW_DD"), Bytes.toBytes("ttl_incumbents_qty"))))
                    .setUpdate_dt(update_dt);
            return hrTurnoverOutput;
        }).filter(r->"WH".equals(r.getSite_code()) && "L5".equals(r.getLevel_code()) && r.getUpdate_dt().matches("[\\d]{13}") && r.getFactory_code()!=null && !("".equals(r.getFactory_code())) )
          .keyBy(r -> {
            return new Tuple6<>(r.getSite_code(),r.getLevel_code(),r.getFactory_code(),r.getProcess_code(),r.getHumresource_type(),r.getWork_date());
        }).coalesce(10, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return Long.parseLong(v1.getUpdate_dt()) >= Long.parseLong(v2.getUpdate_dt()) ? v1 : v2;
        }).map(r -> {
            //还原RDD
            return r._2();
        });
        DPSparkApp.getSession().createDataFrame(HrTurnoverOutputRDD, HrTurnoverOutput.class).createOrReplaceTempView("turnover_day");
        DPSparkApp.getSession().sql("select * from turnover_day").show(50);

        //加载目标值函数
        LoadKpiTarget.loadProductionTarget();

        System.out.println("/******************************* by day Turnover ********************************************/");
        Dataset<Row> temp = DPSparkApp.getSession().sql("select * from (select uuid() id , site_code,if(level_code=='','all',level_code) level_code,factory_code,work_date,sum(turnover_headcount) turnover_headcount,sum(onjob_headcount) onjob_headcount,(sum(turnover_headcount)/sum(onjob_headcount))*100 turnover_rate_actual ,cast(get_aim_target_by_key(concat_ws(\"=\",'D',site_code,level_code,factory_code, 'all', 'all', 'all'),18) AS FLOAT)*100 turnover_rate_target, unix_timestamp() etl_time from turnover_day  group by site_code , level_code,factory_code,work_date) ");
        temp.show();
        List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(temp.schema().toSeq()).asJava();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_turnover_day", temp.toJavaRDD(), structFieldList, SaveMode.Append);

        System.out.println("/******************************* by week Turnover ********************************************/");
        DPSparkApp.getSession().udf().register("calculateYearWeek",new CalculateYearWeek(), DataTypes.IntegerType);
        Dataset<Row> dataset_week = DPSparkApp.getSession().sql(metaGetter.Get("L5Turnover_week.sql"));
        dataset_week.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_turnover_week", dataset_week.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(dataset_week.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by  Lastweek Turnover ********************************************/");
        //上周最后一天
        String lastweek = batchGetter.getStDateWeekAdd(-1, "-")._2;
        DPSparkApp.getSession().udf().register("calculateYearWeek",new CalculateYearWeek(), DataTypes.IntegerType);
        Dataset<Row> dataset_lastweek = DPSparkApp.getSession().sql(metaGetter.Get("L5Turnover_lastweek.sql").replace("###","'"+lastweek+"'"));
        dataset_lastweek.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_turnover_week", dataset_lastweek.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(dataset_lastweek.schema().toSeq()).asJava(), SaveMode.Append);


        System.out.println("/******************************* by month Turnover ********************************************/");
        Dataset<Row> dataset_month = DPSparkApp.getSession().sql(metaGetter.Get("L5Turnover_month.sql"));
        dataset_month.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_turnover_month", dataset_month.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(dataset_month.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by quarter Turnover ********************************************/");
        Dataset<Row> dataset_quarter = DPSparkApp.getSession().sql(metaGetter.Get("L5Turnover_quarter.sql"));
        dataset_quarter.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_turnover_quarter", dataset_quarter.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(dataset_quarter.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by year Turnover ********************************************/");
        Dataset<Row> dataset_year = DPSparkApp.getSession().sql(metaGetter.Get("L5Turnover_year.sql"));
        dataset_year.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_personel_emp_turnover_year", dataset_year.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(dataset_year.schema().toSeq()).asJava(), SaveMode.Append);

    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
