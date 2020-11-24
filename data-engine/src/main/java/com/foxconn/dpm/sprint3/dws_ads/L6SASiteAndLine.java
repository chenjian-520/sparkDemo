package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.L6Oee1DwdToDws;
import com.foxconn.dpm.sprint1_2.dws_ads.L6oeeDwsToAds;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.ProductionPartnoDay;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint3.dws_ads.bean.L6SASiteLineBean;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.GetMonthTime;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.ScanTableDto;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple9;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static com.foxconn.dpm.sprint1_2.dwd_dws.L6Oee1DwdToDws.notNull;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-05-26   未完成  下一阶段完成  sa 到线
 */
public class L6SASiteAndLine extends DPSparkBase {

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
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
            todayStamp = batchGetter.getStDateDayStampAdd(0);
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

        JavaRDD<L6SASiteLineBean> l6SASiteLineBeanJavaRDD = DPHbase.saltRddRead("dpm_dws_production_output_dd", oldYearStamp, todayStamp, new Scan(), true)
                .filter(r -> "WH".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("site_code")))) &&
                        "L6".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("level_code")))))
                .map(r -> {
                    return new L6SASiteLineBean(
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("site_code"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("level_code"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("factory_code"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("process_code"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("area_code"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("line_code"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("part_no"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("sku"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("platform"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("workorder_type"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("customer"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("work_dt"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("work_shift"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("output_qty"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("normalized_output_qty"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("data_granularity"))),
                            Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_OUTPUT_DD"), Bytes.toBytes("update_dt")))
                    );
                }).keyBy(r-> new Tuple9<>(r.getSite_code(),r.getLevel_code(),r.getFactory_code(),r.getProcess_code(),r.getArea_code(),r.getLine_code(),r.getPart_no(),r.getWork_dt(),r.getWork_shift()))
                .reduceByKey((v1,v2)->  Long.valueOf(v1.getUpdate_dt())>=Long.valueOf(v2.getUpdate_dt())?v1:v2)
                .map(r->r._2);
        DPSparkApp.getSession().createDataFrame(l6SASiteLineBeanJavaRDD,L6SASiteLineBean.class).createOrReplaceTempView("l6SASiteLineView");

        ScanTableDto empTurnover = new ScanTableDto();
        empTurnover.setBySalt(true);
        empTurnover.setTableName("dpm_ods_production_planning_day");
        empTurnover.setViewTabelName("productionPlanView");
        empTurnover.setStartRowKey(oldYearStamp);
        empTurnover.setEndRowKey(todayStamp);
        empTurnover.setScan(new Scan());
        DPHbase.loadDatasets(new ArrayList() {{
            add(empTurnover);
        }});


        //构造 大线  和 小线 的对应关系临时表
        Dataset<Row> dataFrame = LoadKpiTarget.getLineDateset();
        dataFrame.show();
        dataFrame.createOrReplaceTempView("lineView");

//        DPSparkApp.getSession().sql("select * from productionPlanView where level_code = 'L6'").show();
//        DPSparkApp.getSession().sql("select lineXF,line_code from (select\n" +
//                "       a.site_code site_code,\n" +
//                "       a.level_code level_code,\n" +
//                "       a.factory_code factory_code,\n" +
//                "       a.process_code process_code,\n" +
//                "       a.line_code line_code,\n" +
//                "       lineView.line lineXF,\n" +
//                "       a.part_no part_no,\n" +
//                "       a.work_dt work_dt,\n" +
//                "       a.output_qty output_qty,\n" +
//                "       a.data_granularity data_granularity\n" +
//                "       from\n" +
//                "        (select\n" +
//                "        site_code,\n" +
//                "        level_code,\n" +
//                "        factory_code,\n" +
//                "        process_code,\n" +
//                "        line_code,\n" +
//                "        part_no,\n" +
//                "        work_dt,\n" +
//                "        output_qty,\n" +
//                "        data_granularity\n" +
//                "        from\n" +
//                "        l6SASiteLineView\n" +
//                "        where data_granularity = 'line') as a\n" +
//                "        left join\n" +
//                "        lineView\n" +
//                "        on\n" +
//                "        a.line_code = lineView.line_XF) group by lineXF,line_code").show(1000);

        System.out.println("/******************************* by day ScheduleAdherence ********************************************/");
        Dataset<Row> datasetDay =DPSparkApp.getSession().sql(metaGetter.Get("L6SASiteAndLine_day.sql"));
        datasetDay.show();

        System.out.println("/******************************* by week and lastWeek ScheduleAdherence ********************************************/");
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
        Dataset<Row> datasetWeek =DPSparkApp.getSession().sql(metaGetter.Get("L6SASiteAndLine_week.sql"));
        datasetWeek.show();

        System.out.println("/******************************* by month ScheduleAdherence ********************************************/");
        Dataset<Row> datasetMonth = DPSparkApp.getSession().sql(metaGetter.Get("L6SASiteAndLine_month.sql"));
        datasetMonth.show();

        System.out.println("/******************************* by quarter ScheduleAdherence ********************************************/");
        Dataset<Row> datasetQuarter =DPSparkApp.getSession().sql(metaGetter.Get("L6SASiteAndLine_quarter.sql"));
        datasetQuarter.show();

        System.out.println("/******************************* by year ScheduleAdherence ********************************************/");
        Dataset<Row> datasetYear =DPSparkApp.getSession().sql(metaGetter.Get("L6SASiteAndLine_year.sql"));
        datasetYear.show();
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
