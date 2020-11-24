package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint3.dws_ads.bean.L6FpyLineBean;
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
import org.apache.spark.storage.StorageLevel;
import scala.Tuple10;
import scala.collection.JavaConverters;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-05-26
 * L6FPYLine
 * dpm_dws_production_pass_station_dd
 * <p>
 * dpm_ads_quality_fpy_detail_day/week/month/quarter/year
 */
public class L6FpyLine extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //业务逻辑
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

        JavaRDD<L6FpyLineBean> persist = DPHbase.saltRddRead("dpm_dws_production_pass_station_dd", oldYearStamp, todayStamp, new Scan(), true).filter(r -> {
            String station_code = Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_code")));
            return "WH".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("site_code")))) &&
                    "L6".equals(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("level_code")))) &&
                    ("ICT REPAIR".equals(station_code) || "FCT REPAIR".equals(station_code) || "R_S_VI_T".equals(station_code)
                            //SMT计入良率工站
                            || station_code.startsWith("AOI INSPECT") || station_code.startsWith("SMT INSPECT") || "S_VI_T".equals(station_code)
                            //ICT计入良率工站
                            || station_code.startsWith("ICT_") || station_code.startsWith("P_ICT")
                            //FT计入良率工站
                            || station_code.startsWith("FT-") || station_code.startsWith("FBT")
                    );
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_PASS_STATION_DD",
                            "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "sku", "platform", "customer", "work_dt", "work_shift", "part_no", "station_code"
                    ).toArray(new String[0])
            );
        }).reduceByKey((kv1, kv2) -> {
            return Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "update_dt"))
                    >
                    Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWS_PRODUCTION_PASS_STATION_DD", "update_dt"))
                    ? kv1 : kv2;
        }).map(t -> {
            return t._2;
        }).map(r -> {
            return new L6FpyLineBean(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("level_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("work_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("customer"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("fail_count"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("total_count")))
            );
        }).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<L6FpyLineBean> beanJavaRDD = persist.filter(r ->
                //SMT计入良率工站
                r.getStation_code().startsWith("AOI INSPECT") || r.getStation_code().startsWith("SMT INSPECT") || "S_VI_T".equals(r.getStation_code())
                        //ICT计入良率工站
                        || r.getStation_code().startsWith("ICT_") || r.getStation_code().startsWith("P_ICT")
                        //FT计入良率工站
                        || r.getStation_code().startsWith("FT-") || r.getStation_code().startsWith("FBT")
        );
        //注册线名对应关系
        LoadKpiTarget.getLineDateset();
        DPSparkApp.getSession().createDataFrame(beanJavaRDD, L6FpyLineBean.class).createOrReplaceTempView("dwsProductionPassStation");

        //误判产量
        JavaRDD<L6FpyLineBean> l6FpyLineFilterRDD = persist.filter(r -> "ICT REPAIR".equals(r.getStation_code()) || "FCT REPAIR".equals(r.getStation_code()) || "R_S_VI_T".equals(r.getStation_code()));
        DPSparkApp.getSession().createDataFrame(l6FpyLineFilterRDD, L6FpyLineBean.class).createOrReplaceTempView("l6FpyLineFilterView");
        Dataset<Row> dataset = DPSparkApp.getSession().sql("select customer,fail_count,level_code,LineTotranfView(line_code) line_code,site_code,station_code,total_count,work_dt from l6FpyLineFilterView");
        dataset.createOrReplaceTempView("l6FpyLineFilterView");
        DPSparkApp.getSession().sql("select * from l6FpyLineFilterView").show();

        //维修误判数量
        //2020-7-22 19:11:05 TO-DO
        JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>>
                repairRdd = DPHbase.saltRddRead("dpm_dwd_production_repair", oldYearStamp, todayStamp, new Scan(), true).map(r -> {
            return new Tuple10<>(
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "site_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "level_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "line_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "work_dt"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "customer"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "sn"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "repair_out_dt"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "fail_station"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "repair_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "update_dt")
            );
        }).filter(x -> {
            return "L6".equals(x._2()) && "WH".equals(x._1()) && !"".equals(x._7()) && !x._7().startsWith("-");
        }).keyBy(r -> {
            //sn + work_dt
            return r._6() + r._4();
        }).reduceByKey((v1, v2) -> {
            return Long.valueOf(v1._10()) > Long.valueOf(v2._10()) ? v1 : v2;
        }).map(t -> {
            return t._2;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("===================1===============");
        Dataset<Row> L6FpyLine = DPSparkApp.getSession().sql(sqlGetter.Get("L6FpyLine_Day.sql"));
        L6FpyLine.createOrReplaceTempView("L6FpyLineDayView");
        L6FpyLine.show();

        /**
         * FPY=SMT良率BY线*ICT良率BY线*FT良率BY线
         * SMT良率BY线=∑PASS QTY-∑FAILED QTY（same SN）/∑TTL QTY
         * 统计工站DELL为AOI INSPECT,SMT INSPECT21，SMT INSPECT1。H/L为S_VI_T
         * ICT良率BY线=∑PASS QTY-∑FAILED QTY（same SN）/∑TTL QTY
         * 统计工站DELL为like'ICT_'。H/L为P_ICT,P_ICT1
         * FT良率BY线=∑PASS QTY-∑FAILED QTY（same SN）/∑TTL QTY
         * 统计工站DELL为like'FT-%'。H/L为S開頭帶四位數字
         */

        // 注册目标值函数 展示目标值表
        LoadKpiTarget.loadProductionTarget();
        DPSparkApp.getSession().sql("select * from dpm_ods_production_target_values where bu_code = 'L6' ").show(50);


        /******************************* by day fyp line ********************************************/

        System.out.println("/******************************* by day fyp line ********************************************/");
        Dataset<Row> datasetDay = DPSparkApp.getSession().sql("select * from L6FpyLineDayView where work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd') ");
        datasetDay.createOrReplaceTempView("L6FpyLineView");
        datasetDay.show();
        fypL6byDay();
        Dataset<Row> datasetDetail = DPSparkApp.getSession().sql("select uuid() id ,'WH' site_code,'L6' level_code,line_code,station_code station_code,work_dt work_date,fail_count ng_qty,total_count pass_qty , unix_timestamp() etl_time from L6FpyLineView");
        datasetDetail.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_detail_day", datasetDetail.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(datasetDetail.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by week fyp line ********************************************/");
        //注册week的 udf函数
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);

        Dataset<Row> datasetWeek = DPSparkApp.getSession().sql(sqlGetter.Get("L6FpyLine_Week.sql"));
        datasetWeek.createOrReplaceTempView("L6FpyLineView");
        datasetWeek.show();
        fypL6byWeek();
        System.out.println("/******************************* by Last week fyp line ********************************************/");

        String lastweekday = batchGetter.getStDateWeekAdd(-1, "-")._1;
        String dateWeek = String.valueOf(batchGetter.getDateWeek(lastweekday));

        Dataset<Row> datasetLastWeek = DPSparkApp.getSession().sql("select * from\n" +
                "(\n" +
                "  select\n" +
                "  a.work_dt work_dt,\n" +
                "  a.station_code station_code,\n" +
                "  a.line_code line_code,\n" +
                "  sum(a.fail_count) fail_count,\n" +
                "  sum(a.total_count) total_count\n" +
                "  from\n" +
                "      (\n" +
                "        select\n" +
                "        line_code,\n" +
                "        station_code,\n" +
                "        calculateYearWeek(work_dt) work_dt,\n" +
                "        fail_count,\n" +
                "        total_count\n" +
                "        from L6FpyLineDayView\n" +
                "        ) as a\n" +
                "  group by a.work_dt,a.station_code,a.line_code\n" +
                ")\n" +
                "where work_dt = '" + dateWeek + "'");
        datasetLastWeek.createOrReplaceTempView("L6FpyLineView");
        datasetLastWeek.show();
        fypL6byWeek();

        System.out.println("/******************************* by month fyp line ********************************************/");

        Dataset<Row> datasetMonth = DPSparkApp.getSession().sql(sqlGetter.Get("L6FpyLine_Month.sql"));
        datasetMonth.createOrReplaceTempView("L6FpyLineView");
        datasetMonth.show();
        fypL6byMonth();
        System.out.println("/******************************* by quarter fyp line ********************************************/");
        Dataset<Row> datasetQuarter = DPSparkApp.getSession().sql(sqlGetter.Get("L6FpyLine_Quarter.sql"));
        datasetQuarter.createOrReplaceTempView("L6FpyLineView");
        datasetQuarter.show();
        fypL6byQuarter();
        System.out.println("/******************************* by year fyp line********************************************/");
        Dataset<Row> datasetYear = DPSparkApp.getSession().sql(sqlGetter.Get("L6FpyLine_Year.sql"));
        datasetYear.createOrReplaceTempView("L6FpyLineView");
        datasetYear.show();
        fypL6byYear();

        //释放资源
        DPSparkApp.stop();
    }

    public void fypL6byDay() throws Exception {
        //SMT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from l6FpyLineFilterView where station_code = 'R_S_VI_T' and work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd') group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewSmt");
        Dataset<Row> smt = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) smt from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'AOI INSPECT%' or station_code like 'SMT INSPECT%' or station_code= 'S_VI_T') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewSmt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt ");
//        smt.show();
        smt.createOrReplaceTempView("L6FpyLineViewSmt");
        //ICT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from l6FpyLineFilterView where station_code = 'ICT REPAIR' and work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd') group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewIct");
        Dataset<Row> ict = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ict from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'ICT_%' or station_code like 'P_ICT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewIct as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ict.show();
        ict.createOrReplaceTempView("L6FpyLineViewIct");
        //FT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from l6FpyLineFilterView where station_code = 'FCT REPAIR' and work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd') group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewFt");
        Dataset<Row> ft = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ft from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'FT-%' or station_code like 'FBT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewFt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ft.show();
        ft.createOrReplaceTempView("L6FpyLineViewFt");
        System.out.println(smt.count() + "--" + ict.count() + "--" + ft.count());

        if (smt.count() != 0 && ict.count() != 0 && ft.count() != 0) {
            Dataset<Row> dataset = DPSparkApp.getSession().sql(" select a.line_code line_code, a.work_dt work_dt, nvl(L6FpyLineViewSmt.smt,1) smt ,nvl(L6FpyLineViewIct.ict,1) ict ,nvl(L6FpyLineViewFt.ft,1) ft from (select line_code , work_dt from L6FpyLineView group by line_code ,work_dt)as a left join L6FpyLineViewSmt on a.line_code = L6FpyLineViewSmt.line_code and a.work_dt =L6FpyLineViewSmt.work_dt left join L6FpyLineViewIct on a.line_code = L6FpyLineViewIct.line_code and a.work_dt =L6FpyLineViewIct.work_dt left join L6FpyLineViewFt on a.line_code = L6FpyLineViewFt.line_code and a.work_dt =L6FpyLineViewFt.work_dt  ");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt work_date,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_day", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        } else {
            Dataset<Row> dataset = DPSparkApp.getSession().sql("select line_code , work_dt  from L6FpyLineView group by line_code ,work_dt");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            if (smt.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewSmt.smt,1) smt from l6fpy left join L6FpyLineViewSmt on l6fpy.line_code = L6FpyLineViewSmt.line_code and l6fpy.work_dt =L6FpyLineViewSmt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 smt from l6fpy  ").createOrReplaceTempView("l6fpy");
            }
            if (ict.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewIct.ict,1) ict from l6fpy left join L6FpyLineViewIct on l6fpy.line_code = L6FpyLineViewIct.line_code and l6fpy.work_dt =L6FpyLineViewIct.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ict from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            if (ft.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewFt.ft,1) ft from l6fpy left join L6FpyLineViewFt on l6fpy.line_code = L6FpyLineViewFt.line_code and l6fpy.work_dt =L6FpyLineViewFt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ft from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt work_date,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_day", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        }
    }

    public void fypL6byWeek() throws Exception {
        //SMT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,calculateYearWeek(work_dt) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'R_S_VI_T' and work_dt = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) or work_dt = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24*8)), 'yyyy-MM-dd'))  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewSmt");
        Dataset<Row> smt = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) smt from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'AOI INSPECT%' or station_code like 'SMT INSPECT%' or station_code= 'S_VI_T') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewSmt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt ");
//        smt.show();
        smt.createOrReplaceTempView("L6FpyLineViewSmt");
        //ICT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,calculateYearWeek(work_dt) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'ICT REPAIR' and work_dt = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) or work_dt = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24*8)), 'yyyy-MM-dd'))  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewIct");
        Dataset<Row> ict = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ict from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'ICT_%' or station_code like 'P_ICT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewIct as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ict.show();
        ict.createOrReplaceTempView("L6FpyLineViewIct");
        //FT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,calculateYearWeek(work_dt) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'FCT REPAIR' and work_dt = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) or work_dt = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24*8)), 'yyyy-MM-dd'))  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewFt");
        Dataset<Row> ft = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ft from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'FT-%' or station_code like 'FBT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewFt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ft.show();
        ft.createOrReplaceTempView("L6FpyLineViewFt");
        System.out.println(smt.count() + "--" + ict.count() + "--" + ft.count());

        if (smt.count() != 0 && ict.count() != 0 && ft.count() != 0) {
            Dataset<Row> dataset = DPSparkApp.getSession().sql(" select a.line_code line_code, a.work_dt work_dt, nvl(L6FpyLineViewSmt.smt,1) smt ,nvl(L6FpyLineViewIct.ict,1) ict ,nvl(L6FpyLineViewFt.ft,1) ft from (select line_code , work_dt from L6FpyLineView group by line_code ,work_dt)as a left join L6FpyLineViewSmt on a.line_code = L6FpyLineViewSmt.line_code and a.work_dt =L6FpyLineViewSmt.work_dt left join L6FpyLineViewIct on a.line_code = L6FpyLineViewIct.line_code and a.work_dt =L6FpyLineViewIct.work_dt left join L6FpyLineViewFt on a.line_code = L6FpyLineViewFt.line_code and a.work_dt =L6FpyLineViewFt.work_dt  ");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt week_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_week", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        } else {
            Dataset<Row> dataset = DPSparkApp.getSession().sql("select line_code , work_dt  from L6FpyLineView group by line_code ,work_dt");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            if (smt.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewSmt.smt,1) smt from l6fpy left join L6FpyLineViewSmt on l6fpy.line_code = L6FpyLineViewSmt.line_code and l6fpy.work_dt =L6FpyLineViewSmt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 smt from l6fpy  ").createOrReplaceTempView("l6fpy");
            }
            if (ict.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewIct.ict,1) ict from l6fpy left join L6FpyLineViewIct on l6fpy.line_code = L6FpyLineViewIct.line_code and l6fpy.work_dt =L6FpyLineViewIct.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ict from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            if (ft.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewFt.ft,1) ft from l6fpy left join L6FpyLineViewFt on l6fpy.line_code = L6FpyLineViewFt.line_code and l6fpy.work_dt =L6FpyLineViewFt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ft from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt week_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_week", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        }
    }

    public void fypL6byMonth() throws Exception {
        //SMT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'R_S_VI_T' and work_dt = cast(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyyMM') AS INTEGER)  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewSmt");
        Dataset<Row> smt = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) smt from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'AOI INSPECT%' or station_code like 'SMT INSPECT%' or station_code= 'S_VI_T') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewSmt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt ");
//        smt.show();
        smt.createOrReplaceTempView("L6FpyLineViewSmt");
        //ICT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'ICT REPAIR' and work_dt = cast(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyyMM') AS INTEGER)  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewIct");
        Dataset<Row> ict = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ict from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'ICT_%' or station_code like 'P_ICT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewIct as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ict.show();
        ict.createOrReplaceTempView("L6FpyLineViewIct");
        //FT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'FCT REPAIR' and work_dt = cast(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyyMM') AS INTEGER)  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewFt");
        Dataset<Row> ft = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ft from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'FT-%' or station_code like 'FBT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewFt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ft.show();
        ft.createOrReplaceTempView("L6FpyLineViewFt");
        System.out.println(smt.count() + "--" + ict.count() + "--" + ft.count());

        if (smt.count() != 0 && ict.count() != 0 && ft.count() != 0) {
            Dataset<Row> dataset = DPSparkApp.getSession().sql(" select a.line_code line_code, a.work_dt work_dt, nvl(L6FpyLineViewSmt.smt,1) smt ,nvl(L6FpyLineViewIct.ict,1) ict ,nvl(L6FpyLineViewFt.ft,1) ft from (select line_code , work_dt from L6FpyLineView group by line_code ,work_dt)as a left join L6FpyLineViewSmt on a.line_code = L6FpyLineViewSmt.line_code and a.work_dt =L6FpyLineViewSmt.work_dt left join L6FpyLineViewIct on a.line_code = L6FpyLineViewIct.line_code and a.work_dt =L6FpyLineViewIct.work_dt left join L6FpyLineViewFt on a.line_code = L6FpyLineViewFt.line_code and a.work_dt =L6FpyLineViewFt.work_dt  ");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt month_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_month", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        } else {
            Dataset<Row> dataset = DPSparkApp.getSession().sql("select line_code , work_dt  from L6FpyLineView group by line_code ,work_dt");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            if (smt.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewSmt.smt,1) smt from l6fpy left join L6FpyLineViewSmt on l6fpy.line_code = L6FpyLineViewSmt.line_code and l6fpy.work_dt =L6FpyLineViewSmt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 smt from l6fpy  ").createOrReplaceTempView("l6fpy");
            }
            if (ict.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewIct.ict,1) ict from l6fpy left join L6FpyLineViewIct on l6fpy.line_code = L6FpyLineViewIct.line_code and l6fpy.work_dt =L6FpyLineViewIct.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ict from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            if (ft.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewFt.ft,1) ft from l6fpy left join L6FpyLineViewFt on l6fpy.line_code = L6FpyLineViewFt.line_code and l6fpy.work_dt =L6FpyLineViewFt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ft from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt month_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_month", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        }
    }

    public void fypL6byQuarter() throws Exception {
        //SMT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'R_S_VI_T' and work_dt = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) ) AS INTEGER)  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewSmt");
        Dataset<Row> smt = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) smt from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'AOI INSPECT%' or station_code like 'SMT INSPECT%' or station_code= 'S_VI_T') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewSmt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt ");
//        smt.show();
        smt.createOrReplaceTempView("L6FpyLineViewSmt");
        //ICT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'ICT REPAIR' and work_dt = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) ) AS INTEGER)  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewIct");
        Dataset<Row> ict = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ict from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'ICT_%' or station_code like 'P_ICT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewIct as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ict.show();
        ict.createOrReplaceTempView("L6FpyLineViewIct");
        //FT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'FCT REPAIR' and work_dt = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) ) AS INTEGER)  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewFt");
        Dataset<Row> ft = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ft from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'FT-%' or station_code like 'FBT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewFt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ft.show();
        ft.createOrReplaceTempView("L6FpyLineViewFt");
        System.out.println(smt.count() + "--" + ict.count() + "--" + ft.count());

        if (smt.count() != 0 && ict.count() != 0 && ft.count() != 0) {
            Dataset<Row> dataset = DPSparkApp.getSession().sql(" select a.line_code line_code, a.work_dt work_dt, nvl(L6FpyLineViewSmt.smt,1) smt ,nvl(L6FpyLineViewIct.ict,1) ict ,nvl(L6FpyLineViewFt.ft,1) ft from (select line_code , work_dt from L6FpyLineView group by line_code ,work_dt)as a left join L6FpyLineViewSmt on a.line_code = L6FpyLineViewSmt.line_code and a.work_dt =L6FpyLineViewSmt.work_dt left join L6FpyLineViewIct on a.line_code = L6FpyLineViewIct.line_code and a.work_dt =L6FpyLineViewIct.work_dt left join L6FpyLineViewFt on a.line_code = L6FpyLineViewFt.line_code and a.work_dt =L6FpyLineViewFt.work_dt  ");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt quarter_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_quarter", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        } else {
            Dataset<Row> dataset = DPSparkApp.getSession().sql("select line_code , work_dt  from L6FpyLineView group by line_code ,work_dt");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            if (smt.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewSmt.smt,1) smt from l6fpy left join L6FpyLineViewSmt on l6fpy.line_code = L6FpyLineViewSmt.line_code and l6fpy.work_dt =L6FpyLineViewSmt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 smt from l6fpy  ").createOrReplaceTempView("l6fpy");
            }
            if (ict.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewIct.ict,1) ict from l6fpy left join L6FpyLineViewIct on l6fpy.line_code = L6FpyLineViewIct.line_code and l6fpy.work_dt =L6FpyLineViewIct.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ict from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            if (ft.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewFt.ft,1) ft from l6fpy left join L6FpyLineViewFt on l6fpy.line_code = L6FpyLineViewFt.line_code and l6fpy.work_dt =L6FpyLineViewFt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ft from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt quarter_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_quarter", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        }
    }

    public void fypL6byYear() throws Exception {
        //SMT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,year(work_dt) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'R_S_VI_T' and work_dt = year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewSmt");
        Dataset<Row> smt = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) smt from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'AOI INSPECT%' or station_code like 'SMT INSPECT%' or station_code= 'S_VI_T') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewSmt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt ");
//        smt.show();
        smt.createOrReplaceTempView("L6FpyLineViewSmt");
        //ICT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,year(work_dt) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'ICT REPAIR' and work_dt = year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewIct");
        Dataset<Row> ict = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ict from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'ICT_%' or station_code like 'P_ICT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewIct as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ict.show();
        ict.createOrReplaceTempView("L6FpyLineViewIct");
        //FT
        DPSparkApp.getSession().sql("select line_code,work_dt,sum(total_count)-sum(fail_count) total_count from (select line_code,year(work_dt) work_dt,fail_count,total_count from l6FpyLineFilterView) where station_code = 'FCT REPAIR' and work_dt = year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))  group by line_code,work_dt").createOrReplaceTempView("FpyLineFilterDayViewFt");
        Dataset<Row> ft = DPSparkApp.getSession().sql("select b.line_code line_code,b.work_dt work_dt,b.actual_count actual_count,b.total_count total_count,nvl(FpyLineFilterDayView.total_count,0) total_count2, if((b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count > 1, 1, (b.actual_count+nvl(FpyLineFilterDayView.total_count,0))/b.total_count) ft from (select a.line_code line_code,a.work_dt work_dt , sum(total_count)-sum(fail_count) actual_count,sum(total_count) total_count from (select * from L6FpyLineView where station_code like 'FT-%' or station_code like 'FBT%') as a group by a.line_code , a.work_dt) b left join FpyLineFilterDayViewFt as FpyLineFilterDayView on b.line_code=FpyLineFilterDayView.line_code and  b.work_dt=FpyLineFilterDayView.work_dt   ");
//        ft.show();
        ft.createOrReplaceTempView("L6FpyLineViewFt");
        System.out.println(smt.count() + "--" + ict.count() + "--" + ft.count());

        if (smt.count() != 0 && ict.count() != 0 && ft.count() != 0) {
            Dataset<Row> dataset = DPSparkApp.getSession().sql(" select a.line_code line_code, a.work_dt work_dt, nvl(L6FpyLineViewSmt.smt,1) smt ,nvl(L6FpyLineViewIct.ict,1) ict ,nvl(L6FpyLineViewFt.ft,1) ft from (select line_code , work_dt from L6FpyLineView group by line_code ,work_dt)as a left join L6FpyLineViewSmt on a.line_code = L6FpyLineViewSmt.line_code and a.work_dt =L6FpyLineViewSmt.work_dt left join L6FpyLineViewIct on a.line_code = L6FpyLineViewIct.line_code and a.work_dt =L6FpyLineViewIct.work_dt left join L6FpyLineViewFt on a.line_code = L6FpyLineViewFt.line_code and a.work_dt =L6FpyLineViewFt.work_dt  ");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt year_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_year", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        } else {
            Dataset<Row> dataset = DPSparkApp.getSession().sql("select line_code , work_dt  from L6FpyLineView group by line_code ,work_dt");
            dataset.show();
            dataset.createOrReplaceTempView("l6fpy");
            if (smt.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewSmt.smt,1) smt from l6fpy left join L6FpyLineViewSmt on l6fpy.line_code = L6FpyLineViewSmt.line_code and l6fpy.work_dt =L6FpyLineViewSmt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 smt from l6fpy  ").createOrReplaceTempView("l6fpy");
            }
            if (ict.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewIct.ict,1) ict from l6fpy left join L6FpyLineViewIct on l6fpy.line_code = L6FpyLineViewIct.line_code and l6fpy.work_dt =L6FpyLineViewIct.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ict from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            if (ft.count() != 0) {
                DPSparkApp.getSession().sql(" select l6fpy.*, nvl(L6FpyLineViewFt.ft,1) ft from l6fpy left join L6FpyLineViewFt on l6fpy.line_code = L6FpyLineViewFt.line_code and l6fpy.work_dt =L6FpyLineViewFt.work_dt ").createOrReplaceTempView("l6fpy");
            } else {
                DPSparkApp.getSession().sql(" select l6fpy.*, 1 ft from l6fpy ").createOrReplaceTempView("l6fpy");
            }
            Dataset<Row> L6fpyDay = DPSparkApp.getSession().sql("select uuid() id,'WH' site_code,'L6' level_code,'SMT/PTH' process_code,line_code,work_dt year_id,smt*ict*ft fpy_actual,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),16) AS FLOAT)*100 fpy_target,unix_timestamp() etl_time from l6fpy");
            L6fpyDay.show();
            List<StructField> structFieldList = (List<StructField>) JavaConverters.seqAsJavaListConverter(L6fpyDay.schema().toSeq()).asJava();
            DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_quality_fpy_year", L6fpyDay.toJavaRDD(), structFieldList, SaveMode.Append);
        }
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
