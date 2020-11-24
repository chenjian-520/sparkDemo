package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.sprint1_2.dws_ads.beans.ProductionPartnoDay;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
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
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple7;


import java.text.SimpleDateFormat;
import java.util.*;

import static com.foxconn.dpm.sprint1_2.dwd_dws.L6Oee1DwdToDws.notNull;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-05-11
 * L6OEE2Line
 *
 * dpm_dws_production_partno_dd     dpm_dws_production_equipment_line_dd
 *
 * dpm_ads_production_oee_day/week/month/quarter/year
 */
public class L6Oee2Line extends DPSparkBase {

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
        Calendar c = Calendar.getInstance();

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
        c.setTimeInMillis(Long.valueOf(todayStamp));
        c.add(Calendar.YEAR, -1);
        c.add(Calendar.MONTH, -1);
        String oldYearStamp = String.valueOf(c.getTime().getTime());

        // work_dt+site_code+level_code+line_code+part_no+platform  联合唯一去重取最新时间
        JavaRDD<ProductionPartnoDay> partnoRdd = DPHbase.saltRddRead("dpm_dws_production_partno_dd", oldYearStamp, todayStamp, new Scan(), true).map(r -> {
            return new ProductionPartnoDay(
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("site_code")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("level_code")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("factory_code")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("process_code")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("area_code")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("line_code")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("part_no")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("platform")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("work_dt")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("work_shift")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("smt_ttl_pass")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("output_qty")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("ct")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_dt")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_by")))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("data_from")))));
        }).filter(r -> "WH".equals(r.getSite_code()) && "L6".equals(r.getLevel_code())).keyBy(r -> new Tuple7<>(r.getWork_dt(), r.getSite_code(), r.getLevel_code(), r.getLine_code(), r.getPart_no(), r.getPlantform(), r.getData_from()))
                .coalesce(10, false).reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return Long.valueOf(v1.getUpdate_dt()) >= Long.valueOf(v2.getUpdate_dt()) ? v1 : v2;
                }).map(r -> {
                    //还原RDD
                    return r._2();
                });
        Dataset<Row> partnoFrame = DPSparkApp.getSession().createDataFrame(partnoRdd, ProductionPartnoDay.class);
        partnoFrame.createOrReplaceTempView("dpm_dws_production_partno_day");
        Dataset<Row> Oee2DayDateSet = DPSparkApp.getSession().sql(metaGetter.Get("sprint3_L6Oee2Line.sql"));
        Oee2DayDateSet.createOrReplaceTempView("Oee2DayDateSet");
        partnoFrame.show();

        // 分母
        ScanTableDto empTurnover = new ScanTableDto();
        empTurnover.setBySalt(true);
        empTurnover.setTableName("dpm_dws_production_equipment_line_dd");
        empTurnover.setViewTabelName("productionEquipment");
        empTurnover.setStartRowKey(oldYearStamp);
        empTurnover.setEndRowKey(todayStamp);
        empTurnover.setScan(new Scan());
        DPHbase.loadDatasets(new ArrayList() {{
            add(empTurnover);
        }});

        Dataset<Row> EquipmentDataset = DPSparkApp.getSession().sql("select site_code,level_code,line_code,work_dt,planned_downtime_loss_hours,production_time,update_dt,if((production_time-planned_downtime_loss_hours)<0,0,production_time-planned_downtime_loss_hours) as production_time_day from productionEquipment where site_code='WH' and level_code='L6' ");
        EquipmentDataset.createTempView("L6productionEquipment");
        EquipmentDataset.show();

        HashMap<String, StructField> schemaList = new HashMap<>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("process_code", DataTypes.createStructField("process_code", DataTypes.StringType, true));
        schemaList.put("work_date", DataTypes.createStructField("work_date", DataTypes.StringType, true));
        schemaList.put("line_code", DataTypes.createStructField("line_code", DataTypes.StringType, true));
        schemaList.put("oee2_actual", DataTypes.createStructField("oee2_actual", DataTypes.DoubleType, true));
        schemaList.put("oee1_actual", DataTypes.createStructField("oee1_actual", DataTypes.FloatType, true));
        schemaList.put("oee2_target", DataTypes.createStructField("oee2_target", DataTypes.FloatType, true));
        schemaList.put("etltime", DataTypes.createStructField("etltime", DataTypes.LongType, true));


        // 注册目标值函数 展示目标值表
        LoadKpiTarget.loadProductionTarget();
        DPSparkApp.getSession().sql("select * from dpm_ods_production_target_values where bu_code = 'L6' ").show(50);


        System.out.println("/******************************* by day oee2 line ********************************************/");
        oee2L6byDay(Oee2DayDateSet, formatYesterday, schemaList);
        System.out.println("/******************************* by week oee2 line ********************************************/");
        //注册week的 udf函数
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);

        oee2L6byWeek(formatYesterday, schemaList);
        System.out.println("/******************************* by Last week oee2 line ********************************************/");
        oee2L6byLastWeek(schemaList);
        System.out.println("/******************************* by month oee2 line ********************************************/");
        oee2L6byMonth(schemaList);
        System.out.println("/******************************* by quarter oee2 line ********************************************/");
        oee2L6byQuarter(schemaList);
        System.out.println("/******************************* by year oee1 oee2 ********************************************/");
        oee2L6byYear(formatYesterday, schemaList);

    }


    public void oee2L6byDay(Dataset<Row> Oee2DayDateSet, String formatYesterday, HashMap<String, StructField> schemaList) throws Exception {
        Dataset<Row> filter = Oee2DayDateSet.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row value) throws Exception {
                return value.getString(2).equals(formatYesterday);
            }
        });
        filter.show(100);
        System.out.println(filter.count());
        filter.createTempView("Oee2DayDateSetDay");
        String sql = "select  id,site_code,level_code,process_code,line_code, actual_output_qty,total_worktime_oee2,total_worktime_oee1, work_date,   etltime,oee2_actual,oee1_actual ,cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),15) AS FLOAT)*100 oee2_target from (select uuid() id ,nvl(a.site_code,'WH') site_code,nvl(a.level_code,'L6') level_code,'SMT/PTH' process_code,Oee2DayDateSetDay.line_code line_code,Oee2DayDateSetDay.oee2_actual actual_output_qty,nvl(a.production_time_day,23.5)*60*60 total_worktime_oee2 ,nvl(a.production_time,24)*60*60  total_worktime_oee1,Oee2DayDateSetDay.work_dt work_date,unix_timestamp() etltime , nvl((Oee2DayDateSetDay.oee2_actual/(nvl(a.production_time_day,23.5)*3600))*100,0) oee2_actual,cast(nvl((Oee2DayDateSetDay.oee2_actual/(nvl(a.production_time,24)*3600))*100,0) AS FLOAT) oee1_actual from Oee2DayDateSetDay left join (select * from L6productionEquipment where work_dt = '" + formatYesterday + "') as a ON Oee2DayDateSetDay.line_code = a.line_code)  as temp ";
        Dataset<Row> l6OeeDayLine = DPSparkApp.getSession().sql(sql);
        l6OeeDayLine.show(50);

        schemaList.put("actual_output_qty", DataTypes.createStructField("actual_output_qty", DataTypes.DoubleType, true));
        schemaList.put("total_worktime_oee2", DataTypes.createStructField("total_worktime_oee2", DataTypes.DoubleType, true));
        schemaList.put("total_worktime_oee1", DataTypes.createStructField("total_worktime_oee1", DataTypes.FloatType, true));

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_day", l6OeeDayLine.toJavaRDD(), schemaList, l6OeeDayLine.schema());
        schemaList.remove("actual_output_qty");
        schemaList.remove("total_worktime_oee2");
        schemaList.remove("total_worktime_oee1");
    }

    public void oee2L6byWeek(String formatYesterday, HashMap<String, StructField> schemaList) throws Exception {

        String dateWeek = String.valueOf(batchGetter.getDateWeek(formatYesterday));
        Dataset<Row> filter = DPSparkApp.getSession().sql("select * from (select sum(a.oee2_actual) oee2_actual,a.line_code line_code,a.work_dt_week work_dt_week from (select oee2_actual,line_code,calculateYearWeek(work_dt) work_dt_week from Oee2DayDateSet)as a  group by a.work_dt_week,a.line_code) where work_dt_week = '" + dateWeek + "' ");
        filter.show(50);
        System.out.println(filter.count());
        filter.createTempView("Oee2DayDateSetWeek");
        String sql = metaGetter.Get("L6Oee2LineWeek.sql");
        Dataset<Row> l6OeeDayLine = DPSparkApp.getSession().sql(sql);
        l6OeeDayLine.show(50);

        System.out.println(l6OeeDayLine.count());

        schemaList.remove("work_date");
        schemaList.put("week_id", DataTypes.createStructField("week_id", DataTypes.IntegerType, true));

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_week", l6OeeDayLine.toJavaRDD(), schemaList, l6OeeDayLine.schema());

    }

    public void oee2L6byLastWeek(HashMap<String, StructField> schemaList) throws Exception {

        String lastweekday = batchGetter.getStDateWeekAdd(-1, "-")._1;
        String dateWeek = String.valueOf(batchGetter.getDateWeek(lastweekday));

        Dataset<Row> filter = DPSparkApp.getSession().sql("select * from (select sum(a.oee2_actual) oee2_actual,a.line_code line_code,a.work_dt_week work_dt_week from (select oee2_actual,line_code,calculateYearWeek(work_dt) work_dt_week from Oee2DayDateSet)as a  group by a.work_dt_week,a.line_code) where work_dt_week = '" + dateWeek + "' ");
        filter.show(50);
        System.out.println(filter.count());
        filter.createOrReplaceTempView("Oee2DayDateSetWeek");
        Dataset<Row> l6OeeDayLine = DPSparkApp.getSession().sql("select\n" +
                "id ,\n" +
                "site_code,\n" +
                "level_code,\n" +
                "process_code,\n" +
                "line_code,\n" +
                "production_time_week,\n" +
                "week_id,\n" +
                "etltime ,\n" +
                "actual_output_qty,\n" +
                "oee2_actual,\n" +
                "cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all','D08', 'all'),15) AS FLOAT)*100 oee2_target,\n" +
                "oee1_actual\n" +
                "from\n" +
                "(" +
                "select\n" +
                "uuid() id ,\n" +
                "'WH' site_code,\n" +
                "'L6' level_code,\n" +
                "'SMT/PTH' process_code,\n" +
                "Oee2DayDateSetWeek.line_code line_code,\n" +
                "a.production_time_week production_time_week,\n" +
                "Oee2DayDateSetWeek.work_dt_week week_id,\n" +
                "unix_timestamp() etltime ,\n" +
                "Oee2DayDateSetWeek.oee2_actual actual_output_qty,\n" +
                "nvl((Oee2DayDateSetWeek.oee2_actual/(a.production_time_week*3600))*100,0) oee2_actual ,\n" +
                "cast(nvl((Oee2DayDateSetWeek.oee2_actual/(a.production_time*3600))*100,0) AS FLOAT) oee1_actual\n" +
                "from Oee2DayDateSetWeek\n" +
                "left join\n" +
                "(\n" +
                "  select * from\n" +
                "  (\n" +
                "    select\n" +
                "    line_code,\n" +
                "    work_dt_week,\n" +
                "    sum(planned_downtime_loss_hours) planned_downtime_loss_hours,\n" +
                "    sum(production_time) production_time,\n" +
                "    sum(production_time_day) production_time_week\n" +
                "    from\n" +
                "      (\n" +
                "      select\n" +
                "      c.line_code line_code,\n" +
                "      c.work_dt_week work_dt_week,\n" +
                "      sum(c.planned_downtime_loss_hours) planned_downtime_loss_hours,\n" +
                "      sum(c.production_time) production_time,\n" +
                "      sum(c.production_time_day) production_time_day\n" +
                "      from\n" +
                "        (select\n" +
                "        line_code,\n" +
                "        calculateYearWeek(work_dt) work_dt_week,\n" +
                "        planned_downtime_loss_hours,\n" +
                "        production_time,\n" +
                "        production_time_day\n" +
                "        from\n" +
                "        L6productionEquipment) as c\n" +
                "      group by\n" +
                "      c.line_code,\n" +
                "      c.work_dt_week\n" +
                "      )\n" +
                "      group by line_code,work_dt_week\n" +
                "    )\n" +
                "  where work_dt_week = '" + dateWeek + "'\n" +
                ")as a\n" +
                "where a.line_code = Oee2DayDateSetWeek.line_code ) as temp");
        l6OeeDayLine.show(50);

        System.out.println(l6OeeDayLine.count());

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_week", l6OeeDayLine.toJavaRDD(), schemaList, l6OeeDayLine.schema());
    }

    public void oee2L6byMonth(HashMap<String, StructField> schemaList) throws Exception {

        Dataset<Row> filter = DPSparkApp.getSession().sql("select * from\n" +
                "  (\n" +
                "  select\n" +
                "  sum(a.oee2_actual) oee2_actual,\n" +
                "  a.line_code line_code,\n" +
                "  a.work_dt_month work_dt_month\n" +
                "  from\n" +
                "    (\n" +
                "      select\n" +
                "      oee2_actual,\n" +
                "      line_code,\n" +
                "      cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt_month\n" +
                "      from Oee2DayDateSet) as a\n" +
                "  group by a.work_dt_month,a.line_code\n" +
                "  )\n" +
                "where work_dt_month = cast(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyyMM') AS INTEGER)");
        filter.show(50);
        System.out.println(filter.count());
        filter.createTempView("Oee2DayDateSetMonth");
        String sql = metaGetter.Get("L6Oee2LineMonth.sql");
        Dataset<Row> l6OeeDayLine = DPSparkApp.getSession().sql(sql);
        l6OeeDayLine.show(50);

        System.out.println(l6OeeDayLine.count());

        schemaList.remove("week_id");
        schemaList.put("month_id", DataTypes.createStructField("month_id", DataTypes.IntegerType, true));

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_month", l6OeeDayLine.toJavaRDD(), schemaList, l6OeeDayLine.schema());

    }

    public void oee2L6byQuarter(HashMap<String, StructField> schemaList) throws Exception {

        Dataset<Row> filter = DPSparkApp.getSession().sql("select * from\n" +
                "  (\n" +
                "  select\n" +
                "  sum(a.oee2_actual) oee2_actual,\n" +
                "  a.line_code line_code,\n" +
                "  a.work_dt_quarter work_dt_quarter\n" +
                "  from\n" +
                "    (\n" +
                "      select\n" +
                "      oee2_actual,\n" +
                "      line_code,\n" +
                "      cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt_quarter\n" +
                "      from Oee2DayDateSet) as a\n" +
                "  group by a.work_dt_quarter,a.line_code\n" +
                "  )\n" +
                "where work_dt_quarter = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) ) AS INTEGER)");

        filter.show(50);
        System.out.println(filter.count());
        filter.createTempView("Oee2DayDateSetQueater");
        String sql = metaGetter.Get("L6Oee2LineQuarter.sql");
        Dataset<Row> l6OeeDayLine = DPSparkApp.getSession().sql(sql);
        l6OeeDayLine.show(50);

        System.out.println(l6OeeDayLine.count());
        schemaList.remove("month_id");
        schemaList.put("quarter_id", DataTypes.createStructField("quarter_id", DataTypes.IntegerType, true));


        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_quarter", l6OeeDayLine.toJavaRDD(), schemaList, l6OeeDayLine.schema());

    }

    public void oee2L6byYear(String formatYesterday, HashMap<String, StructField> schemaList) throws Exception {

        Dataset<Row> filter = DPSparkApp.getSession().sql("select * from\n" +
                "  (\n" +
                "  select\n" +
                "  sum(a.oee2_actual) oee2_actual,\n" +
                "  a.line_code line_code,\n" +
                "  a.work_dt_year work_dt_year\n" +
                "  from\n" +
                "    (\n" +
                "      select\n" +
                "      oee2_actual,\n" +
                "      line_code,\n" +
                "      year(work_dt) work_dt_year\n" +
                "      from Oee2DayDateSet) as a\n" +
                "  group by a.work_dt_year,a.line_code\n" +
                "  )\n" +
                "where work_dt_year = year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))");

        filter.show(50);
        System.out.println(filter.count());
        filter.createTempView("Oee2DayDateSetYear");
        String sql = metaGetter.Get("L6Oee2LineYear.sql");
        Dataset<Row> l6OeeDayLine = DPSparkApp.getSession().sql(sql);
        l6OeeDayLine.show(50);

        System.out.println(l6OeeDayLine.count());
        schemaList.remove("quarter_id");
        schemaList.put("year_id", DataTypes.createStructField("year_id", DataTypes.IntegerType, true));


        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_year", l6OeeDayLine.toJavaRDD(), schemaList, l6OeeDayLine.schema());

    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }

}
