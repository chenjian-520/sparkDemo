package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.ProductionEquipmentDay;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.ProductionPartnoDay;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.enums.SiteEnum;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import scala.*;
import scala.collection.JavaConverters;

import java.lang.Double;
import java.lang.Float;
import java.lang.Long;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className L6oeeDwsToAds
 * @data 2020/01/12
 *
 * L6Oee
 * 输入表：
 *   dpm_dws_production_partno_dd 料号平台生产状态统计 (L6 OEE因子數據 L10 UPH因子數據)
 *   dpm_ods_production_target_values 获取oee2_target 目标值
 *   dpm_ods_production_equipment_day_lsix L6机台日生产状态信息  获取开线时间和开线数
 * 输出表：
 * dpm_ads_production_oee_day/week/month/quarter/year  OEE明細資料
 *
 */
public class L6oeeDwsToAds extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");
        doTaskBySiteEnum(map, SiteEnum.WH);
        doTaskBySiteEnum(map, SiteEnum.CQ);
        System.out.println("==============================>>>Programe End<<<==============================");
        DPSparkApp.stop();
    }

    /**
     * 根据 site 执行 .
     * @param map
     * @param siteEnum
     * @author ws
     * @date 2020/6/29 14:07
     * @return void
     **/
    private void doTaskBySiteEnum(Map<String, Object> map, SiteEnum siteEnum) throws Exception {
        System.out.println("==========>>>current site " + siteEnum.getCode() + "<<<==========");

        //初始化环境   //com.dl.spark.oee.L6oeeDwsToAds
        //初始化时间 2019-12-10
        String yesterday = batchGetter.getStDateDayAdd(-1);
        String today = batchGetter.getStDateDayAdd(0);
        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
        String todayStamp = batchGetter.getStDateDayStampAdd(0);
        batchGetter.getNowYear();

        Date yesterdayFormat = new Date(Long.valueOf(yesterdayStamp));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterdayFormatStr = simpleDateFormat.format(yesterdayFormat);

        Calendar c = Calendar.getInstance();

        c.setTimeInMillis(Long.valueOf(todayStamp));
        c.add(Calendar.YEAR, -1);
        c.add(Calendar.MONTH, -3);
        c.add(Calendar.DATE, -7);
        String oldYear = String.valueOf(c.getTime().getTime());

        System.out.println(oldYear);
        System.out.println(yesterday);
        System.out.println(today);
        System.out.println(yesterdayStamp);
        System.out.println(todayStamp);

//        String stDateDayStampAdd = simpleDateFormat.format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-3))));

        // work_dt+site_code+level_code+line_code+part_no+platform  联合唯一去重取最新时间
        JavaRDD<ProductionPartnoDay> partnoRdd = DPHbase.saltRddRead("dpm_dws_production_partno_dd", oldYear, todayStamp, new Scan(), true).map(r -> {
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
        }).filter(r -> siteEnum.getCode().equals(r.getSite_code()) && "L6".equals(r.getLevel_code()))
                .keyBy(r -> new Tuple7<>(r.getSite_code(), r.getLevel_code(), r.getLine_code(), r.getPart_no(), r.getPlantform(), r.getWork_dt(), r.getData_from()))
                .coalesce(10, false).reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return Long.valueOf(v1.getUpdate_dt()) >= Long.valueOf(v2.getUpdate_dt()) ? v1 : v2;
                }).map(r -> r._2);

        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

        //测试验证产量：
        partnoRdd.filter(r->!"0".equals(r.getTtl_pass_station().trim()) && yesterdayFormatStr.equals(r.getWork_dt())).take(500).forEach(r->{
            System.out.println(r.getPart_no()+","+r.getPlantform()+","+r.getLine_code()+","+r.getTtl_pass_station()+","+r.getSmt_ct()+","+r.getWork_dt()+","+r.getData_from());
        });

        //注册week的 udf函数
        sqlContext.udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
        Dataset<Row> partnoFrame = sqlContext.createDataFrame(partnoRdd, ProductionPartnoDay.class);
        partnoFrame.createOrReplaceTempView("dpm_dws_production_partno_day");

        Pattern pattern = Pattern.compile("^\\d{4}\\-\\d{2}\\-\\d{2}$");

        JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> equipmentDayRdd = DPHbase.rddRead("dpm_ods_production_equipment_day_lsix", new Scan(), true).map(r -> {
            return new Tuple10<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("line_code"))),
                    formatData(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("work_dt")))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("ttl_time"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("planned_downtime_loss_hours"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("status"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("sfc_line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("update_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("update_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("data_from"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_EQUIPMENT_DAY_LSIX"), Bytes.toBytes("site_code")))
            );
        }).filter(r -> siteEnum.getCode().equals(r._10()) && pattern.matcher(r._2() ).matches() && r._7()!=null && r._7() !="").sortBy(r -> r._2(), false, 1).persist(StorageLevel.MEMORY_AND_DISK());

        //最新一天的工作的线
        String data = equipmentDayRdd.take(1).get(0)._2();
        JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> equipmentDayRddNew = equipmentDayRdd.filter(r -> r._2().equals(data)).keyBy(r -> r._1()).reduceByKey((v1, v2) -> v1).map(r -> r._2);
        equipmentDayRddNew.take(5).forEach(r -> System.out.println(r));

        // L6 line昨天的开线数量
        long count = equipmentDayRddNew.filter(r -> "Y".equals(r._5())).count();
        System.out.println("-------------" + count + "-------------");

        Double value = equipmentDayRddNew.filter(r -> "Y".equals(r._5())).mapToPair(new PairFunction<Tuple10<String, String, String, String, String, String, String, String, String, String>, String, Double>() {
                                                        @Override
                                                        public Tuple2<String, Double> call(Tuple10<String, String, String, String, String, String, String, String, String, String> tuple10) throws Exception {
                                                            return new Tuple2<String, Double>(tuple10._2(), Double.valueOf(tuple10._4()));
                                                        }
                                                    }
        ).reduceByKey((a, b) -> a + b).filter(r -> data.equals(r._1)).take(1).get(0)._2;
        System.out.println(value + "-----" + data);
        System.out.println("-----value-------");

        //oee2 分母  day oee1 分母  day
        double oee1 = 24d * count * 3600;
        double oee2 = (23.5 * count - value) * 3600;
        System.out.println(oee2 + "----" + oee1);


        String id = UUID.randomUUID().toString();
        String site_code = siteEnum.getCode();
        String level_code = "L6";
        String etltime = String.valueOf(System.currentTimeMillis());

        //目标值函数加载
        LoadKpiTarget.loadProductionTarget();

        /******************************* by day oee1 oee2 *******************************************yesterdayFormatStr*/
        String sqloee1day = "select sum(day.ttl_pass_station) actual_output_qty, " + oee1 + " total_worktime_oee1, " + oee2 + " total_worktime_oee2, " + count + " equipment_work_qty , " + value + " plan_noproduction_time ,100 * sum(day.ttl_pass_station)/" + oee1 + " oee1_actual ,day.work_dt work_dt , 100 * sum(day.ttl_pass_station)/" + oee2 + " oee2_actual from (select ttl_pass_station*smt_ct ttl_pass_station,work_dt from dpm_dws_production_partno_day) as day group by day.work_dt having day.work_dt = '" + yesterdayFormatStr + "'";
        Dataset<Row> l6OeeDay = sqlContext.sql(sqloee1day);
        l6OeeDay.show();
        l6OeeDay.createOrReplaceTempView("oee1table");
        String str = "select cast(get_aim_target_by_key( concat_ws(\"=\",'D',site_code,level_code,'all', 'all', 'all', 'all'),15) AS FLOAT)*100 oee2_target, id,site_code,level_code,work_date,oee1_actual , oee2_actual,etltime,total_worktime_oee1,total_worktime_oee2,equipment_work_qty, plan_noproduction_time ,actual_output_qty from(select \"" + id + "\" id , \"" + site_code + "\" site_code,\"" + level_code + "\" level_code , work_dt work_date , " + " oee1_actual , oee2_actual " + ",\"" + etltime + "\" etltime ,total_worktime_oee1,total_worktime_oee2,equipment_work_qty, plan_noproduction_time ,actual_output_qty from oee1table)";
        Dataset<Row> l6OeeDay1 = sqlContext.sql(str);
        l6OeeDay1.show(500);
        HashMap<String, StructField> schemaList = new HashMap<>();
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("work_date", DataTypes.createStructField("work_date", DataTypes.StringType, true));
        schemaList.put("oee1_actual", DataTypes.createStructField("oee1_actual", DataTypes.DoubleType, true));
        schemaList.put("oee2_actual", DataTypes.createStructField("oee2_actual", DataTypes.DoubleType, true));
        schemaList.put("oee2_target", DataTypes.createStructField("oee2_target", DataTypes.FloatType, true));
        schemaList.put("etltime", DataTypes.createStructField("etltime", DataTypes.StringType, true));

        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_oee_day", l6OeeDay1.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(l6OeeDay1.schema().toSeq()).asJava(), SaveMode.Append);

        // 按天聚合的分子创建的视图  OeebydayAll
        Dataset<Row> oeeByDay = sqlContext.sql("select sum(day.ttl_pass_station) oee1_actual ,day.work_dt work_dt from (select ttl_pass_station*smt_ct ttl_pass_station,work_dt from dpm_dws_production_partno_day) as day group by day.work_dt");
        oeeByDay.createOrReplaceTempView("OeebydayAll");
        oeeByDay.show();
        System.out.println("----------------------------");
        JavaRDD<ProductionEquipmentDay> map1 = equipmentDayRdd.map(r -> {
            return new ProductionEquipmentDay(r._1(), r._2(), r._3(), r._4(), r._5(), r._6(), r._7(), r._8(), r._9());
        }).filter(r -> r.getWork_dt().matches("\\d{4}-\\d{2}-\\d{2}"));
        Dataset<Row> equipmentFrame = sqlContext.createDataFrame(map1, ProductionEquipmentDay.class);
        //L6机台日生产状态信息
        equipmentFrame.createOrReplaceTempView("dpm_ods_production_equipment_day_lsix");
        Dataset<Row> sql = sqlContext.sql("select * from (select sum(planned_downtime_loss_hours) planned_downtime_loss_hours,count(1) count,work_dt,status from dpm_ods_production_equipment_day_lsix group by work_dt,status sort by work_dt)as a where a.status = 'Y' sort by a.work_dt");
        //L6机台日生产状态信息，补齐今年L6的整厂计划停机时间和线活时间

        JavaRDD<Row> rowJavaRDD = sql.toJavaRDD().sortBy(r -> r.getString(2), false, 1);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, sql.schema());
        dataFrame.show();
        //获取当年第一天 补齐表到当年第一天
        SimpleDateFormat format = new SimpleDateFormat("yyyy");
        String time = format.format(format.parse(yesterdayFormatStr));
        String yearFirstDay = time + "-01-01";
        String todayStr = simpleDateFormat.format(new Date(Long.valueOf(todayStamp)));
        Dataset<Row> view = createView(dataFrame, sqlContext, yearFirstDay, todayStr);
        view.show(10);
        view.createOrReplaceTempView("dpm_ods_production_equipment_day_lsix_All");


        /******************************* by Month oee1 oee2 ********************************************/
        String sql1 = "select sum(planned_downtime_loss_hours) planned_downtime_loss_hours,sum(count) count, month from (select planned_downtime_loss_hours,count,month(work_dt) month,status from dpm_ods_production_equipment_day_lsix_All)  group by month having month = '" + formatMonth(getMonth()) + "'";
        Dataset<Row> oeeFenMuByMonth = sqlContext.sql(sql1);
        oeeFenMuByMonth.show();
        updateMonthOee(sqlContext, oeeFenMuByMonth, schemaList, siteEnum);
        /******************************* by week oee1 oee2 ********************************************/
        updateWeek(sqlContext, yesterdayFormatStr, batchGetter, schemaList, siteEnum);
        /******************************* by Lastweek oee1 oee2 ********************************************/
        updateLastWeek(sqlContext, batchGetter, schemaList, siteEnum);
        /******************************* by quarter oee1 oee2 ********************************************/
        updateQuarter(sqlContext, batchGetter, schemaList, siteEnum);
        /******************************* by year oee1 oee2 ********************************************/
        updateYear(sqlContext, batchGetter, schemaList, siteEnum);

    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }

    private void updateMonthOee(SQLContext sqlContext, Dataset<Row> oeeFenMuByMonth, HashMap<String, StructField> schemaList, SiteEnum siteEnum) throws Exception {
        /******************************* by month oee1 oee2 ********************************************/
        // oee1 和 oee2 当月的分子
        String sqlmonth = "select sum(day.ttl_pass_station) oee1_actual ,month(day.work_dt) month from (select ttl_pass_station*smt_ct ttl_pass_station,work_dt from dpm_dws_production_partno_day) as day group by day.work_dt";
        Dataset<Row> oeemoth = sqlContext.sql(sqlmonth);
        oeemoth.createOrReplaceTempView("ooe1Month");
        String sqlbyMonth = "select sum(oee1_actual) oee1_actual, month from ooe1Month group by month having month = '" + formatMonth(getMonth()) + "'";
        Dataset<Row> oeeFenZiByMonth = sqlContext.sql(sqlbyMonth);
        oeeFenZiByMonth.createOrReplaceTempView("oeebymonth");
        oeeFenZiByMonth.show();
        System.out.println("----------------------------");
        // oee1 和 oee2 当月的分母
        long cout = oeeFenMuByMonth.toJavaRDD().collect().get(0).getLong(1);
        long hours = (long) oeeFenMuByMonth.toJavaRDD().collect().get(0).getDouble(0);
        System.out.println(cout);
        System.out.println(hours);
        double oee1bymonth = Double.valueOf(24) * cout * 3600;
        double oee2bymonth = (Double.valueOf(23.5) * cout - hours) * 3600;
        System.out.println(oee1bymonth + "---------" + oee2bymonth);
        String oee1bymonth1 = " select 100 * oee1_actual/" + oee1bymonth + " oee1_actual,\"" + formatYYYYMM(getMonth()) + "\" month , 100 * oee1_actual/" + oee2bymonth + " oee2_actual from oeebymonth";
        Dataset<Row> l6OeeMonth = sqlContext.sql(oee1bymonth1);
        l6OeeMonth.show();

        String id = UUID.randomUUID().toString();
        String site_code = siteEnum.getCode();
        String level_code = "L6";

        String etltime = String.valueOf(System.currentTimeMillis());
        l6OeeMonth.createOrReplaceTempView("monthtable");
        String str = "select cast(get_aim_target_by_key( concat_ws(\"=\",'D',site_code,level_code,'all', 'all', 'all', 'all'),15) AS FLOAT)*100 oee2_target, id,site_code,level_code,month_id,oee1_actual , oee2_actual,etltime from(select \"" + id + "\" id , \"" + site_code + "\" site_code,\"" + level_code + "\" level_code , month month_id , " + " oee1_actual , oee2_actual " + ",\"" + etltime + "\" etltime from monthtable)";
        Dataset<Row> l6OeeMonth1 = sqlContext.sql(str);

        schemaList.remove("work_date");
        schemaList.put("month_id", DataTypes.createStructField("month_id", DataTypes.StringType, true));
        // 更新月
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_month", l6OeeMonth1.toJavaRDD(), schemaList, l6OeeMonth1.schema());
//        DPMysql.commonOdbcUpdateBatch("dp_ads", "dpm_ads_production_site_kpi_month", l6OeeMonth.toJavaRDD(), schemaList, l6OeeMonth.schema(), new DlFunction<Row, String>() {
//            @Override
//            public String apply(Row row) {
//                StringBuffer stringBuffer = new StringBuffer();
//                String append = stringBuffer.append("month_id = ").append("'").append(formatYYYYMM(getMonth())).append("'")
//                        .append("and site_code = ").append("'").append("WH").append("'")
//                        .append("and level_code = ").append("'").append("L6").append("'").toString();
//                return append;
//            }
//        });
    }

    private void updateWeek(SQLContext sqlContext, String yesterdayFormatStr, BatchGetter batchGetter, HashMap<String, StructField> schemaList, SiteEnum siteEnum) throws Exception {
        String dateWeek = String.valueOf(batchGetter.getDateWeek(yesterdayFormatStr));
        String sqlWeek = "select sum(oee1_actual) oee_actual_week,work_dt_week from (select oee1_actual ,calculateYearWeek(work_dt) work_dt_week from OeebydayAll) group by work_dt_week having work_dt_week = '" + dateWeek + "'";
        //oee1 和 oee2 当周的分子
        Dataset<Row> weekFenZi = sqlContext.sql(sqlWeek);
        weekFenZi.createOrReplaceTempView("oeebyweek");
        weekFenZi.show();
        //oee1 和 oee2 当周的分母
        String sqlWeek1 = "select sum(planned_downtime_loss_hours) planned_downtime_loss_hours_week , sum(count) cout_week , work_dt_week from (select planned_downtime_loss_hours,count,calculateYearWeek(work_dt) work_dt_week from dpm_ods_production_equipment_day_lsix_All) group by work_dt_week having work_dt_week = '" + dateWeek + "'";
        Dataset<Row> oeeFenMuByWeek = sqlContext.sql(sqlWeek1);
        oeeFenMuByWeek.show();
        long count_week = oeeFenMuByWeek.toJavaRDD().collect().get(0).getLong(1);
        long hours_week = (long) oeeFenMuByWeek.toJavaRDD().collect().get(0).getDouble(0);
        double oee1byweek = Double.valueOf(24) * count_week * 3600;
        double oee2byweek = (Double.valueOf(23.5) * count_week - hours_week) * 3600;
        System.out.println(oee1byweek + "---------" + oee2byweek);
        String oeebyWeek = " select 100 * oee_actual_week/" + oee1byweek + " oee1_actual, work_dt_week , 100 * oee_actual_week/" + oee2byweek + " oee2_actual from oeebyweek";
        Dataset<Row> l6OeeWeek = sqlContext.sql(oeebyWeek);
        l6OeeWeek.show();

        String id = UUID.randomUUID().toString();
        String site_code = siteEnum.getCode();
        String level_code = "L6";

        String etltime = String.valueOf(System.currentTimeMillis());
        l6OeeWeek.createOrReplaceTempView("weektable");
        String str = "select cast(get_aim_target_by_key( concat_ws(\"=\",'D',site_code,level_code,'all', 'all', 'all', 'all'),15) AS FLOAT)*100 oee2_target, id,site_code,level_code,week_id,oee1_actual , oee2_actual,etltime from( select \"" + id + "\" id , \"" + site_code + "\" site_code,\"" + level_code + "\" level_code , work_dt_week week_id , " + " oee1_actual , oee2_actual " + ",\"" + etltime + "\" etltime from weektable )";
        Dataset<Row> l6OeeWeek1 = sqlContext.sql(str);

        schemaList.remove("month_id");
        schemaList.put("week_id", DataTypes.createStructField("week_id", DataTypes.IntegerType, true));
        // 更新周
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_week", l6OeeWeek1.toJavaRDD(), schemaList, l6OeeWeek1.schema());

        // 更新周
//        DPMysql.commonOdbcUpdateBatch("dp_ads", "dpm_ads_production_site_kpi_week", l6OeeWeek.toJavaRDD(), schemaList, l6OeeWeek.schema(), new DlFunction<Row, String>() {
//            @Override
//            public String apply(Row row) {
//                StringBuffer stringBuffer = new StringBuffer();
//                String append = stringBuffer.append("week_id = ").append("'").append(dateWeek).append("'")
//                        .append("and site_code = ").append("'").append("WH").append("'")
//                        .append("and level_code = ").append("'").append("L6").append("'").toString();
//                return append;
//            }
//        });
    }

    private void updateLastWeek(SQLContext sqlContext, BatchGetter batchGetter, HashMap<String, StructField> schemaList, SiteEnum siteEnum) throws Exception {
        String lastweekday = batchGetter.getStDateWeekAdd(-1, "-")._1;

        String dateWeek = String.valueOf(batchGetter.getDateWeek(lastweekday));
        String sqlWeek = "select sum(oee1_actual) oee_actual_week,work_dt_week from (select oee1_actual ,calculateYearWeek(work_dt) work_dt_week from OeebydayAll) group by work_dt_week having work_dt_week = '" + dateWeek + "'";
        //oee1 和 oee2 当周的分子
        Dataset<Row> weekFenZi = sqlContext.sql(sqlWeek);
        weekFenZi.createOrReplaceTempView("oeebylastweek");
        weekFenZi.show();
        //oee1 和 oee2 当周的分母
        String sqlWeek1 = "select sum(planned_downtime_loss_hours) planned_downtime_loss_hours_week , sum(count) cout_week , work_dt_week from (select planned_downtime_loss_hours,count,calculateYearWeek(work_dt) work_dt_week from dpm_ods_production_equipment_day_lsix_All) group by work_dt_week having work_dt_week = '" + dateWeek + "'";
        Dataset<Row> oeeFenMuByWeek = sqlContext.sql(sqlWeek1);
        long count_week = oeeFenMuByWeek.toJavaRDD().collect().get(0).getLong(1);
        long hours_week = (long) oeeFenMuByWeek.toJavaRDD().collect().get(0).getDouble(0);
        double oee1byweek = Double.valueOf(24) * count_week * 3600;
        double oee2byweek = (Double.valueOf(23.5) * count_week - hours_week) * 3600;
        System.out.println(oee1byweek + "---------" + oee2byweek);
        String oeebyWeek = " select 100 * oee_actual_week/" + oee1byweek + " oee1_actual, work_dt_week , 100 * oee_actual_week/" + oee2byweek + " oee2_actual from oeebylastweek";
        Dataset<Row> l6OeeWeek = sqlContext.sql(oeebyWeek);
        l6OeeWeek.show();

        String id = UUID.randomUUID().toString();
        String site_code = siteEnum.getCode();
        String level_code = "L6";

        String etltime = String.valueOf(System.currentTimeMillis());
        l6OeeWeek.createOrReplaceTempView("weeklasttable");
        String str = "select cast(get_aim_target_by_key( concat_ws(\"=\",'D',site_code,level_code,'all', 'all', 'all', 'all'),15) AS FLOAT)*100 oee2_target, id,site_code,level_code,week_id,oee1_actual , oee2_actual,etltime from(select \"" + id + "\" id , \"" + site_code + "\" site_code,\"" + level_code + "\" level_code , work_dt_week week_id , " + " oee1_actual , oee2_actual " + ",\"" + etltime + "\" etltime from weeklasttable)";
        Dataset<Row> l6OeeWeek1 = sqlContext.sql(str);

        // 更新周
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_week", l6OeeWeek1.toJavaRDD(), schemaList, l6OeeWeek1.schema());

        // 更新周
//        DPMysql.commonOdbcUpdateBatch("dp_ads", "dpm_ads_production_site_kpi_week", l6OeeWeek.toJavaRDD(), schemaList, l6OeeWeek.schema(), new DlFunction<Row, String>() {
//            @Override
//            public String apply(Row row) {
//                StringBuffer stringBuffer = new StringBuffer();
//                String append = stringBuffer.append("week_id = ").append("'").append(dateWeek).append("'")
//                        .append("and site_code = ").append("'").append("WH").append("'")
//                        .append("and level_code = ").append("'").append("L6").append("'").toString();
//                return append;
//            }
//        });
    }


    private void updateQuarter(SQLContext sqlContext, BatchGetter batchGetter, HashMap<String, StructField> schemaList, SiteEnum siteEnum) throws Exception {
        String dateQuarter = newQuarter();
        String quarter = String.valueOf(batchGetter.getNowQuarter());
        String sqlQuarter = "select sum(oee1_actual) oee_actual_quarter,work_dt_quarter from (select oee1_actual ,cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt_quarter from OeebydayAll) group by work_dt_quarter having work_dt_quarter = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))) AS INTEGER)";
        //oee1 和 oee2 当季的分子
        Dataset<Row> quarterFenZi = sqlContext.sql(sqlQuarter);
        quarterFenZi.createOrReplaceTempView("oeebyquarter");
        quarterFenZi.show();
        //oee1 和 oee2 当季的分母
        String sqlquarter1 = "select sum(planned_downtime_loss_hours) planned_downtime_loss_hours_quarter , sum(count) cout_quarter , work_dt_quarter from (select planned_downtime_loss_hours,count,cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt_quarter from dpm_ods_production_equipment_day_lsix_All) group by work_dt_quarter having work_dt_quarter = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))) AS INTEGER)";
        Dataset<Row> oeeFenMuByquarter = sqlContext.sql(sqlquarter1);
        long countQuarter = oeeFenMuByquarter.toJavaRDD().collect().get(0).getLong(1);
        long hoursQuarter = (long) oeeFenMuByquarter.toJavaRDD().collect().get(0).getDouble(0);
        double oee1byQuarter = Double.valueOf(24) * countQuarter * 3600;
        double oee2byQuarter = (Double.valueOf(23.5) * countQuarter - hoursQuarter) * 3600;
        System.out.println(oee1byQuarter + "---------" + oee2byQuarter);
        String oeebyQuarter = " select 100 * oee_actual_quarter/" + oee1byQuarter + " oee1_actual, work_dt_quarter , 100 * oee_actual_quarter/" + oee2byQuarter + " oee2_actual from oeebyquarter";
        Dataset<Row> l6OeeQuarter = sqlContext.sql(oeebyQuarter);
        l6OeeQuarter.show();

        String id = UUID.randomUUID().toString();
        String site_code = siteEnum.getCode();
        String level_code = "L6";

        String etltime = String.valueOf(System.currentTimeMillis());
        l6OeeQuarter.createOrReplaceTempView("quartertable");
        String str = "select cast(get_aim_target_by_key( concat_ws(\"=\",'D',site_code,level_code,'all', 'all', 'all', 'all'),15) AS FLOAT)*100 oee2_target, id,site_code,level_code,quarter_id,oee1_actual , oee2_actual,etltime from(select \"" + id + "\" id , \"" + site_code + "\" site_code,\"" + level_code + "\" level_code , work_dt_quarter quarter_id , " + " oee1_actual , oee2_actual " + ",\"" + etltime + "\" etltime from quartertable)";
        Dataset<Row> l6OeeQuarter1 = sqlContext.sql(str);

        schemaList.remove("week_id");
        schemaList.put("quarter_id", DataTypes.createStructField("quarter_id", DataTypes.IntegerType, true));
        // 更新季
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_quarter", l6OeeQuarter1.toJavaRDD(), schemaList, l6OeeQuarter1.schema());

//        // 更新季
//        DPMysql.commonOdbcUpdateBatch("dp_ads", "dpm_ads_production_site_kpi_quarter", l6OeeQuarter.toJavaRDD(), schemaList, l6OeeQuarter.schema(), new DlFunction<Row, String>() {
//            @Override
//            public String apply(Row row) {
//                StringBuffer stringBuffer = new StringBuffer();
//                String append = stringBuffer.append("quarter_id = ").append("'").append(dateQuarter).append("'")
//                        .append("and site_code = ").append("'").append("WH").append("'")
//                        .append("and level_code = ").append("'").append("L6").append("'").toString();
//                return append;
//            }
//        });
    }

    private void updateYear(SQLContext sqlContext, BatchGetter batchGetter, HashMap<String, StructField> schemaList, SiteEnum siteEnum) throws Exception {
        String dateYear = String.valueOf(batchGetter.getNowYear());
        String sqlYear = "select sum(oee1_actual) oee_actual_year,work_dt_year from (select oee1_actual ,year(work_dt) work_dt_year from OeebydayAll) group by work_dt_year having work_dt_year = '" + dateYear + "'";
        //oee1 和 oee2 当年的分子
        Dataset<Row> YearFenZi = sqlContext.sql(sqlYear);
        YearFenZi.createOrReplaceTempView("oeebyyear");
        YearFenZi.show();
        //oee1 和 oee2 当年的分母
        String sqlYear1 = "select sum(planned_downtime_loss_hours) planned_downtime_loss_hours_year , sum(count) cout_year , work_dt_year from (select planned_downtime_loss_hours,count,year(work_dt) work_dt_year from dpm_ods_production_equipment_day_lsix_All) group by work_dt_year having work_dt_year = '" + dateYear + "'";
        Dataset<Row> oeeFenMuByYear = sqlContext.sql(sqlYear1);
        long countYear = oeeFenMuByYear.toJavaRDD().collect().get(0).getLong(1);
        long hoursYear = (long) oeeFenMuByYear.toJavaRDD().collect().get(0).getDouble(0);
        double oee1byYear = Double.valueOf(24) * countYear * 3600;
        double oee2byYear = (Double.valueOf(23.5) * countYear - hoursYear) * 3600;
        System.out.println(oee1byYear + "---------" + oee2byYear);
        String oeebyYear = " select 100 * oee_actual_year/" + oee1byYear + " oee1_actual, work_dt_year , 100 * oee_actual_year/" + oee2byYear + " oee2_actual from oeebyyear";
        Dataset<Row> l6OeeYear = sqlContext.sql(oeebyYear);
        l6OeeYear.show();

        String id = UUID.randomUUID().toString();
        String site_code = siteEnum.getCode();
        String level_code = "L6";

        String etltime = String.valueOf(System.currentTimeMillis());
        l6OeeYear.createOrReplaceTempView("yeartable");
        String str = "select cast(get_aim_target_by_key( concat_ws(\"=\",'D',site_code,level_code,'all', 'all', 'all', 'all'),15) AS FLOAT)*100 oee2_target, id,site_code,level_code,year_id,oee1_actual , oee2_actual,etltime from(select \"" + id + "\" id , \"" + site_code + "\" site_code,\"" + level_code + "\" level_code , work_dt_year year_id , " + " oee1_actual , oee2_actual " + ",\"" + etltime + "\" etltime from yeartable)";
        Dataset<Row> l6OeeYear1 = sqlContext.sql(str);

        schemaList.remove("quarter_id");
        schemaList.put("year_id", DataTypes.createStructField("year_id", DataTypes.IntegerType, true));
        // 更新年
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_year", l6OeeYear1.toJavaRDD(), schemaList, l6OeeYear1.schema());

//        // 更新年
//        DPMysql.commonOdbcUpdateBatch("dp_ads", "dpm_ads_production_site_kpi_year", l6OeeYear.toJavaRDD(), schemaList, l6OeeYear.schema(), new DlFunction<Row, String>() {
//            @Override
//            public String apply(Row row) {
//                StringBuffer stringBuffer = new StringBuffer();
//                String append = stringBuffer.append("year_id = ").append("'").append(dateYear).append("'")
//                        .append("and site_code = ").append("'").append("WH").append("'")
//                        .append("and level_code = ").append("'").append("L6").append("'").toString();
//                return append;
//            }
//        });
    }

    public static String notNull(String str) {
        if (StringUtils.isEmpty(str) || "null".equals(str)) {
            return "";
        }
        return str;
    }

    public static String formatData(String str) {
        if (str.contains("/")) {
            String[] split = str.split("/");
            StringBuffer data = new StringBuffer();
            data.append(split[0]).append("-");
            if (split[1].length() <= 1) {
                data.append("0" + split[1]).append("-");
            } else {
                data.append(split[1]).append("-");
            }
            if (split[2].length() <= 1) {
                data.append("0" + split[2]);
            } else {
                data.append(split[2]);
            }
            return data.toString();
        } else {
            return str;
        }
    }

    public static String getLastMonth() {
        LocalDate today = LocalDate.now();
        today = today.minusMonths(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        return formatter.format(today);
    }

    public static String getMonth() {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(System.currentTimeMillis());
        c.add(Calendar.DATE, -1);
        Date time = c.getTime();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM");
        return simpleDateFormat.format(time);
    }

    public static String formatMonth(String date) {
        String[] split = date.split("-");
        int month = Integer.parseInt(split[1]);
        return String.valueOf(month);
    }

    public static String formatYYYYMM(String date) {
        String[] split = date.split("-");
        return split[0] + split[1];
    }


    public static int differentDaysByDate(Date date1, Date date2) {
        return (int) ((date2.getTime() - date1.getTime()) / (1000 * 3600 * 24));
    }

    /**
     * @param sql          dataset
     * @param sqlContext   sparksql context
     * @param yearFirstDay 补齐从 yearFirstDay  这一天  todayStr 这一天 每天的线的情况
     * @param todayStr
     */
    public static Dataset<Row> createView(Dataset<Row> sql, SQLContext sqlContext, String yearFirstDay, String todayStr) throws ParseException {
        //初始化时间
        BatchGetter batchGetter = BatchGetter.getInstance();
        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);

        Date yesterdayFormat = new Date(Long.valueOf(yesterdayStamp));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        List<Row> collect = sql.toJavaRDD().collect();
        List<Row> buffer = new ArrayList<>();
        for (int i = 0; i < collect.size() - 1; i++) {
            String nextDate = collect.get(i + 1).getString(2);
            int len = differentDaysByDate(simpleDateFormat.parse(nextDate), simpleDateFormat.parse(collect.get(i).getString(2)));
            if (len > 1) {
                for (int j = 1; j < len; j++) {
                    Row row = RowFactory.create(collect.get(i + 1).getDouble(0), collect.get(i + 1).getLong(1), batchGetter.getStDateDayStrAdd(nextDate, +j, "-"), collect.get(i + 1).getString(3));
                    buffer.add(row);
                }
            }
        }
        buffer.addAll(collect);
        String lastDate = collect.get(0).getString(2);
        String first = collect.get(collect.size() - 1).getString(2);

        int byDate = differentDaysByDate(simpleDateFormat.parse(lastDate), simpleDateFormat.parse(todayStr));
        if (byDate > 1) {
            for (int i = 1; i < byDate; i++) {
                Row row = RowFactory.create(collect.get(0).getDouble(0), collect.get(0).getLong(1), batchGetter.getStDateDayStrAdd(lastDate, +i, "-"), collect.get(0).getString(3));
                buffer.add(row);
            }
        }

        int byDate1 = differentDaysByDate(simpleDateFormat.parse(yearFirstDay), simpleDateFormat.parse(first));
        if (byDate1 > 1) {
            for (int i = 1; i <= byDate1; i++) {
                Row row = RowFactory.create(collect.get(collect.size() - 1).getDouble(0), collect.get(collect.size() - 1).getLong(1), batchGetter.getStDateDayStrAdd(first, -i, "-"), collect.get(collect.size() - 1).getString(3));
                buffer.add(row);
            }
        }
        JavaRDD<Row> parallelize = DPSparkApp.getContext().parallelize(buffer);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(parallelize, sql.schema());
        return dataFrame;
    }

    @Test
    public void chen() {
        BatchGetter batchGetter = BatchGetter.getInstance();
        System.out.println(formatMonth(getLastMonth()));
        System.out.println(formatMonth(getMonth()));
        System.out.println(formatYYYYMM(getLastMonth()));
        System.out.println(formatYYYYMM(getMonth()));
        System.out.println(batchGetter.getNowYear());
    }

    public String newQuarter() {
        BatchGetter batchGetter = BatchGetter.getInstance();
        Date yesterdayFormat = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy");
        String yesterdayFormatStr = simpleDateFormat.format(yesterdayFormat);
        return yesterdayFormatStr + String.valueOf(batchGetter.getNowQuarter());
    }

}

