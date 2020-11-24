package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmAdsUtilisationTemp;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.FormatFloatNumber;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

/**
 * @author HS
 * @className L10ProductionUtilisation
 * @description TODO
 * @date 2020/4/22 10:18
 */
public class L10ProductionUtilisation extends DPSparkBase {


    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

    Float utilisation_target = 0.0f;
    String etl_time = String.valueOf(System.currentTimeMillis());
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        //每天的线生产情况
        Tuple2<JavaPairRDD<String, Float>, Broadcast<Long>> dayWorkTime = calculateL10Utilisation(Long.valueOf(batchGetter.getStDateDayStampAdd(-1, "-")), Long.valueOf(batchGetter.getStDateDayStampAdd(1, "-")));
        calculateDayProductionUtilisation(dayWorkTime._1, dayWorkTime._2);

        //每周的线生产情况
        Tuple2<JavaPairRDD<String, Float>, Broadcast<Long>> weekWorkTime = calculateL10Utilisation(Long.valueOf(batchGetter.getStDateWeekStampAdd(-1, "-")._1), Long.valueOf(batchGetter.getStDateWeekStampAdd(1, "-")._1));
        calculateWeekProductionUtilisation(weekWorkTime._1, weekWorkTime._2);

        //每月的线生产情况
        Tuple2<JavaPairRDD<String, Float>, Broadcast<Long>> monthWorkTime = calculateL10Utilisation(Long.valueOf(batchGetter.getStDateMonthStampAdd(-1, "-")._1), Long.valueOf(batchGetter.getStDateMonthStampAdd(1, "-")._1));
        calculateMonthProductionUtilisation(monthWorkTime._1, monthWorkTime._2);

        //每季的线生产情况
        Tuple2<JavaPairRDD<String, Float>, Broadcast<Long>> quarterWorkTime = calculateL10Utilisation(Long.valueOf(batchGetter.getStDateQuarterStampAdd(-1, "-")._1), Long.valueOf(batchGetter.getStDateQuarterStampAdd(1, "-")._1));
        calculateQuarterProductionUtilisation(quarterWorkTime._1, quarterWorkTime._2);

        //每年的线生产情况
        Tuple2<JavaPairRDD<String, Float>, Broadcast<Long>> yearWorkTime = calculateL10Utilisation(Long.valueOf(batchGetter.getStDateYearStampAdd(-1, "-")._1), Long.valueOf(batchGetter.getStDateYearStampAdd(1, "-")._1));
        calculateYearProductionUtilisation(yearWorkTime._1, yearWorkTime._2);

    }


    /*
     * ====================================================================
     * 描述:L10线利用率统计
     * ====================================================================
     */
    public Tuple2<JavaPairRDD<String, Float>, Broadcast<Long>> calculateL10Utilisation(Long startStamp, Long endStamp) throws Exception {
        System.out.println(startStamp + "__________" + endStamp);


        Scan production_line_info_scan = new Scan();
        production_line_info_scan.withStartRow("!".getBytes(), true);
        production_line_info_scan.withStopRow("~".getBytes(), true);
        JavaRDD<Result> production_line_info_dd = DPHbase.rddRead("dpm_dws_production_line_info_dd", production_line_info_scan, true);


        JavaRDD<ArrayList<String>> dpm_dws_production_line_info_dd_filted = production_line_info_dd.filter(result -> {
            //必须字段过滤
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_LINE_INFO_DD", "site_code", "level_code", "line_code", "work_dt", "work_shift", "work_time", "update_dt", "update_by", "data_from");
        }).mapPartitions(batchP -> {
            //时间范围过滤
            ArrayList<Result> rs = new ArrayList<>();
            while (batchP.hasNext()) {
                Result next = batchP.next();
                if (batchGetter.getFilterRangeTimeStampHBeans(next, "DPM_DWS_PRODUCTION_LINE_INFO_DD", "work_dt", "yyyy-MM-dd", startStamp, endStamp)
                        &&
                        "L10".equals(Bytes.toString((next.getValue("DPM_DWS_PRODUCTION_LINE_INFO_DD".getBytes(), "level_code".getBytes()))))
                        &&
                        Long.valueOf(Bytes.toString((next.getValue("DPM_DWS_PRODUCTION_LINE_INFO_DD".getBytes(), "work_time".getBytes())))) > 0
                        ) {
                    rs.add(next);
                }
            }
            return rs.iterator();

        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_LINE_INFO_DD", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift", "work_time", "update_dt", "update_by", "data_from", "customer");
        });

        try {
            for (ArrayList<String> strings : dpm_dws_production_line_info_dd_filted.take(5)) {
                System.out.println(strings);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>dpm_dws_production_line_info_dd_filted End<<<==============================");

        JavaPairRDD<String, ArrayList<String>> format_production_line_info_dd = dpm_dws_production_line_info_dd_filted
                .keyBy(r -> {
                    return batchGetter.getStrArrayOrg(",", "-",
                            r.get(0), r.get(1)
                            , r.get(2), r.get(3), r.get(4)
                            , r.get(5)
                            , r.get(6), r.get(7), r.get(12)
                    );
                }).reduceByKey((rv1, rv2) -> {
                    return Long.valueOf(rv1.get(9)) > Long.valueOf(rv2.get(9)) ? rv1 : rv2;
                }).map(t -> {
                    t._2.remove(12);
                    return t._2;
                }).keyBy(new Function<ArrayList<String>, String>() {
                    @Override
                    public String call(ArrayList<String> r) throws Exception {
                        return batchGetter.getStrArrayOrg(",", "-",
                                r.get(0), r.get(1)
                                /*, r.get(2), r.get(3), r.get(4)*/
                                , "N/A"
                                , "N/A"
                                , "N/A"
                                , "N/A"
                                /*, r.get(5)*/
                                , r.get(6)
                                /* ,r.get(7)*/
                                , "N/A"
                        );
                    }
                }).persist(StorageLevel.MEMORY_AND_DISK());

        try {
            for (Tuple2<String, ArrayList<String>> strings : format_production_line_info_dd.take(5)) {
                System.out.println(strings);
            }
        } catch (Exception e) {

        }

        //计算线实际数量
        long realLineCount = format_production_line_info_dd.map(t -> {
            return t._2.get(5) + t._2.get(4);
        }).distinct().count();

        Broadcast<Long> bRealLineCount = DPSparkApp.getContext().broadcast(realLineCount);

        System.out.println("==============================>>>format_production_line_info_dd End<<<==============================");


        JavaPairRDD<String, Float> pairedRDD = format_production_line_info_dd.mapToPair(new PairFunction<Tuple2<String, ArrayList<String>>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(Tuple2<String, ArrayList<String>> t) throws Exception {
                return new Tuple2<String, Float>(t._1, Float.valueOf(t._2.get(8)) / 1000);
            }
        }).reduceByKey((kv1, kv2) -> {
            return kv1 + kv2;
        });

        return new Tuple2<>(pairedRDD, bRealLineCount);

    }

    public void calculateDayProductionUtilisation(JavaPairRDD<String, Float> pairedRDD, Broadcast<Long> bRealLineCount) throws Exception {

        JavaRDD<DpmAdsUtilisationTemp> calculated_production_line_info_dd = pairedRDD.map(t -> {
            //float can_produce_time = (6 * 24 * 60);

            /*
             * String id
             * "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
             * Integer output_workhours;
             * Integer line_qty_actual;
             * Integer can_produce_time;
             * String line_utilisation_actual;
             * String line_utilisation_target;
             * String etl_time
             */
            FormatFloatNumber formatFloatNumber = new FormatFloatNumber();

            ArrayList<String> rvs = new ArrayList<>();
            rvs.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
            String[] org = t._1.split(",");
            for (int i = 0; i < org.length; i++) {
                rvs.add(org[i]);
            }
            //output_hours 换分钟
            rvs.add(String.valueOf(Float.valueOf((t._2 / 60)).intValue()));
            //line_qty_actual 线数量
            rvs.add(String.valueOf(bRealLineCount.value().longValue()));
            //can_produce_time 可开线时间
            rvs.add(String.valueOf(bRealLineCount.value().longValue() * 20 * 60));
            rvs.add(String.valueOf(formatFloatNumber.call((t._2 / 60) / (bRealLineCount.value().longValue() * 20 * 60))));
            rvs.add(String.valueOf(utilisation_target));
            rvs.add(etl_time);
            return batchGetter.<DpmAdsUtilisationTemp>getBeanDeftInit(new DpmAdsUtilisationTemp(), rvs);
        });
        Dataset<Row> production_line_info_dd_DF = sqlContext.createDataFrame(calculated_production_line_info_dd, DpmAdsUtilisationTemp.class);
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            production_line_info_dd_DF.show(50);
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_line_utilisation_day", production_line_info_dd_DF.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_line_utilisation_day"), production_line_info_dd_DF.schema());
    }

    public void calculateWeekProductionUtilisation(JavaPairRDD<String, Float> pairedRDD, Broadcast<Long> bRealLineCount) throws Exception {
        //"site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
        JavaPairRDD<String, Float> groupWorkTimeRangeRDD = pairedRDD.mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(Tuple2<String, Float> kv) throws Exception {
                String[] split = kv._1.split(",");
                String dateWeek = String.valueOf(batchGetter.getDateWeek(split[6]));
                String monthOrg = batchGetter.getStrArrayOrg(",", "-", split[0], split[1], split[2], split[3], split[4], split[5], dateWeek, split[7]);
                return new Tuple2<String, Float>(
                        monthOrg,
                        kv._2
                );
            }
        }).reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
        try {
            for (Tuple2<String, Float> stringFloatTuple2 : groupWorkTimeRangeRDD.take(5)) {
                System.out.println(stringFloatTuple2);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>groupWorkTimeRangeRDD End<<<==============================");

        JavaRDD<Row> calculated_production_line_info_dd = groupWorkTimeRangeRDD.mapPartitions(it -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            ArrayList<Row> rows = new ArrayList<>();
            while (it.hasNext()) {
                Tuple2<String, Float> t = it.next();
                //float can_produce_time = (6 * 24 * 60);
                /*
                 * String id
                 * "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
                 * Integer output_workhours;
                 * Integer line_qty_actual;
                 * Integer can_produce_time;
                 * String line_utilisation_actual;
                 * String line_utilisation_target;
                 * String etl_time
                 */
                FormatFloatNumber formatFloatNumber = new FormatFloatNumber();

                ArrayList<Object> rvs = new ArrayList<>();
                rvs.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
                String[] org = t._1.split(",");

                int weekDayCount = batchGetter.getWeekDayCount(Integer.valueOf(org[6]));

                for (int i = 0; i < org.length; i++) {
                    rvs.add(i == 6 ? Integer.valueOf(org[i]) : org[i]);
                }
                rvs.add(String.valueOf(Float.valueOf((t._2 / 60)).intValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue() * 20 * 60 * weekDayCount));
                rvs.add(String.valueOf(formatFloatNumber.call((t._2 / 60) / (bRealLineCount.value().longValue() * 20 * 60 * weekDayCount))));
                rvs.add(String.valueOf(utilisation_target));
                rvs.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_line_utilisation_week", rvs));
            }
            return rows.iterator();
        });


        try {
            for (Row row : calculated_production_line_info_dd.take(5)) {
                System.out.println(row);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>calculated_production_line_info_dd End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_line_utilisation_week", calculated_production_line_info_dd, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_line_utilisation_week"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_line_utilisation_week"));

    }

    public void calculateMonthProductionUtilisation(JavaPairRDD<String, Float> pairedRDD, Broadcast<Long> bRealLineCount) throws Exception {
        //"site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
        JavaPairRDD<String, Float> groupWorkTimeRangeRDD = pairedRDD.mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(Tuple2<String, Float> kv) throws Exception {
                String[] split = kv._1.split(",");
                String yearMonth = split[6].replace("-", "").substring(0, 6);
                String weekOrg = batchGetter.getStrArrayOrg(",", "-", split[0], split[1], split[2], split[3], split[4], split[5], yearMonth, split[7]);
                return new Tuple2<String, Float>(
                        weekOrg,
                        kv._2
                );
            }
        }).reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
        JavaRDD<Row> calculated_production_line_info_dd = groupWorkTimeRangeRDD.mapPartitions(it -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            ArrayList<Row> rows = new ArrayList<>();
            while (it.hasNext()) {
                Tuple2<String, Float> t = it.next();
                //float can_produce_time = (6 * 24 * 60);
                /*
                 * String id
                 * "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
                 * Integer output_workhours;
                 * Integer line_qty_actual;
                 * Integer can_produce_time;
                 * String line_utilisation_actual;
                 * String line_utilisation_target;
                 * String etl_time
                 */
                FormatFloatNumber formatFloatNumber = new FormatFloatNumber();

                ArrayList<Object> rvs = new ArrayList<>();
                rvs.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
                String[] org = t._1.split(",");

                int monthDayCount = batchGetter.getStDateMonthDayCount(org[6] + "01");

                for (int i = 0; i < org.length; i++) {
                    rvs.add(i == 6 ? Integer.valueOf(org[i]) : org[i]);
                }
                rvs.add(String.valueOf(Float.valueOf((t._2 / 60)).intValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue() * 20 * 60 * monthDayCount));
                rvs.add(String.valueOf(formatFloatNumber.call((t._2 / 60) / (bRealLineCount.value().longValue() * 20 * 60 * monthDayCount))));
                rvs.add(String.valueOf(utilisation_target));
                rvs.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_line_utilisation_month", rvs));
            }
            return rows.iterator();
        });


        try {

            for (Row row : calculated_production_line_info_dd.take(5)) {
                System.out.println(row);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>calculated_production_line_info_dd End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_line_utilisation_month", calculated_production_line_info_dd, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_line_utilisation_month"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_line_utilisation_month"));

    }

    public void calculateQuarterProductionUtilisation(JavaPairRDD<String, Float> pairedRDD, Broadcast<Long> bRealLineCount) throws Exception {
        //"site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
        JavaPairRDD<String, Float> groupWorkTimeRangeRDD = pairedRDD.mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(Tuple2<String, Float> kv) throws Exception {
                String[] split = kv._1.split(",");
                int year = Integer.valueOf(split[6].replace("-", "").substring(0, 4)) * 10;
                int targetDateQuarter = batchGetter.getTargetDateQuarter(split[6], "-");
                String quarterOrg = batchGetter.getStrArrayOrg(",", "-", split[0], split[1], split[2], split[3], split[4], split[5], String.valueOf(year + targetDateQuarter), split[7]);
                return new Tuple2<String, Float>(
                        quarterOrg,
                        kv._2
                );
            }
        }).reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
        JavaRDD<Row> calculated_production_line_info_dd = groupWorkTimeRangeRDD.mapPartitions(it -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            ArrayList<Row> rows = new ArrayList<>();
            while (it.hasNext()) {
                Tuple2<String, Float> t = it.next();
                //float can_produce_time = (6 * 24 * 60);
                /*
                 * String id
                 * "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
                 * Integer output_workhours;
                 * Integer line_qty_actual;
                 * Integer can_produce_time;
                 * String line_utilisation_actual;
                 * String line_utilisation_target;
                 * String etl_time
                 */
                FormatFloatNumber formatFloatNumber = new FormatFloatNumber();

                ArrayList<Object> rvs = new ArrayList<>();
                rvs.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
                String[] org = t._1.split(",");

                int quarterDayCount = batchGetter.getStDateQuarterDayCount(
                        batchGetter.getStTargetQuarterYear(Integer.valueOf(org[6].substring(0, 4)), Integer.valueOf(org[6].substring(4, 5)), "-")._1
                );

                for (int i = 0; i < org.length; i++) {
                    rvs.add(i == 6 ? Integer.valueOf(org[i]) : org[i]);
                }
                rvs.add(String.valueOf(Float.valueOf((t._2 / 60)).intValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue() * 20 * 60 * quarterDayCount));
                rvs.add(String.valueOf(formatFloatNumber.call((t._2 / 60) / (bRealLineCount.value().longValue() * 20 * 60 * quarterDayCount))));
                rvs.add(String.valueOf(utilisation_target));
                rvs.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_line_utilisation_quarter", rvs));
            }
            return rows.iterator();
        });


        try {

            for (Row row : calculated_production_line_info_dd.take(5)) {
                System.out.println(row);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>calculated_production_line_info_dd End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_line_utilisation_quarter", calculated_production_line_info_dd, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_line_utilisation_quarter"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_line_utilisation_quarter"));
    }

    public void calculateYearProductionUtilisation(JavaPairRDD<String, Float> pairedRDD, Broadcast<Long> bRealLineCount) throws Exception {
        //"site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
        JavaPairRDD<String, Float> groupWorkTimeRangeRDD = pairedRDD.mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(Tuple2<String, Float> kv) throws Exception {
                String[] split = kv._1.split(",");
                String year = String.valueOf(split[6].substring(0, 4));
                String yearOrg = batchGetter.getStrArrayOrg(",", "-", split[0], split[1], split[2], split[3], split[4], split[5], year, split[7]);
                return new Tuple2<String, Float>(
                        yearOrg,
                        kv._2
                );
            }
        }).reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
        JavaRDD<Row> calculated_production_line_info_dd = groupWorkTimeRangeRDD.mapPartitions(it -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            ArrayList<Row> rows = new ArrayList<>();
            while (it.hasNext()) {
                Tuple2<String, Float> t = it.next();
                //float can_produce_time = (6 * 24 * 60);
                /*
                 * String id
                 * "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "work_dt", "work_shift"
                 * Integer output_workhours;
                 * Integer line_qty_actual;
                 * Integer can_produce_time;
                 * String line_utilisation_actual;
                 * String line_utilisation_target;
                 * String etl_time
                 */
                FormatFloatNumber formatFloatNumber = new FormatFloatNumber();

                ArrayList<Object> rvs = new ArrayList<>();
                rvs.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
                String[] org = t._1.split(",");

                int yearDayCount = batchGetter.getStDateYearDayCount(org[6] + "0101");

                for (int i = 0; i < org.length; i++) {
                    rvs.add(i == 6 ? Integer.valueOf(org[i]) : org[i]);
                }
                rvs.add(String.valueOf(Float.valueOf((t._2 / 60)).intValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue()));
                rvs.add(String.valueOf(bRealLineCount.value().longValue() * 20 * 60 * yearDayCount));
                rvs.add(String.valueOf(formatFloatNumber.call((t._2 / 60) / (bRealLineCount.value().longValue() * 20 * 60 * yearDayCount))));
                rvs.add(String.valueOf(utilisation_target));
                rvs.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_line_utilisation_year", rvs));
            }
            return rows.iterator();
        });


        try {

            for (Row row : calculated_production_line_info_dd.take(5)) {
                System.out.println(row);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>calculated_production_line_info_dd End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_line_utilisation_year", calculated_production_line_info_dd, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_line_utilisation_year"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_line_utilisation_year"));
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
