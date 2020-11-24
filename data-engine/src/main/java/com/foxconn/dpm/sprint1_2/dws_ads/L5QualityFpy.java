package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.enums.SiteEnum;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

/**
 * @author HS
 * @className L5QualityFpy
 * @description TODO
 * @date 2020/4/22 15:04
 */
public class L5QualityFpy extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
    String etl_time = String.valueOf(System.currentTimeMillis());

    Float fpy_target = 0.0f;

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");
        doTaskBySiteEnum(map, SiteEnum.WH);
        doTaskBySiteEnum(map, SiteEnum.CQ);
        System.out.println("==============================>>>Programe End<<<==============================");
    }


    /**
     * 根据 site 执行 .
     * @param map
     * @param siteEnum
     * @author ws
     * @date 2020/6/30 11:25
     * @return void
     **/
    private void doTaskBySiteEnum(Map<String, Object> map, SiteEnum siteEnum) throws Exception {
        System.out.println("==========>>>current site " + siteEnum.getCode() + "<<<==========");

        //dpm_ods_production_target_values
        String fpyStr = "0.0";

        loadTargets(batchGetter.getStDateDayAdd(-1, "-"), siteEnum);
        loadQualityFpyToDB(batchGetter.getStDateDayStampAdd(-8, "-"), batchGetter.getStDateDayStampAdd(1, "-"), siteEnum);
        //day
        JavaPairRDD<String, Tuple2<Float, Float>> dayQualityFpy = loadQualityFpy(batchGetter.getStDateDayStampAdd(-8, "-"), batchGetter.getStDateDayStampAdd(1, "-"), siteEnum);
        calculateDayQualityFpy(dayQualityFpy, siteEnum);

        //weej
        loadTargets(batchGetter.getStDateWeekAdd(-1, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> weekQualityFpy = loadQualityFpy(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1, siteEnum);
        calculateWeekQualityFpy(weekQualityFpy, siteEnum);
        //weej
        loadTargets(batchGetter.getStDateWeekAdd(0, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> weekQualityFpy2 = loadQualityFpy(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1, siteEnum);
        calculateWeekQualityFpy(weekQualityFpy2, siteEnum);

        //month
        loadTargets(batchGetter.getStDateMonthAdd(-1, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> monthQualityFpy = loadQualityFpy(batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1, siteEnum);
        calculateMonthQualityFpy(monthQualityFpy, siteEnum);

        //month
        loadTargets(batchGetter.getStDateMonthAdd(0, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> monthQualityFpy2 = loadQualityFpy(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1, siteEnum);
        calculateMonthQualityFpy(monthQualityFpy2, siteEnum);

        //quarter
        loadTargets(batchGetter.getStDateQuarterAdd(-1, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> quarterQualityFpy = loadQualityFpy(batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1, siteEnum);
        calculateQuarterQualityFpy(quarterQualityFpy, siteEnum);

        //quarter
        loadTargets(batchGetter.getStDateQuarterAdd(0, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> quarterQualityFpy2 = loadQualityFpy(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1, siteEnum);
        calculateQuarterQualityFpy(quarterQualityFpy2, siteEnum);

        //year
        loadTargets(batchGetter.getStDateYearAdd(-1, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> yearQualityFpy = loadQualityFpy(batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1, siteEnum);
        calculateYearQualityFpy(yearQualityFpy, siteEnum);

        //year
        loadTargets(batchGetter.getStDateYearAdd(0, "-")._1, siteEnum);
        JavaPairRDD<String, Tuple2<Float, Float>> yearQualityFpy2 = loadQualityFpy(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1, siteEnum);
        calculateYearQualityFpy(yearQualityFpy2, siteEnum);
    }

    public void calculateDayQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData, SiteEnum siteEnum) throws Exception {
        JavaRDD<Row> rows = calculateQualityFpy(loadData, "dpm_ads_quality_fpy_day", siteEnum);

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : rows.collect()) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_day", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_day"));
    }

    public void calculateWeekQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData, SiteEnum siteEnum) throws Exception {

        JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy = loadData.mapToPair(new PairFunction<Tuple2<String, Tuple2<Float, Float>>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(Tuple2<String, Tuple2<Float, Float>> t) throws Exception {
                //"work_dt", "site_code", "level_code",
                String[] org = t._1.split(",");
                org[0] = String.valueOf(batchGetter.getDateWeek(org[0]));
                return new Tuple2<String, Tuple2<Float, Float>>(
                        batchGetter.getStrArrayOrg(",", "N/A",
                                org
                        ),
                        t._2
                );
            }
        }).reduceByKey((tv1, tv2) -> {
            return new Tuple2<Float, Float>(tv1._1 + tv2._1, tv1._2 + tv2._2);
        });

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_week", siteEnum);

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_week", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_week"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_week"));
    }

    public void calculateMonthQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData, SiteEnum siteEnum) throws Exception {
        JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy = loadData.mapToPair(new PairFunction<Tuple2<String, Tuple2<Float, Float>>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(Tuple2<String, Tuple2<Float, Float>> t) throws Exception {
                //"work_dt", "site_code", "level_code",
                String[] org = t._1.split(",");
                org[0] = org[0].replace("-", "").substring(0, 6);
                return new Tuple2<String, Tuple2<Float, Float>>(
                        batchGetter.getStrArrayOrg(",", "N/A",
                                org
                        ),
                        t._2
                );
            }
        }).reduceByKey((tv1, tv2) -> {
            return new Tuple2<Float, Float>(tv1._1 + tv2._1, tv1._2 + tv2._2);
        });

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_month", siteEnum);
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_month", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_month"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_month"));
    }

    public void calculateQuarterQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData, SiteEnum siteEnum) throws Exception {
        JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy = loadData.mapToPair(new PairFunction<Tuple2<String, Tuple2<Float, Float>>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(Tuple2<String, Tuple2<Float, Float>> t) throws Exception {
                //"work_dt", "site_code", "level_code",
                String[] org = t._1.split(",");
                org[0] = String.valueOf(org[0].substring(0, 4) + batchGetter.getTargetDateQuarter(org[1], "-"));
                return new Tuple2<String, Tuple2<Float, Float>>(
                        batchGetter.getStrArrayOrg(",", "N/A",
                                org
                        ),
                        t._2
                );
            }
        }).reduceByKey((tv1, tv2) -> {
            return new Tuple2<Float, Float>(tv1._1 + tv2._1, tv1._2 + tv2._2);
        });

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_quarter", siteEnum);
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_quarter", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_quarter"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_quarter"));
    }

    public void calculateYearQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData, SiteEnum siteEnum) throws Exception {
        JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy = loadData.mapToPair(new PairFunction<Tuple2<String, Tuple2<Float, Float>>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(Tuple2<String, Tuple2<Float, Float>> t) throws Exception {
                //"work_dt", "site_code", "level_code",
                String[] org = t._1.split(",");
                org[0] = org[0].substring(0, 4);
                return new Tuple2<String, Tuple2<Float, Float>>(
                        batchGetter.getStrArrayOrg(",", "N/A",
                                org
                        ),
                        t._2
                );
            }
        }).reduceByKey((tv1, tv2) -> {
            return new Tuple2<Float, Float>(tv1._1 + tv2._1, tv1._2 + tv2._2);
        });

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_year", siteEnum);
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_year", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_year"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_year"));
    }

    public void loadQualityFpyToDB(String startStamp, String endStamp, SiteEnum siteEnum) throws Exception {


        JavaRDD<Result> fpy_line_dd = DPHbase.saltRddRead("dpm_dws_quality_fpy_line_dd", startStamp, endStamp, new Scan(), true);

        if (fpy_line_dd == null) {
            System.out.println("==============================>>>dpm_dws_quality_fpy_line_dd NO Data End<<<==============================");
            return;
        }

        final JavaRDD<ArrayList<String>> map = fpy_line_dd.filter(r -> {
            return batchGetter.checkColumns(r, "DPM_DWS_QUALITY_FPY_LINE_DD",
                    "work_dt", "site_code"
            );
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_QUALITY_FPY_LINE_DD",
                    "work_dt", "work_shift", "site_code", "level_code", "line_code", "factory_code", "process_code", "customer_code", "defect_total_qty", "output_qty", "update_dt"
            );
        }).map(r -> {
            r.add(0, String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
            Float v = Float.valueOf(r.get(10)) - Float.valueOf(r.get(9));
            r.set(10, String.valueOf(v.intValue()));
            r.set(11, etl_time);
            r.add(12, "N/A");
            return r;
        });

        try {
            for (ArrayList<String> strings : map.take(5)) {
                System.out.println(strings);
            }
        } catch (Exception e) {

        }

        System.out.println("==============================>>>filted End<<<==============================");
        JavaRDD<Row> rowJavaRDD = map.mapPartitions(batchData -> {

            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            while (batchData.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_quality_fpy_detail_day", batchData.next()));
            }

            return rows.iterator();

        });
        try {

            for (Row row : rowJavaRDD.take(5)) {
                System.out.println(row);
            }

        } catch (Exception e) {

        }

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_detail_day", rowJavaRDD, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_detail_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_detail_day"));
        System.out.println("==============================>>>loadToDB End<<<==============================");

    }

    public JavaPairRDD<String, Tuple2<Float, Float>> loadQualityFpy(String startStamp, String endStamp, SiteEnum siteEnum) throws Exception {

        JavaRDD<Result> fpy_line_dd = DPHbase.saltRddRead("dpm_dws_quality_fpy_line_dd", startStamp, endStamp, new Scan(), true);

        if (fpy_line_dd == null) {
            System.out.println("==============================>>>dpm_dws_quality_fpy_line_dd NO Data End<<<==============================");
            return null;
        }

        JavaPairRDD<String, Tuple2<Float, Float>> stringTuple2JavaPairRDD = fpy_line_dd.filter(r -> {
            return batchGetter.checkColumns(r, "DPM_DWS_QUALITY_FPY_LINE_DD",
                    "work_dt", "defect_total_qty", "output_qty", "update_dt"
            );
        }).keyBy(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_QUALITY_FPY_LINE_DD",
                    "work_dt", "site_code", "level_code", "factory_code", "process_code", "customer_code", "line_code", "series_code"
            );
        }).reduceByKey((kv1, kv2) -> {

            return
                    Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_DWS_QUALITY_FPY_LINE_DD", "update_dt"))
                            >
                            Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWS_QUALITY_FPY_LINE_DD", "update_dt"))
                            ?
                            kv1
                            :
                            kv2;

        }).map(t -> {
            return t._2;
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_QUALITY_FPY_LINE_DD",
                    "work_dt", "site_code", "level_code", "factory_code", "process_code", "customer_code", "line_code", "series_code", "defect_total_qty", "output_qty", "update_dt"
            );
        }).filter(r -> {
            return siteEnum.getCode().equals(r.get(1)) && "L5".equals(r.get(2));
        }).mapToPair(new PairFunction<ArrayList<String>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(ArrayList<String> r) throws Exception {
                String org = batchGetter.getStrArrayOrg(",", "N/A", r.get(0), r.get(1), r.get(2), "N/A", "N/A", "N/A", "N/A", "N/A");
                return new Tuple2<String, Tuple2<Float, Float>>(org, new Tuple2<Float, Float>(batchGetter.formatFloat(r.get(8)), batchGetter.formatFloat(r.get(9))));
            }
        }).reduceByKey((tv1, tv2) -> {
            return new Tuple2<Float, Float>(batchGetter.formatFloat(tv1._1) + batchGetter.formatFloat(tv2._1), batchGetter.formatFloat(tv1._2) + batchGetter.formatFloat(tv2._2));
        });
        try {
            for (Tuple2<String, Tuple2<Float, Float>> stringTuple2Tuple2 : stringTuple2JavaPairRDD.take(5)) {
                System.out.println(stringTuple2Tuple2);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>calculatedOee End<<<==============================");

        return stringTuple2JavaPairRDD;

    }

    public JavaRDD<Row> calculateQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy, String tableName, SiteEnum siteEnum) {
        JavaRDD<Row> rowJavaRDD = aggTimeQualityFpy.mapPartitions(batchData -> {

            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            //"work_dt", "site_code", "level_code", "factory_code", "process_code", "customer_code", "line_code", "series_code",
            while (batchData.hasNext()) {
                ArrayList<Object> objects = new ArrayList<>();
                Tuple2<String, Tuple2<Float, Float>> t = batchData.next();
                String[] orgs = t._1.split(",");
                float unFpy = batchGetter.formatFloat(t._2._1) / batchGetter.formatFloat(t._2._2);
                Float l5_fpy = (1 - (unFpy == 0 ? 1 : unFpy)) * 100;

                objects.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
                objects.add(String.valueOf(orgs[0]));
                objects.add(String.valueOf(orgs[1]));
                objects.add(String.valueOf(orgs[2]));
                objects.add(String.valueOf(orgs[3]));
                objects.add(String.valueOf(orgs[4]));
                objects.add(String.valueOf(orgs[5]));
                objects.add(String.valueOf(orgs[6]));
                objects.add(String.valueOf(batchGetter.formatFloat(l5_fpy)));
                objects.add(String.valueOf(fpy_target));
                objects.add(String.valueOf(etl_time));
                rows.add(beanGetter.creDeftSchemaRow(tableName, objects));
            }

            return rows.iterator();

        });


        try {
            for (Row row : rowJavaRDD.take(5)) {
                System.out.println(row);
            }
        } catch (Exception e) {

        }

        System.out.println("==============================>>>rowJavaRDD End<<<==============================");

        return rowJavaRDD;
    }

    public void loadTargets(String lastDay, SiteEnum siteEnum) {

        try {
            JavaRDD<ArrayList<String>> listJavaRDD = LoadKpiTarget.loadProductionTarget(lastDay);

            String fpyStr = listJavaRDD.filter(r -> {
                boolean result = siteEnum.getCode().equals(r.get(2))
                        && "L5".equals(r.get(3)) && "all".equals(r.get(6));
                if (!result) {
                    return false;
                }
                if (SiteEnum.WH == siteEnum) {
                    return "all".equals(r.get(4)) && "all".equals(r.get(5));
                } else if (SiteEnum.CQ == siteEnum) {
                    // do nothing
                }
                return true;
            }).take(1).get(0).get(16);
            System.out.println("target data " + fpyStr);
            Float aFloat = Float.valueOf(fpyStr);
            // 重庆则乘以 100
            if (SiteEnum.CQ == siteEnum) {
                aFloat = new BigDecimal(fpyStr).multiply(new BigDecimal(100)).floatValue();
            }

            fpy_target = aFloat == null ? 0.0f : aFloat;
        } catch (Exception e) {
            fpy_target = 0.0f;
        }
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
