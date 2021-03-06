package com.foxconn.dpm.sprint5.dws_ads;

import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author HS
 * @className L5QualityFpy
 * @description TODO
 * @date 2020/4/22 15:04
 */
public class L5QualityFpySprintFive extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

    Map<String, Float> fpy_targets = new HashMap<>();
    String etl_time = String.valueOf(System.currentTimeMillis());
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {



        //day
        loadTargets(batchGetter.getStDateDayAdd(0, "-"));
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> dayQualityFpy = loadQualityFpy(batchGetter.getStDateDayStampAdd(-8, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateDayQualityFpy(dayQualityFpy);
        clearTable();


        //weej
        loadTargets(batchGetter.getStDateWeekAdd(-1, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> weekQualityFpy = loadQualityFpy(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        calculateWeekQualityFpy(weekQualityFpy);
        clearTable();
        //weej
        loadTargets(batchGetter.getStDateWeekAdd(0, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> weekQualityFpy2 = loadQualityFpy(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        calculateWeekQualityFpy(weekQualityFpy2);
        clearTable();


        //month
        loadTargets(batchGetter.getStDateMonthAdd(-1, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> monthQualityFpy = loadQualityFpy(batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1);
        calculateMonthQualityFpy(monthQualityFpy);
        clearTable();
        //month
        loadTargets(batchGetter.getStDateMonthAdd(0, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> monthQualityFpy2 = loadQualityFpy(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        calculateMonthQualityFpy(monthQualityFpy2);
        clearTable();

        //quarter
        loadTargets(batchGetter.getStDateQuarterAdd(-1, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> quarterQualityFpy = loadQualityFpy(batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        calculateQuarterQualityFpy(quarterQualityFpy);
        clearTable();
        //quarter
        loadTargets(batchGetter.getStDateQuarterAdd(0, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> quarterQualityFpy2 = loadQualityFpy(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        calculateQuarterQualityFpy(quarterQualityFpy2);
        clearTable();

        //year
        loadTargets(batchGetter.getStDateYearAdd(-1, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> yearQualityFpy = loadQualityFpy(batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1);
        calculateYearQualityFpy(yearQualityFpy);
        clearTable();
        //year
        loadTargets(batchGetter.getStDateYearAdd(0, "-")._2);
        System.out.println(fpy_targets.toString());
        JavaPairRDD<String, Tuple2<Float, Float>> yearQualityFpy2 = loadQualityFpy(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        calculateYearQualityFpy(yearQualityFpy2);
        clearTable();
    }

    public void calculateDayQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData) throws Exception {
        /*  - id=String
         * - work_date=String
         * - site_code=String
         * - level_code=String
         * - factory_code=String
         * - process_code=String
         * - customer_code=String
         * - line_code=String
         * - block_code=String
         * - fpy_actual=Float
         * - fpy_target=Float
         * - etl_time=String
         */
        JavaRDD<Row> rows = calculateQualityFpy(loadData, "dpm_ads_quality_fpy_day");

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            sqlContext.sql("select * from dpm_ods_production_target_values").show(2000);
            for (Row row : rows.collect()) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dpm_ads_quality_fpy_day", rows,
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_day",
                        DataTypes.createStructField("block_code", DataTypes.StringType, true)
                ),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_day",
                        new Tuple2<>(11, DataTypes.createStructField("block_code", DataTypes.StringType, true))
                )
        );
        System.out.println("==============================>>>day End<<<==============================");
    }

    public void calculateWeekQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData) throws Exception {
        JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy = loadData.mapToPair(new PairFunction<Tuple2<String, Tuple2<Float, Float>>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(Tuple2<String, Tuple2<Float, Float>> t) throws Exception {
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

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_week");

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : rows.collect()) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_week", rows,
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_week",
                        DataTypes.createStructField("block_code", DataTypes.StringType, true)
                ),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_week",
                        new Tuple2<>(11, DataTypes.createStructField("block_code", DataTypes.StringType, true))
                )
        );
        System.out.println("==============================>>>week End<<<==============================");
    }

    public void calculateMonthQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData) throws Exception {
        JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy = loadData.mapToPair(new PairFunction<Tuple2<String, Tuple2<Float, Float>>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(Tuple2<String, Tuple2<Float, Float>> t) throws Exception {
                //"work_dt", "site_code", "level_code",
                String[] org = t._1.split(",");
                org[0] =  org[0].replace("-", "").substring(0, 6);
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

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_month");
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : rows.collect()) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_month", rows,
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_month",
                        DataTypes.createStructField("block_code", DataTypes.StringType, true)
                ),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_month",
                        new Tuple2<>(11, DataTypes.createStructField("block_code", DataTypes.StringType, true))
                )
        );
        System.out.println("==============================>>>month End<<<==============================");
    }

    public void calculateQuarterQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData) throws Exception {
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

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_quarter");
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : rows.collect()) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_quarter", rows,
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_quarter",
                        DataTypes.createStructField("block_code", DataTypes.StringType, true)
                ),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_quarter",
                        new Tuple2<>(11, DataTypes.createStructField("block_code", DataTypes.StringType, true))
                )
        );
        System.out.println("==============================>>>quarter End<<<==============================");
    }

    public void calculateYearQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData) throws Exception {
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

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_year");
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : rows.collect()) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_quality_fpy_year", rows,
                MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_year",
                        DataTypes.createStructField("block_code", DataTypes.StringType, true)
                ),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_year",
                        new Tuple2<>(11, DataTypes.createStructField("block_code", DataTypes.StringType, true))
                )
        );
        System.out.println("==============================>>>year End<<<==============================");
    }

    public JavaPairRDD<String, Tuple2<Float, Float>> loadQualityFpy(String startStamp, String endStamp) throws Exception {


        JavaRDD<Result> fpy_line_dd = DPHbase.saltRddRead("dpm_dws_quality_fpy_line_dd", startStamp, endStamp, new Scan(), true);

        if (fpy_line_dd == null) {
            System.out.println("==============================>>>dpm_dws_quality_fpy_line_dd NO Data End<<<==============================");
            return null;
        }

        JavaPairRDD<String, Tuple2<Float, Float>> stringTuple2JavaPairRDD = fpy_line_dd.keyBy(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DWS_QUALITY_FPY_LINE_DD",
                    "work_dt", "site_code", "level_code", "factory_code", "process_code", "customer_code", "line_code", "series_code", "area_code"
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
                    "work_dt", "site_code", "level_code", "factory_code", "process_code", "customer_code", "line_code", "series_code", "defect_total_qty", "output_qty", "update_dt", "area_code"
            );
        }).filter(r -> {
            return "L5".equals(r.get(2));
        }).mapToPair(new PairFunction<ArrayList<String>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(ArrayList<String> r) throws Exception {

                /*
                 * 塗裝 成型 衝壓 涂装 沖壓 组装 組裝
                 */
                String processCode = r.get(4);
                try {
                    switch (processCode) {
                        case "塗裝":
                        case "涂装":
                            processCode = "Painting";
                            break;
                        case "成型":
                            processCode = "Molding";
                            break;
                        case "衝壓":
                        case "沖壓":
                            processCode = "Stamping";
                            break;
                        case "组装":
                        case "組裝":
                            processCode = "Assy";
                            break;
                    }
                } catch (Exception e) {
                    processCode = "N/A";
                }

                String site_code = r.get(1);
                switch (site_code) {
                    case "武漢":
                    case "武汉":
                        site_code = "WH";
                        break;
                    case "重庆":
                    case "重慶":
                        site_code = "CQ";
                        break;
                }
                String area_code = r.get(11);
                String line_code = r.get(6);

                //line_code = batchGetter.cleanChinese(line_code);
                if (line_code.matches(".+線$")){
                    line_code = line_code.replace("線", "");
                }
                if (!(processCode.equals("Molding") && site_code.equals("WH") && r.get(2).equals("L5"))) {
                    area_code = "NULL";
                }else{
                    switch (area_code) {
                        case "裝配":
                        case "装配":
                            area_code = "Assy";
                            break;
                    }
                    line_code = "NULL";
                }

                //work_dt site_code level_code factory_code process_code customer_code line_code series_code area_code
                String org = batchGetter.getStrArrayOrg(",", "N/A",
                        r.get(0), site_code, r.get(2), r.get(3), processCode, "N/A", line_code, "N/A", area_code
                );
                return new Tuple2<String, Tuple2<Float, Float>>(org, new Tuple2<Float, Float>(batchGetter.formatFloat(r.get(8)), batchGetter.formatFloat(r.get(9))));
            }
        }).reduceByKey((tv1, tv2) -> {
            return new Tuple2<Float, Float>(batchGetter.formatFloat(tv1._1 + tv2._1), batchGetter.formatFloat(tv1._2 + tv2._2));
        });

        return stringTuple2JavaPairRDD;

    }

    public JavaRDD<Row> calculateQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy, String tableName) {
        final StructType targetStructType = MetaGetter.getBeanGetter().getDeftSchemaStruct(tableName,
                new Tuple2<>(11, DataTypes.createStructField("block_code", DataTypes.StringType, true))
        );
        JavaRDD<Row> rowJavaRDD = aggTimeQualityFpy.mapPartitions(batchData -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();

            //"work_dt", "site_code", "level_code", "factory_code", "process_code", "customer_code", "line_code",  series_code  area_code
            while (batchData.hasNext()) {
                ArrayList<Object> objects = new ArrayList<>();
                Tuple2<String, Tuple2<Float, Float>> t = batchData.next();
                String[] orgs = t._1.split(",");
                Float fpy_target = fpy_targets.get(orgs[1] + "," + orgs[2] + "," + orgs[3] + "," + orgs[4] + "," + orgs[6] + "," + orgs[8]);
                Float formatFpy = batchGetter.formatFloat(t._2._1 / t._2._2);
                Float l5_fpy = (1 - (formatFpy == 0 ? 1 : formatFpy)) * 100;
                objects.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
                objects.add(String.valueOf(orgs[0]));
                objects.add(String.valueOf(orgs[1]));
                objects.add(String.valueOf(orgs[2]));
                objects.add(String.valueOf(orgs[3]));
                objects.add(String.valueOf(orgs[4]));
                objects.add(String.valueOf(orgs[5]));
                objects.add(String.valueOf(orgs[6]));
                objects.add(String.valueOf(l5_fpy));
                objects.add(String.valueOf(fpy_target == null ? 0.0f : fpy_target));
                objects.add(etl_time);
                objects.add(String.valueOf(orgs[8]));
                rows.add(beanGetter.creSchemaRow(targetStructType, objects));
            }

            return rows.iterator();

        });

        return rowJavaRDD;
    }

    public void clearTable() {
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }

    public void loadTargets(String lastDay) throws Exception {
        fpy_targets.clear();
        LoadKpiTarget.loadProductionTarget(lastDay).filter(r -> {
            return "D".equals(r.get(1)) && "WH".equals(r.get(2)) && "L5".equals(r.get(3)) && !"all".equals(r.get(4)) && !"all".equals(r.get(5)) && !"all".equals(r.get(6)) && "all".equals(r.get(7));
        }).mapToPair(new PairFunction<ArrayList<String>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(ArrayList<String> r) throws Exception {
                return new Tuple2<String, Float>(
                        batchGetter.getStrArrayOrg(",", "N/A",
                                r.get(2), r.get(3), r.get(4), r.get(5), r.get(6)
                        )
                        , batchGetter.formatFloat(r.get(16)));
            }
        }).collectAsMap().forEach(new BiConsumer<String, Float>() {
            @Override
            public void accept(String k, Float v) {
                fpy_targets.put(k, v);
            }
        });
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
