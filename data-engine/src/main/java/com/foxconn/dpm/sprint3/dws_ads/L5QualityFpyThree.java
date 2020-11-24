package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.DpMysql;
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * @author HS
 * @className L5QualityFpy
 * @description TODO
 * @date 2020/4/22 15:04
 */
public class L5QualityFpyThree extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
    String etl_time = String.valueOf(System.currentTimeMillis());
    Map<String, Float> fpy_targets;

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {


        //dpm_ods_production_target_values
        loadTargets(batchGetter.getStDateDayAdd(-1, "-"));
        System.out.println(fpy_targets.toString());
        loadQualityFpyToDB(batchGetter.getStDateDayStampAdd(-8, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        //day
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
        JavaRDD<Row> rows = calculateQualityFpy(loadData, "dpm_ads_quality_fpy_day");
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            sqlContext.sql("select * from dpm_ods_production_target_values").show(200);
            for (Row row : rows.collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        StructType structType = MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_day", new Tuple2<>(11, DataTypes.createStructField("Counter", DataTypes.StringType, true)));
        //"defect_total_qty", "output_qty"
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_day", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_day"), structType);

        JavaRDD<Row> rowJavaRDD = calculateL5LevelFpy(rows, structType);
        System.out.println("==============================>>>L5 Level FPY QA Log Start<<<==============================");
        try {
            for (Row row : rowJavaRDD.collect()) {
                System.out.println(row);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>L5 Level FPY QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_day", rowJavaRDD, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_day"), structType);

    }

    public void calculateWeekQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData) throws Exception {
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
        StructType structType = MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_week", new Tuple2<>(11, DataTypes.createStructField("Counter", DataTypes.StringType, true)));
        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_week");
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_week", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_week"), structType);


        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_week", calculateL5LevelFpy(rows, structType), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_week"), structType);
    }

    public void calculateMonthQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> loadData) throws Exception {
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

        JavaRDD<Row> rows = calculateQualityFpy(aggTimeQualityFpy, "dpm_ads_quality_fpy_month");
        StructType structType = MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_month", new Tuple2<>(11, DataTypes.createStructField("Counter", DataTypes.StringType, true)));
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_month", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_month"), structType);


        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_month", calculateL5LevelFpy(rows, structType), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_month"), structType);
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
        StructType structType = MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_quarter", new Tuple2<>(11, DataTypes.createStructField("Counter", DataTypes.StringType, true)));
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_quarter", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_quarter"), structType);


        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_quarter", calculateL5LevelFpy(rows, structType), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_quarter"), structType);
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
        StructType structType = MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_year", new Tuple2<>(11, DataTypes.createStructField("Counter", DataTypes.StringType, true)));
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_year", rows, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_year"), structType);


        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_year", calculateL5LevelFpy(rows, structType), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_year"), structType);
    }

    public void loadQualityFpyToDB(String startStamp, String endStamp) throws Exception {


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
            r.set(11, String.valueOf(System.currentTimeMillis()));
            return r;
        });
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
        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_detail_day", rowJavaRDD, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_quality_fpy_detail_day"), MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_quality_fpy_detail_day"));
        System.out.println("==============================>>>loadToDB End<<<==============================");
    }

    public JavaPairRDD<String, Tuple2<Float, Float>> loadQualityFpy(String startStamp, String endStamp) throws Exception {


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
        }).map(r -> {

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
            r.set(4, processCode);
            return r;
        }).filter(r -> {
            return "L5".equals(r.get(2));
        }).mapToPair(new PairFunction<ArrayList<String>, String, Tuple2<Float, Float>>() {
            @Override
            public Tuple2<String, Tuple2<Float, Float>> call(ArrayList<String> r) throws Exception {
                String customer = r.get(1).equals("WH") &&  r.get(2).equals("L5") &&  r.get(3).equals("DT1") ? r.get(5) : "N/A";
                String org = batchGetter.getStrArrayOrg(",", "N/A", r.get(0), r.get(1), r.get(2), r.get(3), "N/A", customer, "N/A", "N/A");
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
        System.out.println("==============================>>>fpy_line_dd End<<<==============================");

        return stringTuple2JavaPairRDD;

    }

    public JavaRDD<Row> calculateQualityFpy(JavaPairRDD<String, Tuple2<Float, Float>> aggTimeQualityFpy, String tableName) {
        JavaRDD<Row> rowJavaRDD = aggTimeQualityFpy.mapPartitions(batchData -> {

            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            //"work_dt", "site_code", "level_code", "factory_code", "process_code", "customer_code", "line_code", "series_code",
            while (batchData.hasNext()) {
                ArrayList<Object> objects = new ArrayList<>();
                Tuple2<String, Tuple2<Float, Float>> t = batchData.next();
                String[] orgs = t._1.split(",");
                Float fpy_target = fpy_targets.get(orgs[1] + "," + orgs[2] + "," + orgs[3] + "," + ( orgs[1].equals("WH") &&  orgs[2].equals("L5") &&  orgs[3].equals("DT1") ? orgs[5] : "all"));
                float unFpy = batchGetter.formatFloat(t._2._1 / t._2._2);
                Float l5_fpy = (1 - unFpy) * 100;
                objects.add(String.valueOf(System.currentTimeMillis() + "-" + UUID.randomUUID().toString()));
                objects.add(String.valueOf(orgs[0]));
                objects.add(String.valueOf(orgs[1]));
                objects.add(String.valueOf(orgs[2]));
                objects.add(String.valueOf(orgs[3]));
                objects.add(String.valueOf(orgs[4]));
                objects.add(String.valueOf(orgs[5]));
                objects.add(String.valueOf(orgs[6]));
                objects.add(String.valueOf(batchGetter.formatFloat(l5_fpy)));
                objects.add(String.valueOf(fpy_target == null ? 0.0f : fpy_target));
                objects.add(etl_time);
                objects.add((String.valueOf(t._2._1).concat("_").concat(String.valueOf(t._2._2))));
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

    public void clearTable() {
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }

    public JavaRDD<Row> calculateL5LevelFpy(JavaRDD<Row> resultRows, StructType schema) {
        JavaRDD<Row> l5FactoryFpy = sqlContext.createDataFrame(resultRows, schema).where("level_code='L5' and factory_code in ('DT1', 'DT2') ").javaRDD();
        return l5FactoryFpy.keyBy(r -> {
            //work_date site_code level_code
            return String.valueOf(r.get(1)).concat(r.getString(2)).concat(r.getString(3));
        }).groupByKey().map(t -> {

            ArrayList<Row> rows = new ArrayList<>();
            Iterator<Row> it = t._2.iterator();
            while (it.hasNext()) {
                rows.add(it.next());
            }
            ArrayList<Object> resultObj = new ArrayList<Object>();
            resultObj.addAll(JavaConverters.seqAsJavaListConverter(rows.get(0).toSeq()).asJava());
            /**
             * * id=String
             * work_date=String
             * site_code=String
             * level_code=String
             * factory_code=String
             * process_code=String
             * customer_code=String
             * line_code=String
             * fpy_actual=Float
             * fpy_target=Float
             * etl_time=String
             * Counter  该字段为字符串 放 "defect_total_qty", "output_qty"
             */
            //new id
            resultObj.set(0, String.valueOf(System.currentTimeMillis()).concat(UUID.randomUUID().toString().replace("-", "")));
            resultObj.set(4, "N/A");//new Factory code
            /*----------------------------------------------------------------------------------------------*/
            /**
             * ====================================================================
             * 描述:
             *      DT1    A Fpy 实际值 B Fpy目标值  E  defect_total_qty G output_qty
             *      DT2    C Fpy 实际值 D Fpy目标值  F  defect_total_qty H output_qty
             *      Level Fpy target  (B * A + D * C) / (A + C)
             *      Level Fpy actual  (E + F) / (G + H)
             * ====================================================================
             */
            //分子_分母
            Float defect_total_qty = 0f;
            Float output_qty = 0f;
            Float fpy_hv_sum = 0f;
            Float fpy_actual_sum = 0f;
            for (Row row : rows) {
                ArrayList<Object> temp = new ArrayList<Object>();
                temp.addAll(JavaConverters.seqAsJavaListConverter(row.toSeq()).asJava());
                String[] qtyStr = String.valueOf(temp.get(11)).split("_");
                defect_total_qty += batchGetter.formatFloat(qtyStr[0]);
                output_qty += batchGetter.formatFloat(qtyStr[1]);
                fpy_hv_sum += batchGetter.formatFloat(batchGetter.formatFloat(temp.get(8)) / 100) * batchGetter.formatFloat(batchGetter.formatFloat(temp.get(9)) / 100);
                fpy_actual_sum += batchGetter.formatFloat(batchGetter.formatFloat(temp.get(8)) / 100);
            }
            resultObj.set(8,  (1 - batchGetter.formatFloat(defect_total_qty / output_qty)) * 100);// Fpy actual
            resultObj.set(9,fpy_hv_sum / fpy_actual_sum * 100);// Fpy target
            resultObj.set(10, etl_time);
            return new GenericRowWithSchema(resultObj.toArray(new Object[0]), schema);
        });
    }

    public void loadTargets(String workDt) throws Exception {
        fpy_targets = LoadKpiTarget.loadProductionTarget(workDt).filter(r -> {
            return "WH".equals(r.get(2)) && "L5".equals(r.get(3)) && !"all".equals(r.get(4)) && "all".equals(r.get(5)) && "all".equals(r.get(6));
        }).mapToPair(new PairFunction<ArrayList<String>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(ArrayList<String> r) throws Exception {
                return new Tuple2<String, Float>(
                        batchGetter.getStrArrayOrg(",", "N/A",
                                r.get(2), r.get(3), r.get(4), r.get(24)
                        )
                        , batchGetter.formatFloat(r.get(16)) * 100);
            }
        }).collectAsMap();
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
