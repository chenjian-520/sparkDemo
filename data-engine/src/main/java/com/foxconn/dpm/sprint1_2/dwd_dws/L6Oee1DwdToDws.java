package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.enums.SiteEnum;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import scala.*;

import java.lang.Long;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className L6Oee1DwdToDws
 * @data 2019/12/20
 *
 * L6Oee
 * 输入表：
 *   dpm_dwd_production_standary_ct 标准CT
 *   dpm_dwd_production_output 日产出SN基础资料
 *   lineCode() 线对应关系方法。UDF函数为LineTotranfView() 在com.foxconn.dpm.target_const.LoadKpiTarget#getLineDateset()
 * 输出表：
 * dpm_dws_production_partno_dd 料号平台生产状态统计 (L6 OEE因子數據 L10 UPH因子數據)
 *
 */
public class L6Oee1DwdToDws extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
//        初始化环境
//        初始化时间
        String tomoreStamp = batchGetter.getStDateDayStampAdd(2);
        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
        JavaRDD<Tuple5<String, String, String, String, String>> standaryCtRdd1 = DPHbase.rddRead("dpm_dwd_production_standary_ct", new Scan(), true).map(r -> {
            return new Tuple5<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("part_no"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("cycle_time"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("update_dt")))
            );
        }).persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> productionRDD1 = DPHbase.saltRddRead("dpm_dwd_production_output", yesterdayStamp, tomoreStamp, new Scan(), true).map(r -> {
            return new Tuple22<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("level_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("factory_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("process_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("area_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("machine_id"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("part_no"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sku"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("platform"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("customer"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("wo"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("workorder_type"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_dt"))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_shift")))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sn"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_name"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("is_fail"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("output_qty")))
            );
        }).persist(StorageLevel.MEMORY_AND_DISK());

        l6OeeCalculateWH(standaryCtRdd1,yesterdayStamp,productionRDD1,SiteEnum.WH);
        l6OeeCalculateCQ(standaryCtRdd1,yesterdayStamp,productionRDD1,SiteEnum.CQ);
        DPSparkApp.stop();
    }

    private void l6OeeCalculateWH(JavaRDD<Tuple5<String, String, String, String, String>> standaryCtRdd1,String yesterdayStamp,JavaRDD<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> productionRDD1, SiteEnum siteEnum) throws Exception {
        System.out.println("==============================>>>WH Programe Start<<<==============================");

        JavaPairRDD<Tuple2<String, String>, Tuple5<String, String, String, String,String>> standaryCtRdd = standaryCtRdd1.filter(r -> siteEnum.getCode().equals(r._4()))
                .keyBy(r -> new Tuple2<>(r._1(), r._2()))
                .coalesce(10, false)
                .reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return Long.valueOf(v1._5()) >= Long.valueOf(v2._5()) ? v1 : v2;
                }).mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>>, Tuple2<String, String>, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>> call(Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>> tuple2Tuple5Tuple2) throws Exception {
                        return tuple2Tuple5Tuple2;
                    }
                });

        standaryCtRdd.take(5).forEach(r -> System.out.println(r));

        String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(yesterdayStamp)));

        JavaPairRDD<Tuple9<String,String, String, String, String, String, String,String, String>, Iterable<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>>
                productionRDD = productionRDD1.filter(r -> siteEnum.getCode().equals(r._1()) && "L6".equals(r._2()) && format.equals(r._14()))
                .keyBy(r -> {
                    //day site level sn station_code去重
                    return new Tuple5<>(r._1(),r._2(),r._16(),r._17(),r._14());
                }).coalesce(10, false).reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return v1;
                }).map(r -> {
                    //还原RDD
                    return r._2();
                    //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","work_dt"
                }).keyBy(r -> new Tuple9<>(r._1(),r._2(),r._3(),r._4(),r._5(),r._6(), r._8(),r._10(),r._14())).groupByKey().persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println(productionRDD.count());
        System.out.println("----------------0-------------------");

        JavaPairRDD<String, Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> stringTuple15JavaPairRDD = productionRDD.map(r -> {
            HashSet<String> setHash = new HashSet<>();
            List<String> listQty = new ArrayList<>();
            Iterator<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> iterator = r._2.iterator();
            while (iterator.hasNext()) {
                Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> next = iterator.next();
                if ("0".equals(next._19().trim())) {
                    listQty.add(next._16());
                    if ("PACKING".equals(next._17().trim())) {
                        setHash.add(next._16());
                    }
                }
            }
            Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> first = r._2.iterator().next();
            return new Tuple15<>(first._1(), first._2(), first._3(), first._4(), first._5(), first._6(), first._8(), first._10(), first._14(), first._15(), String.valueOf(setHash.size()), String.valueOf(listQty.size()), yesterdayStamp, "cj", "dpm_dwd_production_output");
        }).keyBy(r -> r._6());

        stringTuple15JavaPairRDD.take(5).forEach(r -> System.out.println(r));
        System.out.println("-----------------1------------------");

        JavaSparkContext context = DPSparkApp.getContext();
        JavaPairRDD<String, Tuple2<String, String>> lineToSfc = context.parallelize(lineCode()).map(r -> {
            String[] split = r.split(",");
            return new Tuple2<String, String>(split[0], split[1]);
        }).keyBy(r -> r._2);
        lineToSfc.take(5).forEach(r -> System.out.println(r));

        JavaRDD<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> tuple15JavaRDD = stringTuple15JavaPairRDD.leftOuterJoin(lineToSfc).map(r -> {
            Tuple2<String, String> stringStringTuple2 = r._2._2.orElse(null);
            if (stringStringTuple2 == null) {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._6(), "xxx", r._2._1._7(), r._2._1._8(),
                        r._2._1._9(), r._2._1._10(), r._2._1._11(), r._2._1._12(),
                        r._2._1._13(), r._2._1._14(), r._2._1._15());
            } else {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._6(), stringStringTuple2._1, r._2._1._7(), r._2._1._8(),
                        r._2._1._9(), r._2._1._10(), r._2._1._11(), r._2._1._12(),
                        r._2._1._13(), r._2._1._14(), r._2._1._15());
            }
        });

        tuple15JavaRDD.take(5).forEach(r -> System.out.println(r));
        System.out.println("------------------6-----------------");

//        WH,L6,,,,J6S3CT,P3A,L02065-001,HPQ,2020-01-16,D,178,,1579104000000,cj,P3A
        JavaRDD<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> partnoRdd = tuple15JavaRDD.keyBy(r -> new Tuple2<>(r._7(), r._8())).leftOuterJoin(standaryCtRdd).map(r -> {
            Tuple5<String, String, String, String,String> stringStringStringTuple3 = r._2._2.orElse(null);
            if (stringStringStringTuple3 == null) {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._7(), r._2._1._8(), r._2._1._9(),
                        r._2._1._10(), r._2._1._11(), r._2._1._12(), r._2._1._13(),
                        "0", r._2._1._14(), r._2._1._15(), r._2._1._6());
            } else {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._7(), r._2._1._8(), r._2._1._9(),
                        r._2._1._10(), r._2._1._11(), r._2._1._12(), r._2._1._13(),
                        stringStringStringTuple3._3(), r._2._1._14(), r._2._1._15(), r._2._1._6());
            }
        });

        System.out.println("-----------------2------------------");
        partnoRdd.filter(r -> !("0".equals(r._11()))).take(200).forEach(r -> System.out.println(r._6() + "," + r._7() + "," + r._11() + "," + r._13() + "," + r._16()));

        System.out.println("-----------------3------------------");

        JavaRDD<Put> putJavaRdd = partnoRdd.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
            while (r.hasNext()) {
                Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> line = r.next();
                //salt+work_dt+site_code+level_code+line_code+part_no+platform+data_from
                String baseRowKey = sb.append(line._14()).append(":").append(line._1()).append(":").append(line._2()).append(":").append(line._6()).append(":").append(line._7()).append(":").append(line._9()).append(":").append(line._16()).toString();
                String salt = consistentHashLoadBalance.selectNode(baseRowKey);
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(line._1()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("level_code"), Bytes.toBytes(line._2()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("factory_code"), Bytes.toBytes(line._3()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("process_code"), Bytes.toBytes(line._4()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("area_code"), Bytes.toBytes(line._5()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("line_code"), Bytes.toBytes(line._6()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("part_no"), Bytes.toBytes(line._7()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("platform"), Bytes.toBytes(line._8()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("work_dt"), Bytes.toBytes(line._9()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("work_shift"), Bytes.toBytes(line._10()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("smt_ttl_pass"), Bytes.toBytes(line._11()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("output_qty"), Bytes.toBytes(line._12()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("ct"), Bytes.toBytes(line._13()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("data_granularity"), Bytes.toBytes("level"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_dt"), Bytes.toBytes(line._14()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_by"), Bytes.toBytes(line._15()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("data_from"), Bytes.toBytes(line._16()));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        putJavaRdd.take(5).forEach(r -> System.out.println(r));
//        dpm_dwd_production_partno_day 写入 dpm_dws_production_partno_day
        DPHbase.rddWrite("dpm_dws_production_partno_dd", putJavaRdd);

        System.out.println("==============================>>>WH Programe End<<<==============================");
    }

    private void l6OeeCalculateCQ(JavaRDD<Tuple5<String, String, String, String, String>> standaryCtRdd1,String yesterdayStamp,JavaRDD<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> productionRDD1, SiteEnum siteEnum) throws Exception {
        System.out.println("==============================>>>CQ Programe Start<<<==============================");

        JavaPairRDD<Tuple2<String, String>, Tuple5<String, String, String, String,String>> standaryCtRdd = standaryCtRdd1.filter(r -> siteEnum.getCode().equals(r._4()))
                .keyBy(r -> new Tuple2<>(r._1(), r._2()))
                .coalesce(10, false)
                .reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return Long.valueOf(v1._5()) >= Long.valueOf(v2._5()) ? v1 : v2;
                }).mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>>, Tuple2<String, String>, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>> call(Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>> tuple2Tuple5Tuple2) throws Exception {
                        return tuple2Tuple5Tuple2;
                    }
                });

        standaryCtRdd.take(5).forEach(r -> System.out.println(r));

        String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(yesterdayStamp)));

        JavaPairRDD<Tuple9<String,String, String, String, String, String, String,String, String>, Iterable<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>>
                productionRDD = productionRDD1.filter(r -> siteEnum.getCode().equals(r._1()) && "L6".equals(r._2()) && format.equals(r._14()))
                .keyBy(r -> {
                    //day site level sn station_code去重
                    return new Tuple5<>(r._1(),r._2(),r._16(),r._17(),r._14());
                }).coalesce(10, false).reduceByKey((v1, v2) -> {
                    //取最新的数据
                    return v1;
                }).map(r -> {
                    //还原RDD
                    return r._2();
                    ////"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","work_dt"
                }).keyBy(r -> new Tuple9<>(r._1(),r._2(),r._3(),r._4(),r._5(),r._6(), r._8(),r._10(),r._14())).groupByKey().persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println(productionRDD.count());
        System.out.println("----------------0-------------------");

        JavaPairRDD<String, Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> stringTuple15JavaPairRDD = productionRDD.map(r -> {
            HashSet<String> setHash = new HashSet<>();
            List<String> listQty = new ArrayList<>();
            Iterator<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> iterator = r._2.iterator();
            while (iterator.hasNext()) {
                Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> next = iterator.next();
                if ("0".equals(next._19().trim())) {
                    listQty.add(next._16());
                    if ("SMT".equals(next._4().trim())) {
//                    if ("PACKING".equals(next._17().trim())) {
                        setHash.add(next._16());
                    }
                }
            }
            Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> first = r._2.iterator().next();
            return new Tuple15<>(first._1(), first._2(), first._3(), first._4(), first._5(), first._6(), first._8(), first._10(), first._14(), first._15(), String.valueOf(setHash.size()), String.valueOf(listQty.size()), yesterdayStamp, "cj", "dpm_dwd_production_output");
        }).keyBy(r -> r._6());

        stringTuple15JavaPairRDD.take(5).forEach(r -> System.out.println(r));
        System.out.println("-----------------1------------------");

        JavaSparkContext context = DPSparkApp.getContext();
        JavaPairRDD<String, Tuple2<String, String>> lineToSfc = context.parallelize(lineCodeCQ()).map(r -> {
            String[] split = r.split(",");
            return new Tuple2<String, String>(split[0], split[1]);
        }).keyBy(r -> r._2);
        lineToSfc.take(5).forEach(r -> System.out.println(r));

        JavaRDD<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> tuple15JavaRDD = stringTuple15JavaPairRDD.leftOuterJoin(lineToSfc).map(r -> {
            Tuple2<String, String> stringStringTuple2 = r._2._2.orElse(null);
            if (stringStringTuple2 == null) {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._6(), r._2._1._6(), r._2._1._7(), r._2._1._8(),
                        r._2._1._9(), r._2._1._10(), r._2._1._11(), r._2._1._12(),
                        r._2._1._13(), r._2._1._14(), r._2._1._15());
            } else {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._6(), stringStringTuple2._1, r._2._1._7(), r._2._1._8(),
                        r._2._1._9(), r._2._1._10(), r._2._1._11(), r._2._1._12(),
                        r._2._1._13(), r._2._1._14(), r._2._1._15());
            }
        });

        tuple15JavaRDD.take(5).forEach(r -> System.out.println(r));
        System.out.println("------------------6-----------------");

//        WH,L6,,,,J6S3CT,P3A,L02065-001,HPQ,2020-01-16,D,178,,1579104000000,cj,P3A
        JavaRDD<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> partnoRdd = tuple15JavaRDD.keyBy(r -> new Tuple2<>(r._7(), r._8())).leftOuterJoin(standaryCtRdd).map(r -> {
            Tuple5<String, String, String, String,String> stringStringStringTuple3 = r._2._2.orElse(null);
            if (stringStringStringTuple3 == null) {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._7(), r._2._1._8(), r._2._1._9(),
                        r._2._1._10(), r._2._1._11(), r._2._1._12(), r._2._1._13(),
                        "0", r._2._1._14(), r._2._1._15(), r._2._1._6());
            } else {
                return new Tuple16<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(),
                        r._2._1._5(), r._2._1._7(), r._2._1._8(), r._2._1._9(),
                        r._2._1._10(), r._2._1._11(), r._2._1._12(), r._2._1._13(),
                        stringStringStringTuple3._3(), r._2._1._14(), r._2._1._15(), r._2._1._6());
            }
        });

        System.out.println("-----------------2------------------");
        partnoRdd.filter(r -> !("0".equals(r._11()))).take(200).forEach(r -> System.out.println(r._9() + "," +r._6() + "," + r._7() + "," + r._11() + "," + r._13() + "," + r._16()));

        System.out.println("-----------------3------------------");

        JavaRDD<Put> putJavaRdd = partnoRdd.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
            while (r.hasNext()) {
                Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> line = r.next();
                //salt+work_dt+site_code+level_code+line_code+part_no+platform+data_from
                String baseRowKey = sb.append(line._14()).append(":").append(line._1()).append(":").append(line._2()).append(":").append(line._6()).append(":").append(line._7()).append(":").append(line._9()).append(":").append(line._16()).toString();
                String salt = consistentHashLoadBalance.selectNode(baseRowKey);
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(line._1()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("level_code"), Bytes.toBytes(line._2()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("factory_code"), Bytes.toBytes(line._3()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("process_code"), Bytes.toBytes(line._4()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("area_code"), Bytes.toBytes(line._5()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("line_code"), Bytes.toBytes(line._6()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("part_no"), Bytes.toBytes(line._7()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("platform"), Bytes.toBytes(line._8()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("work_dt"), Bytes.toBytes(line._9()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("work_shift"), Bytes.toBytes(line._10()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("smt_ttl_pass"), Bytes.toBytes(line._11()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("output_qty"), Bytes.toBytes(line._12()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("ct"), Bytes.toBytes(line._13()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("data_granularity"), Bytes.toBytes("level"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_dt"), Bytes.toBytes(line._14()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_by"), Bytes.toBytes(line._15()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("data_from"), Bytes.toBytes(line._16()));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        putJavaRdd.take(5).forEach(r -> System.out.println(r));
//        dpm_dwd_production_partno_day 写入 dpm_dws_production_partno_day
        DPHbase.rddWrite("dpm_dws_production_partno_dd", putJavaRdd);

        System.out.println("==============================>>>CQ Programe End<<<==============================");
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }

    public static String notNull(String str) {
        if (StringUtils.isEmpty(str) || "null".equals(str)) {
            return "";
        }
        return str;
    }

    public static List<String> lineCode() {
        List lineList = new ArrayList(32);
        lineList.add("D01A,SMTD01A");
        lineList.add("D01A,PTHD01A");
        lineList.add("D01B,SMTD01B");
        lineList.add("D01B,PTHD01B");
        lineList.add("D02,SMTD02A");
        lineList.add("D02,SMTD02B");
        lineList.add("D02,PTHD02");
        lineList.add("D03,SMTD03A");
        lineList.add("D03,SMTD03B");
        lineList.add("D03,PTHD03");
        lineList.add("D04,SMTD04A");
        lineList.add("D04,SMTD04B");
        lineList.add("D04,PTHD04");
        lineList.add("D05,SMTD05A");
        lineList.add("D05,SMTD05B");
        lineList.add("D05,PTHD05");
        lineList.add("D06A,SMTD06A");
        lineList.add("D06A,PTHD06A");
        lineList.add("D06B,SMTD06B");
        lineList.add("D06B,PTHD06B");
        lineList.add("D07,SMTD07A");
        lineList.add("D07,SMTD07B");
        lineList.add("D07,PTHD07");
        lineList.add("D08,SMTD08A");
        lineList.add("D08,SMTD08B");
        lineList.add("D08,PTHD08");
        lineList.add("D09,SMTD09A");
        lineList.add("D09,SMTD09B");
        lineList.add("D09,PTHD09");

        lineList.add("D010A,SMTD010A");
        lineList.add("D010A,PTHD010A");
        lineList.add("D010B,SMTD010B");
        lineList.add("D010B,PTHD010B");

        lineList.add("D10A,SMTD10A");
        lineList.add("D10A,PTHD10A");
        lineList.add("D10B,SMTD10B");
        lineList.add("D10B,PTHD10B");

        lineList.add("D11,SMTD11A");
        lineList.add("D11,SMTD11B");
        lineList.add("D11,PTHD11");
        lineList.add("P31,J6S31T");
        lineList.add("P31,J6P31");
        lineList.add("P31,J6P41");
        lineList.add("P32,J6S32T");
        lineList.add("P32,J6P32");
        lineList.add("P32,J6P42");
        lineList.add("P33,J6S33T");
        lineList.add("P33,J6P33");
        lineList.add("P33,J6P43");
        lineList.add("P34,J6S34T");
        lineList.add("P34,J6P34");
        lineList.add("P35,J6S35T");
        lineList.add("P35,J6P35");
        lineList.add("P36,J6S36T");
        lineList.add("P36,J6P36");
        lineList.add("P37,J6S37T");
        lineList.add("P37,J6P37");
        lineList.add("P38,J6S38T");
        lineList.add("P38,J6S39T");
        lineList.add("P38,J6P38");
        lineList.add("P38,J6P48");
        lineList.add("P3A,J6S3BT");
        lineList.add("P3A,J6S3AT");
        lineList.add("P3A,J6P3A");
        lineList.add("P3A,J6P4A");
        lineList.add("P3C,J6S3CT");
        lineList.add("P3C,J6P3C");

        lineList.add("P3D,J6S3DT");
        lineList.add("P3D,J6P3D");

        lineList.add("P3F,J6S3FT");
        lineList.add("P3F,J6S3GT");
        lineList.add("P3F,J6P3F");
        lineList.add("P3F,J6P4F");

        lineList.add("P3J,J6S3HT");
        lineList.add("P3J,J6P3J");
        lineList.add("P3H,J6S3JT");
        lineList.add("P3H,J6P3H");

        lineList.add("P21,J6S21T");
        lineList.add("P21,J6P21");

        lineList.add("P24,J6S24T");
        lineList.add("P24,J6P24");
        lineList.add("P25,J6S25T");
        lineList.add("P25,J6P25");

        lineList.add("P26,J6S26T");
        lineList.add("P26,J6P26");

        lineList.add("J6P51,J6P51");
        return lineList;
    }

    public static List<String> lineCodeCQ() {
        List lineList = new ArrayList(32);
        //S04= CS04= D1S04
        lineList.add("D1S01,CS01");lineList.add("D1S01,S01");
        lineList.add("D1S02,CS02");lineList.add("D1S02,S02");
        lineList.add("D1S03,CS03");lineList.add("D1S03,S03");
        lineList.add("D1S04,CS04");lineList.add("D1S04,S04");
        lineList.add("D1S05,CS05");lineList.add("D1S05,S05");
        lineList.add("D1S06,CS06");lineList.add("D1S06,S06");
        lineList.add("D1S07,CS07");lineList.add("D1S07,S07");
        lineList.add("D1S08,CS08");lineList.add("D1S08,S08");
        lineList.add("D1S09,CS09");lineList.add("D1S09,S09");
        lineList.add("D1S10,CS10");lineList.add("D1S10,S10");
        lineList.add("D1S11,CS11");lineList.add("D1S11,S11");
        lineList.add("D1S12,CS12");lineList.add("D1S12,S12");
        lineList.add("D1S13,CS13");lineList.add("D1S13,S13");
        lineList.add("D1S14,CS14");lineList.add("D1S14,S14");
        lineList.add("D1S15,CS15");lineList.add("D1S15,S15");
        lineList.add("D1S16,CS16");lineList.add("D1S16,S16");
        lineList.add("D1S17,CS17");lineList.add("D1S17,S17");
        lineList.add("D1S18,CS18");lineList.add("D1S18,S18");
        lineList.add("D1S19,CS19");lineList.add("D1S19,S19");
        lineList.add("D1S20,CS20");lineList.add("D1S20,S20");
        lineList.add("D1S21,CS21");lineList.add("D1S21,S21");
        lineList.add("D1S22,CS22");lineList.add("D1S22,S22");
        lineList.add("D1S23,CS23");lineList.add("D1S23,S23");
        lineList.add("D1S24,CS24");lineList.add("D1S24,S24");
        lineList.add("D1S25,CS25");lineList.add("D1S25,S25");
        lineList.add("D1S26,CS26");lineList.add("D1S26,S26");
        return lineList;
    }

    @Test
    public void test() {
        List<String> list = lineCode();
        String[] split = list.get(1).split(",");
        System.out.println(split[1]);
    }

}
