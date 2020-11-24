package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.comparator.AscIntegerComparator;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className DayLineProductionWorkTime
 * @description TODO
 * @date 2020/1/11 13:40
 */
public class DayLineProductionWorkTime extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    String START_TIME_RANGE = "";
    String END_TIME_RANGE = "";


    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        START_TIME_RANGE = (String) map.get("START_TIME_RANGE");
        END_TIME_RANGE = (String) map.get("END_TIME_RANGE");

        if (START_TIME_RANGE == null || END_TIME_RANGE == null || "".equals(START_TIME_RANGE) || "".equals(END_TIME_RANGE)) {
            calculateDayLineRealWorkTime(batchGetter.getStDateDayStampAdd(-1), batchGetter.getStDateDayStampAdd(1));
        } else {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            calculateDayLineRealWorkTime(String.valueOf(simpleDateFormat.parse(START_TIME_RANGE).getTime()-10000), String.valueOf(simpleDateFormat.parse(END_TIME_RANGE).getTime()+10000));
        }

    }
    /* ********************************************************************************************
     * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRIBE             <<<<<<<<<<<<<<<<<<<<<<<<<<<
     * ********************************************************************************************
     * sourceData=>
     *              取昨天和今天的数据，同key行使用update_dt进行reduce
     * sourceColumns=>
     *              L5 L6 L10
     *              "site_code","level_code","plant_code","process_code","area_code","line_code","machine_id","part_no","sku","
     *              plantform","customer","wo","workorder_type","work_dt","work_shift","sn","station_code","station_name","is_fail",
     *              "scan_by","scan_dt","output_qty","update_dt","update_by","data_from"
     * targetData=>
     *              存储每条线每天的真实生产时间
     * targetColumns=>
     *              "site_code","level_code","factory_code","process_code","area_code","line_code","work_dt",
     *              "work_shift","work_time","update_dt","update_by","data_from"
     *
     * Task=>
     *          计算策略：
     *              1.
     *                  1).最后一条过站时间减去第一条过站时间得到该线到目前为止总时长
     *                  2).计算该区间所有数据中数据之间间隔时间超过10分钟的
     *                  3).总的时间 - 该段时间 = 剩余的时间
     *              2.
     *                  1).先排序然后逐条计算该段时间所有的时间间隔得到更准确的工时
     *                  2).计算期间忽略掉两条时间之间间隔10分钟以上的间隔数据
     *          计算选择：默认选择策略2
     *          按照-某天-某线-的所有过站信息-排序，
     *          运算标准=>毫秒值计算
     *          存储标准=>每次取update_dt最新的一条数据
     *          空闲标准=>10分钟间隔则为空闲 大于10分钟
     *
     *          排序策略：
     *             1.时间区间确定=>site_code,level_code,factory_code,process_code,area_code,line_code,work_dt
     *             2.一天数据量最高为500w级=》每小时区间为20 8333.3333333333
     *             3.数据集中在正常生产时间段08 - 20
     *             4.按照生产时间特定分区
     *                  使用Calendar获取小时：区间结束为 < 区间开始为 >=  分区数为9
     *                  00-04
     *                  05-08
     *                  08-10
     *                  10-12
     *                  12-14
     *                  14-16
     *                  16-20
     *                  20-22
     *                  22-00
     *             5.分区内排序后返回该分区内去除休息时间工时统计
     *             6.对各个分区的工时进行二次汇总（此时Collect数据到Driver）
     * ExternalLogic;
     *          1.添加班别
     *                                                                             **   **
     *
     *                                                                           ************
     ********************************************************************************************** */

    public void calculateDayLineRealWorkTime(String startTime, String endTime) throws Exception {

        System.out.println(batchGetter.getStDateDayStampAdd(-1));
        System.out.println(batchGetter.getStDateDayStampAdd(1));


        JavaRDD<Result> day_dop_rdd = DPHbase.saltRddRead("dpm_dwd_production_output", startTime, endTime, new Scan(), true);
        JavaPairRDD<String, ArrayList<String>> checkedDataRDD = day_dop_rdd.filter((r) -> {
            return batchGetter.checkColumns(r, "DPM_DWD_PRODUCTION_OUTPUT",
                    "site_code", "level_code", "work_dt", "sn", "station_code", "is_fail", "scan_dt", "update_dt", "data_from");
        }).map((r) -> {
            return batchGetter.resultGetColumns(r, "DPM_DWD_PRODUCTION_OUTPUT",
                    "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "part_no", "sku", "platform", "customer", "wo", "workorder_type", "work_dt", "work_shift", "sn", "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"
            );
        }).filter(r -> {
            switch (r.get(0).concat(r.get(1))) {
                case "WHL5":
                case "WHL6":
                case "WHL10":
                case "CQL5":
                case "CQL6":
                case "CQL10":
                    return "ODS".equals(r.get(24)) ? true : false;
                default:
                    return false;
            }
        }).keyBy(r -> {
            return r.get(15) + "-" + r.get(20);
        }).reduceByKey((rv1, rv2) -> {
            try {
                if (rv1.get(22).matches("\\d+") && !rv2.get(22).matches("\\d+")) {
                    return rv1;
                }
                if (!rv1.get(22).matches("\\d+") && rv2.get(22).matches("\\d+")) {
                    return rv2;
                }
                //使用最后更新时间进行去重
                return Long.valueOf(rv1.get(22)) >= Long.valueOf(rv2.get(22)) ? rv1 : rv2;
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r._2 != null;
        }).map(t -> {
            return t._2;
        })/*.map(r -> {
            //"site_code", "level_code", "plant_code", "process_code", "area_code", "line_code", "machine_id",
            // "part_no", "sku", "plantform", "customer", "wo", "workorder_type", "work_dt", "work_shifit", "sn",
            // "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"


            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            //work_dt
            String workDt = r.get(13);

            //此处修正的workdt，因为需要使用scandt统计当工作日的实际开线时间
            //scan_dt
            Date scan_date = new Date(Long.valueOf(r.get(20)));
            String scanDt = simpleDateFormat.format(scan_date);

            //今天零点到12点的晚班
            String todayDateZero = workDt + " 00:00:00.000";
            //昨天凌晨晚班结束也就是今天12点以前
            String todayDateEnd = workDt + " 12:00:00.000";
            //如果扫描时间大于等于今天零点并且小于等于今天凌晨6点并且班别为晚班的则为昨天的产量
            if (batchGetter.dateStrCompare(scanDt, todayDateZero, "yyyy-MM-dd HH:mm:ss.SSS", ">=") && batchGetter.dateStrCompare(scanDt, todayDateEnd, "yyyy-MM-dd HH:mm:ss.SSS", "<=") && r.get(14).equals("N")) {
                //工作日期要算昨天的
                //work_dt
                r.set(13, batchGetter.getStDateDayStrAdd(workDt, -1, "-"));
                //此处不需要修正scandt 只需要修正工作日即可
            } else {
                //如果不是凌晨扫描的则不需要修正
            }
            return r;
        }).filter(r -> {
            return !r.get(13).contains(batchGetter.getStDateDayAdd(-2, "-"));
        })*/.keyBy(r -> {
            return batchGetter.getStrArrayOrg("=", "-",
                    //"site_code","level_code","plant_code","process_code","area_code","line_code","machine_id","part_no","sku","plantform","customer",
                    // "wo","workorder_type","work_dt","work_shifit","sn","station_code","station_name","is_fail","scan_by","scan_dt","output_qty","update_dt",
                    // "update_by","data_from"
                    // "site_code","level_code","factory_code","process_code","area_code","line_code","work_dt","work_shifit", customer
                    //此处work_dt为工作日，自然日  注意：此处用的时间为真实工作时间
                    r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(13), r.get(14), r.get(10)
            );
        }).persist(StorageLevel.MEMORY_AND_DISK());


        try {
            List<Tuple2<String, ArrayList<String>>> collect = checkedDataRDD/*.filter(t -> {
                return "L10".equals(t._2.get(1)) && "D".equals(t._2.get(14)) && Integer.valueOf(t._2.get(20).split(" ")[1].split(":")[0]) < 3;
            })*/.take(5);
            for (Tuple2<String, ArrayList<String>> stringArrayListTuple2 : collect) {
                System.out.println(stringArrayListTuple2);
            }
            System.out.println("==============================>>>collect End<<<==============================");
        } catch (Exception e) {

        }

        try {


            List<String> take = checkedDataRDD.filter(t -> {
                return "L10".equals(t._2.get(1));
            }).map(t -> {
                return t._2.get(13) + t._2.get(14) + t._2.get(5);
            }).distinct().collect();
            for (String s : take) {
                System.out.println(s);
            }
        } catch (Exception e) {

        }

        System.out.println("==============================>>>checkedDataRDD End<<<==============================");


        JavaRDD<Tuple2<String, Tuple3<Long, Long, Long>>> formatTargetRDD = checkedDataRDD.mapToPair(new PairFunction<Tuple2<String, ArrayList<String>>, String, Tuple2<String, Tuple3<Long, Long, Long>>>() {
            @Override
            public Tuple2<String, Tuple2<String, Tuple3<Long, Long, Long>>> call(Tuple2<String, ArrayList<String>> kv) throws Exception {
                /*
                 * ====================================================================
                 * 描述: build calculate meta struct data
                 *      1.sourceKey=>"site_code","level_code","factory_code","process_code","area_code","line_code","work_dt"
                 *      2.sourceValues=>
                 *          site_code", "level_code", "plant_code", "process_code", "area_code", "line_code", "machine_id",
                 *          "part_no", "sku", "plantform", "customer", "wo", "workorder_type", "work_dt", "work_shifit",
                 *          "sn", "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"
                 *
                 *      3.formatTuple=>hourMark,(key, (now_scan_dt, next_scan_dt, sum_real_time))
                 *
                 * ====================================================================
                 */
                //scan_dt
                Long scanTimeStamp = Long.valueOf(kv._2.get(20));
                int day_hour = Integer.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(scanTimeStamp)).split(" ")[1].split(":")[0]);
                return new Tuple2<String, Tuple2<String, Tuple3<Long, Long, Long>>>(day_hour + "-" + kv._2.get(20), new Tuple2<String, Tuple3<Long, Long, Long>>(kv._1, new Tuple3<Long, Long, Long>(scanTimeStamp, scanTimeStamp, 0L)));
            }
        }).repartitionAndSortWithinPartitions(new Partitioner() {
            /*
             * ====================================================================
             * 描述:
             * 4.按照生产时间特定分区
             *                  使用Calendar获取小时：区间结束为 < 区间开始为 >= 分区数为9
             *                  00-04
             *                  05-08
             *                  08-10
             *                  10-12
             *                  12-14
             *                  14-16
             *                  16-20
             *                  20-22
             *                  22-00
             * ====================================================================
             */
            @Override
            public int getPartition(Object key) {
                Integer hour = Integer.parseInt(((String) key).split("-")[0]);
                //! 此处会把不同的天的同小时放到同一个分区中
                if (hour >= 0 && hour < 4) {
                    return 0;
                } else if (hour >= 5 && hour < 8) {
                    return 1;
                } else if (hour >= 8 && hour < 10) {
                    return 2;
                } else if (hour >= 10 && hour < 12) {
                    return 3;
                } else if (hour >= 12 && hour < 14) {
                    return 4;
                } else if (hour >= 14 && hour < 16) {
                    return 5;
                } else if (hour >= 16 && hour < 20) {
                    return 6;
                } else if (hour >= 20 && hour < 22) {
                    return 7;
                } else if (hour >= 22 && hour < 0) {
                    return 8;
                } else {
                    return 9;
                }
            }

            @Override
            public int numPartitions() {
                return 10;
            }
        }, new AscIntegerComparator()).mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Tuple2<String, Tuple3<Long, Long, Long>>>>, Iterator<Tuple2<String, Tuple2<String, Tuple3<Long, Long, Long>>>>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Tuple3<Long, Long, Long>>>> call(Integer idx, Iterator<Tuple2<String, Tuple2<String, Tuple3<Long, Long, Long>>>> it) throws Exception {
                return idx != 9 ? it : new ArrayList<Tuple2<String, Tuple2<String, Tuple3<Long, Long, Long>>>>().iterator();
            }
        }, true).map(t -> {
            return t._2;
        }).persist(StorageLevel.MEMORY_AND_DISK());
        try {
            List<Tuple2<String, Tuple3<Long, Long, Long>>> take = formatTargetRDD.take(5);
            for (Tuple2<String, Tuple3<Long, Long, Long>> integerTuple2Tuple2 : take) {
                System.out.println(integerTuple2Tuple2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("==============================>>>formatTargetRDD End<<<===============================");


        Map<String, ArrayList<Tuple3<Long, Long, Long>>> hoursRangePreCalculate = formatTargetRDD.mapPartitions(it -> {

            HashMap<String, ArrayList<Tuple2<String, Tuple3<Long, Long, Long>>>> dayHourScan = new HashMap<>();

            while (it.hasNext()) {


                /*
                 * ====================================================================
                 * 描述:
                 * TODO
                 * 此处为危险方法：如果出现内存不够的情况则需要再在此分区的基础上再进行
                 *                  下一级分区。
                 *
                 * ====================================================================
                 */
                Tuple2<String, Tuple3<Long, Long, Long>> next = it.next();

                // "site_code","level_code","factory_code","process_code","area_code","line_code","work_dt","work_shifit" customer
                Long scandt = next._2._1();
                String realDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date(scandt));
                String[] splitKey = next._1.split("=");
                splitKey[6] = realDate;
                if ("-".equals(splitKey[5]) || " ".equals(splitKey[5])) {
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < splitKey.length; i++) {
                    sb.append(splitKey[i]);
                }
                String realWorkDt = sb.toString();


                ArrayList<Tuple2<String, Tuple3<Long, Long, Long>>> array = dayHourScan.get(realWorkDt);
                if (array != null) {
                    array.add(next);
                } else {
                    ArrayList<Tuple2<String, Tuple3<Long, Long, Long>>> one_day_data = new ArrayList<>();
                    one_day_data.add(next);
                    dayHourScan.put(realWorkDt, one_day_data);
                }
            }

            ArrayList<Tuple2<String, Tuple3<Long, Long, Long>>> resultScan = new ArrayList<>();

            for (String k : dayHourScan.keySet()) {
                ArrayList<Tuple2<String, Tuple3<Long, Long, Long>>> hoursScan = dayHourScan.get(k);

                //结束key赋值
                String orgKey = hoursScan.get(0)._1;

                //此处排除无数据和一条数据的情况，一条数据则真实工时为默认0，留到全局聚合的时候进行处理
                if (hoursScan.size() <= 1) {
                    hoursScan.set(0, new Tuple2<String, Tuple3<Long, Long, Long>>(orgKey, hoursScan.get(0)._2));
                    resultScan.addAll(hoursScan);
                } else {

                    //首先排除一条的情况


                    /*
                     * ====================================================================
                     * 描述:
                     *      1.遍历该降序集中的数据：
                     *              0号数据为该区间第一条
                     *              最后一条数据为该区间最后一条
                     *      2.逐条前后计算partition总时长
                     *      3.如果遇到间隔时常大于10分钟的则直接pass
                     * ====================================================================
                     */


                    //区间开始和结束时间
                    Long startHours = hoursScan.get(0)._2._1();
                    Long endtHours = hoursScan.get(hoursScan.size() - 1)._2._1();
                    //区间内真实工时求和
                    Long hoursRangeWorkSum = 0L;
                    //此处减一是为了避免最后一条数据
                    for (int i = 0; i < hoursScan.size() - 1; i++) {
                        //获取当前扫描时间戳和下次扫描时间戳
                        Long nowScanDtTimestamp = hoursScan.get(i)._2._1();
                        Long nextScanDtTimestamp = hoursScan.get(i + 1)._2._1();

                        //因为时间戳间隔是按照毫秒计算的所以最后需要乘以一千
                        long len = nextScanDtTimestamp - nowScanDtTimestamp;
                        if (len <= 60 * 10 * 1000) {
                            hoursRangeWorkSum += len;
                        }
                    }
                    resultScan.add(new Tuple2<String, Tuple3<Long, Long, Long>>(orgKey, new Tuple3<Long, Long, Long>(startHours, endtHours, hoursRangeWorkSum)));
                }
            }
            return resultScan.listIterator();
        }, true).mapToPair(new PairFunction<Tuple2<String, Tuple3<Long, Long, Long>>, String, ArrayList<Tuple3<Long, Long, Long>>>() {
            @Override
            public Tuple2<String, ArrayList<Tuple3<Long, Long, Long>>> call(Tuple2<String, Tuple3<Long, Long, Long>> t) throws Exception {
                ArrayList<Tuple3<Long, Long, Long>> arrayList = new ArrayList<>();
                arrayList.add(t._2);
                return new Tuple2<String, ArrayList<Tuple3<Long, Long, Long>>>(t._1, arrayList);
            }
        }).filter(r->{
            return r._2.size() > 0;
        }).reduceByKey((tv1, tv2) -> {
            tv1.addAll(tv2);
            return tv1;
        }).collectAsMap();


        try {
            for (Map.Entry<String, ArrayList<Tuple3<Long, Long, Long>>> stringArrayListEntry : hoursRangePreCalculate.entrySet()) {
                if (!stringArrayListEntry.getKey().contains("L10")) {
                    continue;
                }
                System.out.println(stringArrayListEntry.getKey() + "-" + stringArrayListEntry.getValue());
            }
        } catch (Exception e) {

        }

        System.out.println("==============================>>>hoursRangePreCalculate End<<<==============================");

        ArrayList<Tuple2<String, Long>> hoursRangeAllCalculate = new ArrayList<Tuple2<String, Long>>();
        //final Map<String, Tuple3<Long, Long, Long>> hoursRangePreCalculate = hoursRangePreCalculate;

        for (String org : hoursRangePreCalculate.keySet()) {

            ArrayList<Tuple3<Long, Long, Long>> orgKeyScanDTs = new ArrayList<>();
            for (Tuple3<Long, Long, Long> t : hoursRangePreCalculate.get(org)) {
                orgKeyScanDTs.add(t);
            }


            //排除区间只有一条的情况和没得的情况
            if (orgKeyScanDTs.size() <= 1) {
                if (orgKeyScanDTs.get(0)._3() < 0) {
                    System.out.println("Single==============>>>>>>>>>>>ERR  ORG =====" + org + "-" + orgKeyScanDTs.get(0)._1() + "_" + orgKeyScanDTs.get(0)._2());
                }
                hoursRangeAllCalculate.add(new Tuple2<String, Long>(org, orgKeyScanDTs.get(0)._3()));
                continue;
            }

            orgKeyScanDTs.sort(new Comparator<Tuple3<Long, Long, Long>>() {
                @Override
                public int compare(Tuple3<Long, Long, Long> t1, Tuple3<Long, Long, Long> t2) {
                    /*a negative integer, zero, or a positive
                     *          integer as the first argument is less than
                     *         ,equal to, or greater than the
                     *         second.*/

                    //升序 由于区间不存在交叉所以可以直接按照起始时间排序。
                    return t1._2() < t2._2() ? -1 : 1;
                }
            });

            System.out.println(orgKeyScanDTs.toString());

            //区间内真实工时求和
            Long hoursRangeWorkSum = 0L;
            //获取当前扫描时间戳和下次扫描时间戳
            for (int i = 0; i < orgKeyScanDTs.size() - 1; i++) {
                //获取开始条的结束时刻
                //获取下一条的开始时刻
                Long nowScanDtTimestamp = orgKeyScanDTs.get(i)._2();
                Long nextScanDtTimestamp = orgKeyScanDTs.get(i + 1)._1();
                //因为时间戳间隔是按照毫秒计算的所以最后需要乘以一千
                long len = nextScanDtTimestamp - nowScanDtTimestamp;

                if (len < 0) {
                    System.out.println("Multi==============>>>>>>>>>>>ERR  ORG =====" + org + "_" + orgKeyScanDTs.get(i) + "_" + orgKeyScanDTs.get(i + 1));
                }

                if (len <= 60 * 10 * 1000) {
                    hoursRangeWorkSum += len;
                }
                if (i == orgKeyScanDTs.size() - 2) {
                    //加上该区间段预聚合的时常
                    hoursRangeWorkSum += orgKeyScanDTs.get(i)._3();
                    //最后一条需要额外加上最后一个区间的
                    hoursRangeWorkSum += orgKeyScanDTs.get(i + 1)._3();
                } else {
                    //加上该区间段预聚合的时常
                    hoursRangeWorkSum += orgKeyScanDTs.get(i)._3();
                }
            }
            hoursRangeAllCalculate.add(new Tuple2<String, Long>(org, hoursRangeWorkSum));
        }

        for (Tuple2<String, Long> stringLongTuple2 : hoursRangeAllCalculate) {
            if (!stringLongTuple2._1.contains("L10")) {
                continue;
            }
            System.out.println(stringLongTuple2);
        }

        System.out.println("==============================>>>hoursRangeAllCalculate End<<<==============================");

        //"site_code","level_code","factory_code","process_code","area_code","line_code","work_dt"
        JavaRDD<ArrayList<String>> formatedRDD = DPSparkApp.getContext().parallelize(hoursRangeAllCalculate).map(t -> {
            //final Tuple2<String, Long> t1 = t;

            /*
             * ====================================================================
             * 描述:
             *
             * - Rowkey=String
             * - site_cod=String
             * - level_code=String
             * - factory_code=String
             * - process_code=String
             * - area_code=String
             * - line_code=String
             * - work_dt=String
             * - work_shift=String
             * - work_time=Long
             * - update_dt=Long
             * - update_by=String
             * - data_from=String
             * ====================================================================
             */
            //(WH,L5,DT1,-,-,F3,2020-04-17,D,959000)
            String[] orgs = t._1.split("=");
            ArrayList<String> row = new ArrayList<>();

            StringBuilder sb = new StringBuilder();
            String workDate = orgs[6];
            Date parse = new SimpleDateFormat("yyyy-MM-dd").parse(workDate);
            String baseRowKey = sb.append(String.valueOf(parse.getTime())).append(":").append(orgs[0]).append(":").append(orgs[1]).append(":").append(orgs[5]).append(":").append(orgs[7]).append(":").append(UUID.randomUUID().toString().replace("-", "")).toString();
            String rowkey = new ConsistentHashLoadBalance(20).selectNode(baseRowKey) + ":" + baseRowKey;

            row.add(rowkey);

            for (int i = 0; i < orgs.length; i++) {
                row.add(orgs[i]);
            }
            //work_time
            row.add(String.valueOf(t._2));
            row.add(String.valueOf(System.currentTimeMillis()));
            row.add("HS");
            row.add("DWD");
            return row;
        });


//        try {
//            Tuple2<String, Long> stringLongTuple2 = formatedRDD.filter(r -> {
//                return "L10".equals(r.get(2))
//                        &&
//                        batchGetter.getStDateDayAdd(-1, "-").equals(r.get(7))
//                        ;
//            }).map(r -> {
//                return Long.valueOf(r.get(9));
//            }).keyBy(r -> {
//                return "1";
//            }).reduceByKey((kv1, kv2) -> {
//                return kv1 + kv2;
//            }).collect().get(0);
//            System.out.println(stringLongTuple2);
//            for (ArrayList<String> strings : formatedRDD.take(5)) {
//                System.out.println(strings);
//            }
//        } catch (Exception e) {
//
//        }

        System.out.println("==============================>>>QA Log Start<<<==============================");
        List<ArrayList<String>> collect = formatedRDD.collect();
        for (ArrayList<String> strings : collect) {
            System.out.println(strings.toString());
        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        JavaRDD<Put> dayLineRealWorkTime = formatedRDD.mapPartitions(it -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Put> puts = new ArrayList<>();
            while (it.hasNext()) {
                Put put = beanGetter.getPut("dpm_dws_production_line_info_dd", "DPM_DWS_PRODUCTION_LINE_INFO_DD", it.next().toArray(new String[0]));
                if (put != null) {
                    puts.add(put);
                }
            }
            return puts.iterator();
        });


        try {

            DPHbase.rddWrite("dpm_dws_production_line_info_dd", dayLineRealWorkTime);
        } catch (Exception e) {
            System.out.println("==============================>>>>>Write Data Err<<<<<<<<<==============================");
        }


        System.out.println("==============================>>>Programe End<<<==============================");
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
