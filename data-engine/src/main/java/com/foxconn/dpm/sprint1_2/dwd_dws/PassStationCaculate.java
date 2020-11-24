package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.PassStation;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @version 1.0
 * @program: ehr->DailyRepairCaculate
 * @description: DWD層dpm_dwd_production_output到DWS層dpm_dws_production_pass_station_dd
 * output_day表中station_code（PRETEST,POST RUNIN，Testing）失敗count和total count
 * @author: Axin
 * @create: 2020-01-15 21:34
 **/
public class PassStationCaculate extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //初始化环境

        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;
        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1, "-");
            today = batchGetter.getStDateDayAdd(0, "-");
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
            todayStamp = batchGetter.getStDateDayStampAdd(1);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }

        final String family = "DPM_DWD_PRODUCTION_OUTPUT";
        String finalYesterday = yesterday;
        JavaRDD<PassStation> productionRDD = DPHbase.saltRddRead("dpm_dwd_production_output", yesterdayStamp, todayStamp, new Scan(), true).map((r) -> {
            return batchGetter.resultGetColumns(r, "DPM_DWD_PRODUCTION_OUTPUT",
                    "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "part_no", "sku", "platform", "customer", "wo", "workorder_type", "work_dt", "work_shift", "sn", "station_code", "station_name", "is_fail", "scan_by", "scan_dt", "output_qty", "update_dt", "update_by", "data_from"
            );
        })/*.filter(r -> "L10".equals(r.get(1)) && finalYesterday.equals(r.get(13)) && !"".equals(r.get(14)))*/
                .keyBy(r -> {
                    //work_dt site_code level_code  sn station_code
                    return r.get(13).concat(r.get(0)).concat(r.get(1)).concat(r.get(15)).concat(r.get(16));
                }).reduceByKey((rv1, rv2) -> {
                    //使用最后更新时间进行去重
                    return Long.valueOf(rv1.get(22)) >= Long.valueOf(rv2.get(22)) ? rv1 : rv2;
                }).filter(r -> {
                    return r != null && r._2 != null;
                }).map(t -> {
                    return t._2;
                })/*.keyBy((r) -> {
            return batchGetter.getStrArrayOrg("=", "",
                    //"site_code","level_code","factory_code","process_code","area_code","line_code","machine_id","part_no","sku","platform","customer",
                    // "wo","workorder_type","work_dt","work_shift","sn","station_code","station_name","is_fail","scan_by","scan_dt","output_qty","update_dt",
                    // "update_by","data_from"
                    //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","workorder_type","work_dt","customer"
                    r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(7), r.get(8), r.get(9), r.get(12), r.get(13), r.get(10), r.get(14)
            );
        }).map(t -> {
            //"site_code","level_code","factory_code","process_code","area_code","line_code","machine_id","part_no","sku","platform","customer",
            // "wo","workorder_type","work_dt","work_shift","sn","station_code","station_name","is_fail","scan_by","scan_dt","output_qty","update_dt",
            // "update_by","data_from"

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


            ArrayList<String> r = t._2;
            String workDt = r.get(13);
            String scanDt = simpleDateFormat.format(new Date(Long.valueOf(r.get(20))));

            //今天零点到12点的晚班
            String todayDateZero = workDt + " 00:00:00.000";
            //昨天凌晨晚班结束也就是今天12点以前
            String todayDateEnd = workDt + " 12:00:00.000";
            //如果扫描时间大于等于今天零点并且小于等于今天凌晨6点并且班别为晚班的则为昨天的产量
            if (batchGetter.dateStrCompare(scanDt, todayDateZero, "yyyy-MM-dd HH:mm:ss.SSS", ">=") && batchGetter.dateStrCompare(scanDt, todayDateEnd, "yyyy-MM-dd HH:mm:ss.SSS", "<=") && r.get(14).equals("N")) {
                //工作日期要算昨天的
                r.set(13, batchGetter.getStDateDayStrAdd(workDt, -1, "-"));
                return new Tuple2<String, ArrayList<String>>(batchGetter.getStrArrayOrg("=", "",
                        //"site_code","level_code","factory_code","process_code","area_code","line_code","machine_id","part_no","sku","platform","customer",
                        // "wo","workorder_type","work_dt","work_shift","sn","station_code","station_name","is_fail","scan_by","scan_dt","output_qty","update_dt",
                        // "update_by","data_from"
                        //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","workorder_type","work_dt","customer"
                        r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(7), r.get(8), r.get(9), r.get(12), r.get(13), r.get(10), r.get(14)
                ), r);
            } else {
                //如果不是凌晨扫描的则不需要修正
                return t;
            }
        }).map(t -> {
            return t._2;
        })*/.map(r -> {
                    /**
                     *  site_code;
                     * level_code;
                     * factory_code;
                     * process_code;
                     * area_code;
                     * line_code;
                     * work_dt;
                     * work_shift;
                     * sku;
                     * part_no;
                     * plantform;
                     * station_code;
                     * station_name;
                     * customer;
                     * is_fail
                     */

                    return new PassStation(
                            r.get(0),//site_code
                            r.get(1),//level_code
                            r.get(2),//factory_code
                            r.get(3),//process_code
                            r.get(4),//area_code
                            r.get(5),//line_code
                            r.get(13),//work_dt
                            r.get(14),//work_shift
                            r.get(8),//sku
                            r.get(7),//part_no
                            r.get(9),//platform
                            r.get(16),//station_code
                            r.get(17),//station_name
                            r.get(10),//customer
                            r.get(18)//is_fail
                    );
                });

        System.out.println(productionRDD.filter(r->{return "PRETEST".equals(r.getStation_code()) && "HP".equals(r.getCustomer()) && "WH".equals(r.getSite_code()) && "L10".equals(r.getLevel_code());}).count());
        System.out.println(productionRDD.filter(r->{return "POST RUNIN".equals(r.getStation_code()) && "HP".equals(r.getCustomer()) && "WH".equals(r.getSite_code()) && "L10".equals(r.getLevel_code());}).count());
        System.out.println(productionRDD.filter(r->{return "Testing".equals(r.getStation_code()) && "LENOVO".equals(r.getCustomer()) && "WH".equals(r.getSite_code()) && "L10".equals(r.getLevel_code());}).count());
        SparkSession session = DPSparkApp.getSession();
        session.createDataFrame(productionRDD, PassStation.class).createOrReplaceTempView("passStation");
        Dataset<Row> ds = session.sql(sqlGetter.Get("pass_station.sql"));

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            for (Row row : ds.javaRDD().collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        String updateTime = String.valueOf(System.currentTimeMillis());
        JavaRDD<Put> toWriteData = ds.toJavaRDD().mapPartitions(iterator -> {
            List<Put> datas = new ArrayList<>();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
            SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
            final String toWriteFamily = "DPM_DWS_PRODUCTION_PASS_STATION_DD";
            iterator.forEachRemaining(row -> {
                String site_code = row.getString(0);
                String level_code = row.getString(1);
                String factory_code = row.getString(2);
                String process_code = row.getString(3);
                String area_code = row.getString(4);
                String line_code = row.getString(5);
                String sku = row.getString(6);
                String plantform = row.getString(7);
                String customer = row.getString(8);
                String work_dt = row.getString(9);
                String work_shift = row.getString(10);
                String part_no = row.getString(11);
                String station_code = row.getString(12);
                String station_name = row.getString(13);
                String total_count = String.valueOf(row.getLong(14));
                String fail_count = String.valueOf(row.getLong(15));
                try {
                    String rowkey = formatWorkDt.parse(work_dt).getTime() + ":" + site_code + ":" + level_code + ":" + line_code + ":" + UUID.randomUUID().toString();
                    rowkey = consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
                    String data_granularity = "level";
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("site_code"), Bytes.toBytes(site_code));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("level_code"), Bytes.toBytes(level_code));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("factory_code"), Bytes.toBytes(factory_code));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("process_code"), Bytes.toBytes(process_code));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("area_code"), Bytes.toBytes(area_code));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("line_code"), Bytes.toBytes(line_code));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("work_dt"), Bytes.toBytes(work_dt));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("work_shift"), Bytes.toBytes(work_shift == null ? "" : work_shift));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("sku"), Bytes.toBytes(sku));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("part_no"), Bytes.toBytes(part_no));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("platform"), Bytes.toBytes(plantform));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("station_code"), Bytes.toBytes(station_code));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("station_name"), Bytes.toBytes(station_name));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("fail_count"), Bytes.toBytes(fail_count));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("total_count"), Bytes.toBytes(total_count));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("customer"), Bytes.toBytes(customer));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("data_granularity"), Bytes.toBytes(data_granularity));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("update_dt"), Bytes.toBytes(updateTime));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("update_by"), Bytes.toBytes("Axin"));
                    put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("data_from"), Bytes.toBytes("dpm_dwd_production_output_day"));
                    datas.add(put);
                } catch (Exception e) {
                    //ignore
                    System.out.println("数据异常,必要参数缺失");
                }
            });
            return datas.iterator();
        });

        try {
            System.out.println(toWriteData.count());
            List<Put> take15 = toWriteData.take(5);
            for (Put put : take15) {
                System.out.println(put);
            }
        } catch (Exception e) {
        }
        //   Dwd写入Dws
        DPHbase.rddWrite("dpm_dws_production_pass_station_dd", toWriteData);
        System.out.println("==============================>>>Programe End<<<==============================");
        DPSparkApp.stop();
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
