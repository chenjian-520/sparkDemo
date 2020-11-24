package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.util.StringUtils;
import scala.Tuple12;
import scala.Tuple14;
import scala.Tuple22;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * @author wxj
 * @date 2020/1/14 9:49
 * L6FPY 一次良率
 * 输入表： 抽取dwd表 dpm_dwd_production_output (线体或者机台的单日产量 事实数据 标准化表 高表 每日数据镜像,保留一天的真实数据)
 * 输出表：放入dws表 dpm_dws_production_pass_station_dd 日partno Line工站過站表 (L6 FPY因子數據  L10 FPY因子數據)
 * 描述：抽取的L6对应工站的数据，没有全表抽取 对应工站为：FCT REPAIR,ICT REPAIR,R_S_VI_T,AOI INSPECT,SMT INSPECT21,SMT INSPECT1,S_VI_T,P_ICT1,ICT_*,P_ICT,FT-*
 */
public class FtyDwdToDwsSix extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    public static boolean strLength(String str, int length, String station) {
        if (!StringUtils.isEmpty(str)) {
            if (str.length() >= length) {
                if (station.equals(str.substring(0, length))) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String emptyStrNull(String s) {
        if (StringUtils.isEmpty(s)) {
            return "";
        }
        return s;
    }

    //station_code为S開頭帶四位數字的删选
    public static boolean match(String r) {
        return StringUtils.isEmpty(r) ? false : r.matches("[S][0-9]{4}");
    }

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //业务逻辑
        fty();
        //释放资源
        DPSparkApp.stop();
    }

    private void fty() throws Exception {
        //初始化环境
        //初始化时间
        String yesterday = batchGetter.getStDateDayAdd(-1, "-");
        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
        String todayStamp = batchGetter.getStDateDayStampAdd(0);

        System.out.println(yesterday);
        System.out.println(yesterdayStamp);
        System.out.println(todayStamp);
        System.out.println("-----------------------------------");

        System.out.println("==============================>>>Programe Start<<<==============================");
        JavaRDD<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                filter = DPHbase.saltRddRead("dpm_dwd_production_output", yesterdayStamp, todayStamp, new Scan(), true).map(r -> {
            return new Tuple22<>(
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("site_code")))),//1
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("level_code")))),//2
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("factory_code")))),//3
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("process_code")))),//4
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("area_code")))),//5
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("line_code")))),//6
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("machine_id")))),//7
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("part_no")))),//8
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("platform")))),//9
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("workorder_type")))),//10
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_dt")))),//11
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_shift")))),//12
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sn")))),//13
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_code")))),//14
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_name")))),//15
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("is_fail")))),//16
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_dt")))),//17
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("output_qty")))),//18
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("update_dt")))),//19
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("customer")))),//20
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("data_from")))),//21
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sku"))))//22
            );
        }).filter(r -> r._11().equals(yesterday) && r._2().equals("L6") && r._1().equals("WH") &&
                ("FCT REPAIR".equals(r._14()) || "ICT REPAIR".equals(r._14()) || "R_S_VI_T".equals(r._14())
                        //SMT计入良率工站
                        || r._14().startsWith("AOI INSPECT") || r._14().startsWith("SMT INSPECT") || "S_VI_T".equals(r._14())
                        //ICT计入良率工站
                        || r._14().startsWith("ICT_") || r._14().startsWith("P_ICT")
                        //FT计入良率工站
                        || r._14().startsWith("FT-") || r._14().startsWith("FBT")
                )
        ).keyBy(r -> {
            //day site level sn station_code去重
            return new Tuple5<>(r._11(),r._1(),r._2(),r._13(),r._14());
        }).coalesce(10, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return Long.valueOf(v1._19()) >= Long.valueOf(v2._19()) ? v1 : v2;
        }).map(r -> {
            //还原RDD
            return r._2();
            //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","work_dt"
        });
        filter.take(10).forEach(r -> System.out.println(r));
        System.out.println(filter.count());
        System.out.println("*************************************");
        JavaPairRDD<Tuple12, Iterable<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>>
                tuple8IterableJavaPairRDD = filter.groupBy(r -> {
            return new Tuple12(r._3(), r._4(), r._5(), r._6(), r._8(), r._9(), r._12(), r._14(), r._20(), r._22(), r._1(), r._2());
        });
        JavaRDD<Tuple14> map = tuple8IterableJavaPairRDD.map(r -> {
            //总数
            int sum = 0;
            //良品数
            int fty = 0;
            Iterator<Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                    iterator = r._2.iterator();
            while (iterator.hasNext()) {
                Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>
                        next = iterator.next();
                if ("1".equals(next._16())) {
                    fty = fty + 1;
                }
                sum = sum + 1;
            }
            return new Tuple14(r._1._1(), r._1._2(), r._1._3(), r._1._4(), r._1._5(), r._1._6(), r._1._7(), r._1._8(), r._1._9(), r._1._10(), fty, sum, r._1._11(), r._1._12());
        });
        map.take(10).forEach(r -> System.out.println(r));
        JavaRDD<Put> putJavaRDD = map.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            while (r.hasNext()) {
                Tuple14 line = r.next();
                ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
                long timeMillis = System.currentTimeMillis();
                String baseRowKey = sb.append(yesterdayStamp).append(":").append(line._13()).append(":").append(line._14()).append(line._4()).append(":").append(UUID.randomUUID()).toString();
                String salt = consistentHashLoadBalance.selectNode(baseRowKey);
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(line._13().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("level_code"), Bytes.toBytes(line._14().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("factory_code"), Bytes.toBytes(line._1().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("process_code"), Bytes.toBytes(line._2().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("area_code"), Bytes.toBytes(line._3().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("line_code"), Bytes.toBytes(line._4().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("part_no"), Bytes.toBytes(line._5().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("platform"), Bytes.toBytes(line._6().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("work_shift"), Bytes.toBytes(line._7().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_code"), Bytes.toBytes(line._8().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_name"), Bytes.toBytes(line._8().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("customer"), Bytes.toBytes(line._9().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("sku"), Bytes.toBytes(line._10().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("fail_count"), Bytes.toBytes(line._11().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("total_count"), Bytes.toBytes(line._12().toString()));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("work_dt"), Bytes.toBytes(yesterday));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(timeMillis)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("update_by"), Bytes.toBytes("wxj"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("data_from"), Bytes.toBytes("DWD"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("data_granularity"), Bytes.toBytes("level"));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        //DWD 写入DWS
        DPHbase.rddWrite("dpm_dws_production_pass_station_dd", putJavaRDD);
        System.out.println("==============================>>>Programe End<<<==============================");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }


}
