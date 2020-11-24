package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.*;

import java.lang.Double;
import java.lang.Long;
import java.lang.reflect.Member;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author HS
 * @className PersonalWorkHourCalculateSprintThree
 * @description TODO
 * @date 2020/1/2 15:29
 */
public class PersonalWorkHourCalculate extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        calculateOneDayPersonalWorkHours();
    }


    public void calculateOneDayPersonalWorkHours() throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");

        JavaRDD<Put> putJavaRDD = calculateDayPersonalWorkHours();

        System.out.println(putJavaRDD.count());
        DPHbase.rddWrite("dpm_dws_personnel_emp_workhours_dd", putJavaRDD);


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    /*
     * ===================  =================================================
     * 描述:
     *
     *      计算每天的人力数量和人力工时
     *
     * ====================================================================
     */
    public JavaRDD<Put> calculateDayPersonalWorkHours() throws Exception {

        Scan mbu_scan = new Scan();
        mbu_scan.withStartRow(Bytes.toBytes("!"), true);
        mbu_scan.withStopRow(Bytes.toBytes("~"), true);
        JavaRDD<Result> resultJavaRDD = DPHbase.rddRead("dpm_ods_personnel_hr_mbu_definition", mbu_scan, true);
        List<Tuple2<String, String>> mbu_to_level = resultJavaRDD.map(r -> {
            String function_department = emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_MBU_DEFINITION"), Bytes.toBytes("function_department"))));
            String level_code = emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_MBU_DEFINITION"), Bytes.toBytes("level_code"))));
            return new Tuple2<String, String>(
                    function_department == null ? function_department : function_department.trim()
                    ,
                    level_code == null ? level_code : level_code.trim()
            );
        }).filter(r -> {
            return null != r._1 && null != r._2 && !"".equals(r._1) && !"".equals(r._2);
        }).collect();


        try {

            for (int i = 0; i < mbu_to_level.size() && i < 5; i++) {
                System.out.println(mbu_to_level.get(i));
            }
        } catch (Exception e) {

        }

        System.out.println("==============================>>>  HR_MBU_DEFINITION  <<<==============================");


        HashMap<String, String> mbu_to_Level_Map = new HashMap<>();
        mbu_to_level.forEach(new Consumer<Tuple2<String, String>>() {
            @Override
            public void accept(Tuple2<String, String> mbu_level_entry) {
                mbu_to_Level_Map.put(mbu_level_entry._1, mbu_level_entry._2);
            }
        });

        Broadcast<HashMap<String, String>> mbu_to_Level_Map_broadcast = DPSparkApp.getContext().broadcast(mbu_to_Level_Map);

        //初始化每日的时间

        String yesterday = batchGetter.getStDateDayStampAdd(-8, "-");
        String tomorrow = batchGetter.getStDateDayStampAdd(1, "-");
/*        yesterday = String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2020-06-19 00:00:00.000").getTime());
        tomorrow = String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2020-06-21 00:00:00.000").getTime());*/
        System.out.println(yesterday + "_" + tomorrow);

        JavaRDD<Result> sourceJavaRDD = DPHbase.saltRddRead("dpm_dwd_personnel_emp_workhours", yesterday, tomorrow, new Scan(), true);

        JavaRDD<Tuple10<String, String, String, String, String, String, Double, Double, Double, String>> sourceDataSourceRDD = sourceJavaRDD.map(r -> {

            //SiteCode BU WorkDT work_shift，humresource_type
            //attendance_workhours + overtime_hours - leave_hours
            return new Tuple11<>(
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("site_code")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("level_code")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("work_dt")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("work_shift")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("humresource_type")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("emp_id")))),
                    formatDoubleValue(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("attendance_workhours")))),
                    formatDoubleValue(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("overtime_hours")))),
                    formatDoubleValue(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("leave_hours")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("data_granularity")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("update_dt"))))
            );
        }).filter(t10 -> {
            return //t9._1() != null &&
                    t10._2() != null &&
                            t10._3() != null &&
                            t10._4() != null &&
                            t10._5() != null &&
                            t10._6() != null;
        }).keyBy(t -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    t._3(), t._4(), t._6()
            );
        }).reduceByKey((kv1, kv2) -> {
            return Long.valueOf(kv1._11()) >= Long.valueOf(kv2._11()) ? kv1 : kv2;
        }).map(t -> {
            return new Tuple10<>(t._2._1(), t._2._2(), t._2._3(), t._2._4(), t._2._5(), t._2._6(), t._2._7(), t._2._8(), t._2._9(), t._2._10());
        });

        try {


            System.out.println("==============================>>>collect End<<<==============================");
            for (Tuple10<String, String, String, String, String, String, Double, Double, Double, String> stringStringStringStringStringStringDoubleDoubleDoubleStringTuple10 : sourceDataSourceRDD.take(5)) {
                System.out.println(stringStringStringStringStringStringDoubleDoubleDoubleStringTuple10);
            }
        } catch (Exception e) {

        }
        System.out.println(sourceDataSourceRDD.count());

        System.out.println("==============================>>>  sourceDataSourceRDD  <<<==============================");

        JavaRDD<Tuple9<String, String, String, String, String, String, Double, Double, Double>> targetDataSourceRDD = sourceDataSourceRDD.map(r -> {

            try {
                HashMap<String, String> mbu_to_Level_Map_get = mbu_to_Level_Map_broadcast.value();

//                String bu = r._2() != null ? r._2() : "";
                String site_code = "";
                String site_name = r._1() != null ? r._1() : "";

                switch (r._2()) {
                    case "L5":
                    case "L6":
                    case "L10":
                        break;
                    default:
                        throw new RuntimeException();
                }


                switch (site_name) {
                    case "武汉":
                    case "武漢":
                    case "WH":
                        site_code = "WH";
                        break;
                    case "CQ":
                    case "重庆":
                    case "重慶":
                        site_code = "CQ";
                }

                if (r._2() != null && (r._2().contains("武汉") || r._2().contains("武漢"))) {
                    site_code = "WH";
                }
                if (r._2() != null && (r._2().contains("重庆") || r._2().contains("重慶"))) {
                    site_code = "CQ";
                }
//                String sourceBu = bu.trim();
//                String level = mbu_to_Level_Map_get.get(sourceBu.startsWith("/") ? sourceBu.substring(1, sourceBu.length()) : sourceBu);
//                if (level == null) {
//                    level = "";
//                }
                return new Tuple9<String, String, String, String, String, String, Double, Double, Double>(
                        site_code,
                        r._2(),
                        r._3(),
                        r._4(),
                        r._5(),
                        r._6(),
                        r._7(),
                        r._8(),
                        r._9()
                );
            } catch (Exception e) {
                return null;
            }

        }).filter(r -> {
            return r != null && !"".equals(r._2()) && !"".equals(r._1());
        });

        try {

            for (Tuple9<String, String, String, String, String, String, Double, Double, Double> stringStringStringStringStringStringDoubleDoubleDoubleTuple9 : targetDataSourceRDD.take(5)) {
                System.out.println(stringStringStringStringStringStringDoubleDoubleDoubleTuple9);
            }

        } catch (Exception e) {

        }
        System.out.println("==============================>>>  targetDataSourceRDD  <<<==============================");

        JavaPairRDD<String, Tuple4<Double, Double, Double, Integer>> preparedData = targetDataSourceRDD.filter(r -> {
            switch (r._2()) {
                case "L5":
                case "L6":
                case "L10":
                    return true;
                default:
                    return false;
            }
        }).distinct().mapToPair(new PairFunction<Tuple9<String, String, String, String, String, String, Double, Double, Double>, String, Tuple4<Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple4<Double, Double, Double, Integer>> call(Tuple9<String, String, String, String, String, String, Double, Double, Double> t9) throws Exception {
                //SiteCode BU WorkDT work_shift，humresource_type
                //attendance_workhours + overtime_hours - leave_hours
                //组织key
                StringBuilder sb = new StringBuilder();
                String person_info = sb.
                        append(t9._1()).append(":").
                        append(t9._2()).append(":").
                        append(t9._3()).append(":").
                        append(t9._4()).append(":").
                        append(t9._5()).append(":").toString();
                return new Tuple2<String, Tuple4<Double, Double, Double, Integer>>(
                        person_info
                        ,
                        new Tuple4<Double, Double, Double, Integer>(t9._7(), t9._8(), t9._9(), 1));
            }
        }).reduceByKey((t3v1, t3v2) -> {
            return new Tuple4<Double, Double, Double, Integer>(
                    t3v1._1() + t3v2._1(),
                    t3v1._2() + t3v2._2(),
                    t3v1._3() + t3v2._3(),
                    t3v1._4() + t3v2._4()
            );
        });

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            preparedData.collectAsMap().forEach(new BiConsumer<String, Tuple4<Double, Double, Double, Integer>>() {
                @Override
                public void accept(String k, Tuple4<Double, Double, Double, Integer> v) {
                    System.out.println(k + "===============" + v.toString());
                }
            });
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        System.out.println("==============================>>>  preparedData  <<<==============================");


        JavaRDD<Put> putJavaRDD = preparedData.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
            StringBuilder sb = new StringBuilder();
            while (r.hasNext()) {
                //SiteCode BU WorkDT work_shift，humresource_type
                //attendance_workhours + overtime_hours - leave_hours

                //SiteCodeID LevelCodeID WorkDT work_shift humresource_type
                //AttendanceQTY attendance_workhours

                //destRowKey:addsalt+WorkDT+Site+LevelCode+UUID
                //构造put
                Tuple2<String, Tuple4<Double, Double, Double, Integer>> line = r.next();
                String[] orgs = line._1.split(":");
                String workDTStamp = String.valueOf(new SimpleDateFormat("yyyy-MM-dd").parse(orgs[2]).getTime());
                String levelCode = orgs[1];
                String baseRowKey = sb.append(workDTStamp).append(":").append(orgs[0]).append(":").append(levelCode).append(":").append(UUID.randomUUID().toString()).toString();
                String salt = consistentHashLoadBalance.selectNode(baseRowKey);

                //dpm_dws_personnel_emp_workhours_dd
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(orgs[0]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("level_code"), Bytes.toBytes(levelCode));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("work_dt"), Bytes.toBytes(orgs[2]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("work_shift"), Bytes.toBytes(orgs[3]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("humresource_type"), Bytes.toBytes(orgs[4]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("attendance_qty"), Bytes.toBytes(String.valueOf((int) (line._2._4()))));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("act_attendance_workhours"), Bytes.toBytes(String.valueOf((float) (line._2._1() + line._2._2() - line._2._3()))));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("data_granularity"), "level".getBytes());
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("data_from"), Bytes.toBytes("DWD"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("update_by"), Bytes.toBytes(String.valueOf("HS")));
                puts.add(put);

                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });

        try {
            for (Put put : putJavaRDD.take(5)) {
                System.out.println(put);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>  putData  <<<==============================");
        return putJavaRDD;


    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }


    /* ********************************************************************************************
     * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRIBE             <<<<<<<<<<<<<<<<<<<<<<<<<<<
     * ********************************************************************************************
     *                                                                             **   **
     *
     *                                                                           ************
     ********************************************************************************************** */
    public static Double formatDoubleValue(Object o) {
        try {
            return Double.parseDouble((String) o);
        } catch (Exception e) {
            return 0.0D;
        }
    }

    public static String emptyStrNull(String s) {
        return s == null || s.equals("") ? null : s;
    }


}
