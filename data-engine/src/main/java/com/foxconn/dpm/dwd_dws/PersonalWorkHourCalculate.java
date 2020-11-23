package com.foxconn.dpm.dwd_dws;

import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple9;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author HS
 * @className PersonalWorkHourCalculate
 * @description TODO
 * @date 2020/1/2 15:29
 */
public class PersonalWorkHourCalculate extends DPSparkBase {
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        calculateOneDayPersonalWorkHours();
        //calculateBackDayPersonalWorkHours();

        DPSparkApp.stop();
    }


    public void calculateOneDayPersonalWorkHours() throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");


        Date today = new Date();
        Calendar instance = Calendar.getInstance();
        instance.setTime(today);
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        instance.set(Calendar.MILLISECOND, 0);

        instance.add(Calendar.DAY_OF_YEAR, -1);
        String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(instance.getTime());
        instance.add(Calendar.DAY_OF_YEAR, 1);


        JavaRDD<Put> putJavaRDD = calculateDayPersonalWorkHours(fromDayDateAdd(instance.getTime(), -1), fromDayDateAdd(instance.getTime(), 1), yesterday);
        List<Put> take = putJavaRDD.collect();
        for (Put put : take) {
            System.out.println(put);
        }
        if (take.size() > 0) {
            DPHbase.rddWrite("dpm_dws_personnel_emp_workhours_day", putJavaRDD);
        }


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    public void calculateBackDayPersonalWorkHours() throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");

        Date today = new Date();
        Calendar instance = Calendar.getInstance();
        instance.setTime(today);
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        instance.set(Calendar.MILLISECOND, 0);
        //18
        for (int i = getDayOfMonth(); i > 1; i--) {

            //起始时间为当天，结束时间为隔天，由于SDK的查询不包含结束RowKey所以需要加一天
            try {
                String todayDateStr = new SimpleDateFormat("yyyy-MM-dd").format(fromDayDateAdd(instance.getTime(), -i));
                JavaRDD<Put> putJavaRDD = calculateDayPersonalWorkHours(fromDayDateAdd(instance.getTime(), -i), fromDayDateAdd(instance.getTime(), -(i - 1)), todayDateStr);
                List<Put> take = putJavaRDD.take(10);
                for (Put put : take) {
                    System.out.println(put);
                }
                if (take.size() > 0) {
                    DPHbase.rddWrite("dpm_dws_personnel_emp_workhours_day", putJavaRDD);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        System.out.println("==============================>>>Programe End<<<==============================");
    }


    /*
     * ====================================================================
     * 描述:
     *
     *      计算每天的人力数量和人力工时
     *
     * ====================================================================
     */
    public JavaRDD<Put> calculateDayPersonalWorkHours(Date startDate, Date endDate, String targetDateStr) throws Exception {
        /* ********************************************************************************************
         * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRIBE             <<<<<<<<<<<<<<<<<<<<<<<<<<<
         * ********************************************************************************************
         * 前提：dpm_ods_personnel_hr_mbu_definition  组织表提供机能处和Level的对照关系
         *
         * 步骤：
         * 1.sourceRowKey:addsalt+WorkDT+Site+LevelCode+Line+UUID
         *   destRowKey:addsalt+WorkDT+Site+LevelCode+Line+UUID
         *   sourceTable;dpm_dws_personnel_emp_workhours_day
         *   destTable:dpm_dws_personnel_emp_workhours_day
         * 2.sourceColumns：组织：SiteCode BU WorkDT WorkShifitClass，HumresourceCode
         *                  计算：AttendanceWorkhours + WorkoverTimeHours - LeaveHours
         * 3.设置每天的日期获取数据源,按照
         * 4.targetColumns：组织：SiteCodeID LevelCodeID WorkDT WorkShifitClass HumresourceCode
         *                  计算：AttendanceQTY AttendanceWorkhours
         *
         *                                                                             **   **
         *
         *                                                                           ************
         ********************************************************************************************** */

        Scan mbu_scan = new Scan();
        mbu_scan.withStartRow(Bytes.toBytes("0"), true);
        mbu_scan.withStopRow(Bytes.toBytes("z"), true);
        JavaRDD<Result> resultJavaRDD = DPHbase.rddRead("dpm_ods_personnel_hr_mbu_definition", mbu_scan, true);
        List<Tuple2<String, String>> mbu_to_level = resultJavaRDD.map(r -> {

            return new Tuple2<String, String>(
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_MBU_DEFINITION"), Bytes.toBytes("FunctionDepartment")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_MBU_DEFINITION"), Bytes.toBytes("LevelFunctionDepartment"))))
            );
        }).filter(r -> {
            return null != r._1 && null != r._2 && !"".equals(r._1) && !"".equals(r._2);
        }).collect();
        HashMap<String, String> mbu_to_Level_Map = new HashMap<>();
        mbu_to_level.forEach(new Consumer<Tuple2<String, String>>() {
            @Override
            public void accept(Tuple2<String, String> mbu_level_entry) {
                mbu_to_Level_Map.put(mbu_level_entry._1, mbu_level_entry._2);
            }
        });

        Broadcast<HashMap<String, String>> mbu_to_Level_Map_broadcast = DPSparkApp.getContext().broadcast(mbu_to_Level_Map);

        //初始化每日的时间
        String today = new SimpleDateFormat("yyyy-MM-dd").format(startDate);
        String tomorrow = new SimpleDateFormat("yyyy-MM-dd").format(endDate);


        System.out.println(startDate.getTime());
        System.out.println(endDate.getTime());
        System.out.println(today);
        System.out.println(tomorrow);

        JavaRDD<Result> sourceJavaRDD = DPHbase.saltRddRead("dpm_dwd_personnel_emp_workhours_day", String.valueOf(startDate.getTime()), String.valueOf(endDate.getTime()), new Scan());

        JavaRDD<Tuple9<String, String, String, String, String, String, Double, Double, Double>> targetDataSourceRDD = sourceJavaRDD.map(r -> {
            //SiteCode BU WorkDT WorkShifitClass，HumresourceCode
            //AttendanceWorkhours + WorkoverTimeHours - LeaveHours
            return new Tuple9<>(
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("SiteCode")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("BU")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("WorkDT")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("WorkShifitClass")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("HumresourceCode")))),
                    emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("EmpID")))),
                    formatDoubleValue(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("AttendanceWorkhours")))),
                    formatDoubleValue(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("WorkoverTimeHours")))),
                    formatDoubleValue(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("LeaveHours"))))
            );
        }).filter(t9 -> {
            return //t9._1() != null &&
                    t9._2() != null &&
                            t9._3() != null &&
                            t9._4() != null &&
                            t9._5() != null &&
                            t9._6() != null;
        }).map(r -> {
            HashMap<String, String> mbu_to_Level_Map_get = mbu_to_Level_Map_broadcast.value();
            if (mbu_to_Level_Map_get == null) {
                return null;
            }

            String bu = r._2();
            String site_code = "";
            String site_name = r._1();
            switch (site_name) {
                case "武汉":
                case "武漢":
                    site_code = "WH";
            }

            String level = mbu_to_Level_Map_get.get(bu.trim());
            if (level != null) {
                site_code = "WH";
                return new Tuple9<String, String, String, String, String, String, Double, Double, Double>(
                        site_code,
                        level,
                        r._3(),
                        r._4(),
                        r._5(),
                        r._6(),
                        r._7(),
                        r._8(),
                        r._9()
                );
            } else {
                return null;
            }
        });
        System.out.println(targetDataSourceRDD.count());
        return targetDataSourceRDD.filter(r -> {
            return r != null;
        }).filter(r -> {
            switch (r._2()) {
                case "L5":
                case "L6":
                case "L10":
                    return true;
                default:
                    return false;
            }
        }).filter(r -> {
            return targetDateStr.replace("-", "").equals(r._3().replace("-", ""));
        }).distinct().mapToPair(new PairFunction<Tuple9<String, String, String, String, String, String, Double, Double, Double>, String, Tuple4<Double, Double, Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple4<Double, Double, Double, Integer>> call(Tuple9<String, String, String, String, String, String, Double, Double, Double> t9) throws Exception {
                //SiteCode BU WorkDT WorkShifitClass，HumresourceCode
                //AttendanceWorkhours + WorkoverTimeHours - LeaveHours
                //组织key
                StringBuilder sb = new StringBuilder();
                return new Tuple2<String, Tuple4<Double, Double, Double, Integer>>(sb.
                        append(t9._1()).append(":").
                        append(t9._2()).append(":").
                        append(t9._3()).append(":").
                        append(t9._4()).append(":").
                        append(t9._5()).append(":").toString(), new Tuple4<Double, Double, Double, Integer>(t9._7(), t9._8(), t9._9(), 1));
            }
        }).reduceByKey((t3v1, t3v2) -> {
            return new Tuple4<Double, Double, Double, Integer>(
                    t3v1._1() + t3v2._1(),
                    t3v1._2() + t3v2._2(),
                    t3v1._3() + t3v2._3(),
                    t3v1._4() + t3v2._4()
            );
        }).mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
            StringBuilder sb = new StringBuilder();
            while (r.hasNext()) {
                //SiteCode BU WorkDT WorkShifitClass，HumresourceCode
                //AttendanceWorkhours + WorkoverTimeHours - LeaveHours

                //SiteCodeID LevelCodeID WorkDT WorkShifitClass HumresourceCode
                //AttendanceQTY AttendanceWorkhours

                //destRowKey:addsalt+WorkDT+Site+LevelCode+UUID
                //构造put
                Tuple2<String, Tuple4<Double, Double, Double, Integer>> line = r.next();
                String[] orgs = line._1.split(":");
                String workDTStamp = String.valueOf(new SimpleDateFormat("yyyy-MM-dd").parse(orgs[2]).getTime());
                String levelCode = orgs[1];
                String baseRowKey = sb.append(workDTStamp).append(":").append(orgs[0]).append(":").append(levelCode).append(":").append(UUID.randomUUID().toString()).toString();
                String salt = consistentHashLoadBalance.selectNode(baseRowKey);

                //dpm_dws_personnel_emp_workhours_day
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("site_code"), Bytes.toBytes(orgs[0]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("level_code"), Bytes.toBytes(levelCode));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("work_dt"), Bytes.toBytes(orgs[2]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("workshift"), Bytes.toBytes(orgs[3]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("humresource_type"), Bytes.toBytes(orgs[4]));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("attendance_qty"), Bytes.toBytes(String.valueOf((int) (line._2._4()))));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("act_attendance_workhours"), Bytes.toBytes(String.valueOf((float) (line._2._1() + line._2._2() - line._2._3()))));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("data_from"), Bytes.toBytes("DWD"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DAY"), Bytes.toBytes("update_by"), Bytes.toBytes(String.valueOf("HS")));
                puts.add(put);

                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
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

    public static Integer formatIntegerValue(Object o) {
        try {
            return Integer.parseInt((String) o);
        } catch (Exception e) {
            return 0;
        }
    }

    public static String emptyStrNull(String s) {
        return s == null || s.equals("") ? null : s;
    }

    public static String buCodeToLevel(String buCode) {
        String levelCode = null;
        switch (buCode.trim()) {
            case "BU1001":
                levelCode = "L5";
                break;
            case "BU1061":
                levelCode = "L6";
                break;
            case "BU1008":
                levelCode = "L10";
                break;
            default:
                return null;
        }
        return levelCode;
    }

    public static Date fromDayDateAdd(Date startDate, int addDay) {
        Calendar instance = Calendar.getInstance();
        instance.setTime(startDate);
        instance.add(Calendar.DAY_OF_YEAR, addDay);
        return instance.getTime();
    }

    public static int getDayOfMonth() {
        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date());
        return instance.get(Calendar.DAY_OF_MONTH);
    }

}
