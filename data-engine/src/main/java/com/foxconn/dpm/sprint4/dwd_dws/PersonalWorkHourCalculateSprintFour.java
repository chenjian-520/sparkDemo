    package com.foxconn.dpm.sprint4.dwd_dws;

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
    import org.apache.hadoop.hbase.util.Bytes;
    import org.apache.spark.api.java.JavaPairRDD;
    import org.apache.spark.api.java.JavaRDD;
    import org.apache.spark.api.java.function.PairFunction;
    import org.apache.spark.broadcast.Broadcast;
    import scala.Tuple2;
    import scala.Tuple3;
    import scala.Tuple4;

    import java.text.SimpleDateFormat;
    import java.util.*;
    import java.util.function.BiConsumer;

    /**
     * @author HS
     * @className PersonalWorkHourCalculateSprintFour
     * @description TODO
     * @date 2020/1/2 15:29
     */
    public class PersonalWorkHourCalculateSprintFour extends DPSparkBase {

        BatchGetter batchGetter = MetaGetter.getBatchGetter();

        @Override
        public void scheduling(Map<String, Object> map) throws Exception {
            calculateOneDayPersonalWorkHours();
            calculateL10_5_6_DayUpLoadPersonalWorkHours();
        }



        public void calculateOneDayPersonalWorkHours() throws Exception {
            System.out.println("==============================>>>Programe Start<<<==============================");

            JavaRDD<Put> putJavaRDD = calculateDayPersonalWorkHours();

            System.out.println(putJavaRDD.count());
            DPHbase.rddWrite("dpm_dws_personnel_emp_workhours_dd", putJavaRDD);


            System.out.println("==============================>>>Programe End<<<==============================");
        }

        public JavaRDD<Put> calculateDayPersonalWorkHours() throws Exception {
            /* ********************************************************************************************
             * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRIBE             <<<<<<<<<<<<<<<<<<<<<<<<<<<
             * ********************************************************************************************
             * 前提：dpm_ods_personnel_hr_mbu_definition  组织表提供机能处和Level的对照关系
             *
             * 步骤：
             * 1.sourceRowKey:addsalt+WorkDT+Site+LevelCode+Line+UUID
             *   destRowKey:addsalt+WorkDT+Site+LevelCode+Line+UUID
             *   sourceTable;dpm_dws_personnel_emp_workhours_dd
             *   destTable:dpm_dws_personnel_emp_workhours_dd
             * 2.sourceColumns：组织：SiteCode BU WorkDT work_shift，humresource_type
             *                  计算：attendance_workhours + overtime_hours - leave_hours
             * 3.设置每天的日期获取数据源,按照
             * 4.targetColumns：组织：SiteCodeID LevelCodeID WorkDT work_shift humresource_type
             *                  计算：AttendanceQTY attendance_workhours
             *
             *                                                                             **   **
             *
             *                                                                           ************
             ********************************************************************************************** */

            Scan mbu_scan = new Scan();
            mbu_scan.withStartRow(Bytes.toBytes("!"), true);
            mbu_scan.withStopRow(Bytes.toBytes("~"), true);
            JavaRDD<Result> resultJavaRDD = DPHbase.rddRead("dpm_ods_personnel_hr_mbu_definition", mbu_scan, true);
            Map<String, String> mbu_to_level = resultJavaRDD.mapToPair(new PairFunction<Result, String, String>() {
                @Override
                public Tuple2<String, String> call(Result r) throws Exception {
                    String function_department = emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_MBU_DEFINITION"), Bytes.toBytes("department"))));
                    String factory_code = emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_MBU_DEFINITION"), Bytes.toBytes("factory_code"))));
                    return new Tuple2<String, String>(
                            function_department == null ? function_department : function_department.trim()
                            ,
                            factory_code == null ? factory_code : factory_code.trim()
                    );
                }
            }).filter(r -> {
                return null != r._1 && null != r._2 && !"".equals(r._1) && !"".equals(r._2);
            }).collectAsMap();


            System.out.println("==============================>>>  HR_MBU_DEFINITION  <<<==============================");
            HashMap<String, String> serializableMap = new HashMap<>();
            mbu_to_level.forEach(new BiConsumer<String, String>() {
                @Override
                public void accept(String k, String v) {
                    serializableMap.put(k, v);
                }
            });
            Broadcast<Map<String, String>> mbu_to_Level_Map_broadcast = DPSparkApp.getContext().broadcast(serializableMap);

            //初始化每日的时间
            String yesterday = batchGetter.getStDateDayStampAdd(-8, "-");
            String tomorrow = batchGetter.getStDateDayStampAdd(1, "-");
        /*        yesterday = String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2020-06-19 00:00:00.000").getTime());
        tomorrow = String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2020-06-21 00:00:00.000").getTime());*/
            System.out.println(yesterday + "_" + tomorrow);

            JavaRDD<Result> sourceJavaRDD = DPHbase.saltRddRead("dpm_dwd_personnel_emp_workhours", yesterday, tomorrow, new Scan(),true);

            JavaRDD<ArrayList<String>> sourceDataSourceRDD = sourceJavaRDD.map(r -> {

                //SiteCode BU WorkDT work_shift，humresource_type
                //attendance_workhours + overtime_hours - leave_hours
                return batchGetter.resultGetColumns(r, "DPM_DWD_PERSONNEL_EMP_WORKHOURS",
                        "site_code", "level_code", "work_dt", "work_shift", "humresource_type", "emp_id", "attendance_workhours", "overtime_hours", "leave_hours", "data_granularity", "update_dt", "department", "factory_code", "process_code"
                );
            }).repartition(50).keyBy(r -> {
                return batchGetter.getStrArrayOrg(",", "-",
                        r.get(2), r.get(3), r.get(5)
                );
            }).reduceByKey((kv1, kv2) -> {
                return Long.valueOf(kv1.get(10)) >= Long.valueOf(kv2.get(10)) ? kv1 : kv2;
            }).map(t -> {
                return t._2;
            });

            try {


                for (ArrayList<String> r : sourceDataSourceRDD.take(5)) {
                    System.out.println(r);
                }
            } catch (Exception e) {

            }
            System.out.println(sourceDataSourceRDD.count());

            System.out.println("==============================>>>  sourceDataSourceRDD  <<<==============================");
            //"site_code", "level_code", "work_dt", "work_shift", "humresource_type", "emp_id", "attendance_workhours", "overtime_hours", "leave_hours", "data_granularity", "update_dt", "mbu", "factory_code"
            JavaRDD<ArrayList<String>> targetDataSourceRDD = sourceDataSourceRDD.map(r -> {

                try {
                    String factory_code = mbu_to_Level_Map_broadcast.value().get(r.get(11).trim());
                    if (factory_code == null) {
                        factory_code = "N/A";
                    }
                    switch (factory_code) {
                        case "DT(I)":
                            factory_code = "DT1";
                            break;
                        case "DT(II)":
                            factory_code = "DT2";
                            break;
                    }
                    r.set(12, factory_code);
                } catch (Exception e) {
                    r.set(12, "N/A");
                }

                if (r.get(13) == null || "".equals(r.get(13))) {
                    r.set(13, "N/A");
                }

                return r;


            }).filter(r -> {
                return r != null;
            }).filter(r -> {
                boolean isLevel5610 = false;
                switch (r.get(1)) {
                    case "L5":
                   /* case "L6":
                    case "L10":*/
                        isLevel5610 = true;
                        break;
                    default:
                        isLevel5610 = false;
                }
                return !"".equals(r.get(1)) && isLevel5610;
            });
            try {

                for (ArrayList<String> r : targetDataSourceRDD.take(5)) {
                    System.out.println(r);
                }

            } catch (Exception e) {

            }
            System.out.println("==============================>>>  targetDataSourceRDD  <<<==============================");

            JavaPairRDD<String, Tuple4<Double, Double, Double, Integer>> preparedData = targetDataSourceRDD.repartition(50).mapToPair(new PairFunction<ArrayList<String>, String, Tuple4<Double, Double, Double, Integer>>() {
                @Override
                public Tuple2<String, Tuple4<Double, Double, Double, Integer>> call(ArrayList<String> r) throws Exception {
                    ///"site_code", "level_code", "work_dt", "work_shift", "humresource_type", "emp_id", "attendance_workhours", "overtime_hours", "leave_hours", "data_granularity", "update_dt", "mbu", "factory_code", "process_code"
                    StringBuilder sb = new StringBuilder();
                    String person_info = sb.
                            append(r.get(0)).append("=").
                            append(r.get(1)).append("=").
                            append(r.get(2)).append("=").
                            append(r.get(3)).append("=").
                            append(r.get(4)).append("=").
                            append(r.get(12)).append("=").
                            append(r.get(13)).toString();
                    return new Tuple2<String, Tuple4<Double, Double, Double, Integer>>(
                            person_info
                            ,
                            new Tuple4<Double, Double, Double, Integer>(
                                    batchGetter.formatDouble(r.get(6)),
                                    batchGetter.formatDouble(r.get(7)),
                                    batchGetter.formatDouble(r.get(8)),
                                    1));
                }
            }).reduceByKey((t3v1, t3v2) -> {
                return new Tuple4<Double, Double, Double, Integer>(
                        t3v1._1() + t3v2._1(),
                        t3v1._2() + t3v2._2(),
                        t3v1._3() + t3v2._3(),
                        t3v1._4() + t3v2._4()
                );
            });

            try {
                List<Tuple2<String, Tuple4<Double, Double, Double, Integer>>> collect = preparedData.collect();
                for (Tuple2<String, Tuple4<Double, Double, Double, Integer>> stringTuple4Tuple2 : collect) {
                    System.out.println(stringTuple4Tuple2);
                }
            } catch (Exception e) {
            }
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
                    String[] orgs = line._1.split("=");
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
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("factory_code"), Bytes.toBytes(orgs[5]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("process_code"), Bytes.toBytes(orgs[6]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("attendance_qty"), Bytes.toBytes(String.valueOf((int) (line._2._4()))));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("act_attendance_workhours"), Bytes.toBytes(String.valueOf((float) (line._2._1() + line._2._2() - line._2._3()))));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("data_granularity"), "process".getBytes());
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


        public void calculateL10_5_6_DayUpLoadPersonalWorkHours() throws Exception {

            System.out.println("==============================>>>line calculateL10_5_6_DayUpLoadPersonalWorkHours Start<<<==============================");

            String startDayStamp = batchGetter.getStDateDayStampAdd(-1, "-");
            String endDayStamp = batchGetter.getStDateDayStampAdd(1, "-");
            
            System.out.println(startDayStamp + "_" + endDayStamp);

            JavaPairRDD<String, Tuple2<Float, Long>> calculatedRDD = DPHbase.saltRddRead("dpm_dwd_production_post_hours", startDayStamp,  endDayStamp, new Scan(), true).keyBy(r -> {

                return batchGetter.getStrArrayOrg("=", "N/A",
                        batchGetter.resultGetColumns(r, "DPM_DWD_PRODUCTION_POST_HOURS",
                                "work_dt", "work_shift", "site_code", "level_code", "factory_code", "process_code", "line_code", "humresource_type", "emp_id"
                        ).toArray(new String[0])
                );
            }).reduceByKey((kv1, kv2) -> {

                return
                        Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_DWD_PRODUCTION_POST_HOURS", "update_dt"))
                                >
                                Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWD_PRODUCTION_POST_HOURS", "update_dt"))
                                ?
                                kv1
                                :
                                kv2
                        ;
            }).map(t -> {
                return t._2;
            }).mapPartitions(batch -> {
                ArrayList<ArrayList<String>> lines = new ArrayList<>();
                BeanGetter beanGetter = MetaGetter.getBeanGetter();
                while (batch.hasNext()) {
                    lines.add(beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_dwd_production_post_hours", "DPM_DWD_PRODUCTION_POST_HOURS"));
                }
                return lines.iterator();
            }).mapToPair(new PairFunction<ArrayList<String>, String, Tuple2<Float, Long>>() {
                @Override
                public Tuple2<String, Tuple2<Float, Long>> call(ArrayList<String> r) throws Exception {
                    return new Tuple2<String, Tuple2<Float, Long>>(
                            batchGetter.getStrArrayOrg("=", "N/A",
                                    //work_dt work_shift site_code level_code factory_code process_code line_code humresource_type
                                    r.get(1), r.get(2), r.get(3), r.get(4), r.get(7), r.get(11), r.get(13), r.get(17), r.get(12)
                            )
                            ,
                            new Tuple2<Float, Long>(batchGetter.formatFloat(r.get(18)) + batchGetter.formatFloat(r.get(19)), batchGetter.formatLong(1L))
                    );
                }
            }).reduceByKey((v1, v2) -> {
                return new Tuple2<Float, Long>(
                        v1._1 + v2._1,
                        v1._2 + v2._2
                );
            });

            try {
                for (Tuple2<String, Tuple2<Float, Long>> stringTuple2Tuple2 : calculatedRDD.collect()) {
                    System.out.println(stringTuple2Tuple2.toString());
                }
            }catch (Exception e){

            }
            System.out.println("==============================>>>calculatedRDD End<<<==============================");


            JavaRDD<Put> putJavaRDD = calculatedRDD.mapPartitions(r -> {
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
                    //work_dt work_shift site_code level_code factory_code process_code line_code humresource_type
                    //WorkHours QTY
                    Tuple2<String, Tuple2<Float, Long>> next = r.next();
                    String[] orgs = next._1.split("=");
                    String workDTStamp = String.valueOf(new SimpleDateFormat("yyyy-MM-dd").parse(orgs[0]).getTime());
                    String levelCode = orgs[3];
                    String baseRowKey = sb.append(workDTStamp).append(":").append(orgs[2]).append(":").append(levelCode).append(":").append(UUID.randomUUID().toString()).toString();
                    String salt = consistentHashLoadBalance.selectNode(baseRowKey);

                    //dpm_dws_personnel_emp_workhours_dd
                    Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(orgs[2]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("level_code"), Bytes.toBytes(levelCode));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("work_dt"), Bytes.toBytes(orgs[0]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("work_shift"), Bytes.toBytes(orgs[1]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("humresource_type"), Bytes.toBytes(orgs[7]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("factory_code"), Bytes.toBytes(orgs[4]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("process_code"), Bytes.toBytes(orgs[5]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("area_code"), Bytes.toBytes(orgs[8]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("line_code"), Bytes.toBytes(orgs[6]));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("attendance_qty"), Bytes.toBytes(String.valueOf(next._2._2)));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("act_attendance_workhours"), Bytes.toBytes(String.valueOf(next._2._1)));
                    put.addColumn(Bytes.toBytes("DPM_DWS_PERSONNEL_EMP_WORKHOURS_DD"), Bytes.toBytes("data_granularity"), "line".getBytes());
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
                DPHbase.rddWrite("dpm_dws_personnel_emp_workhours_dd", putJavaRDD);
            } catch (Exception e) {

            }
            System.out.println("==============================>>>  line putJavaRDD end  <<<==============================");
        }


        @Override
        public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

        }


        public static String emptyStrNull(String s) {
            return s == null || s.equals("") ? null : s;
        }

    }
