package com.foxconn.dpm.sprint1_2.ods_dwd;

import com.foxconn.dpm.sprint1_2.ods_dwd.beans.*;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.*;

import java.lang.Long;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Description:
 * 离职率计算
 * 根据ods的dpm_ods_hr_dl_emp_info表，取出对应的离职信息 放入dwd表 dpm_dwd_personnel_separation 离职人员表
 *
 * @author FL cj
 * @version 1.0
 * @timestamp 2020/1/10
 */
public class TurnoverDwd extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");
        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;
        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1);
            today = batchGetter.getStDateDayAdd(0);
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-7);
            todayStamp = batchGetter.getStDateDayStampAdd(1);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }
        /**
         *
         * 获取组织机构信息，静态数据，用于取得离职人员的site_code，使用org_code关联得出plant字段
         * 全表 rowkey就是组织编码
         */
        JavaRDD<Result> orgData = DPHbase.rddRead("dpm_ods_hr_hr_org", new Scan(), true);
        JavaRDD<EhrOdsOrg> orgFormatData = orgData.map((Function<Result, EhrOdsOrg>) result -> {
            final String family = "DPM_ODS_HR_HR_ORG";
            String bg = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("bg")));
            String plant = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("plant")));
            String dept = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("dept")));
            String bu = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("bu")));
            String org_fee_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_fee_code")));
            String org_name = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_name")));
            String org_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_code")));
            return new EhrOdsOrg(bg, plant, dept, bu, org_fee_code, org_name, org_code);
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrOdsOrg> take6 = orgFormatData.take(5);
        for (EhrOdsOrg ehrOdsOrg : take6) {
            System.out.println(ehrOdsOrg);
        }
        System.out.println("==============================>>>orgFormatData Source End<<<==============================");

        /**
         *
         * 获取考勤组织机构，加盐数据，用于取得离职人员的site_code，使用org_code关联得出plant字段
         * 扫描指定时间或者昨天的数据
         */
        JavaRDD<Result> empOrgData = DPHbase.saltRddRead("dpm_ods_hr_dl_emp_dept", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<EhrEmpOdsOrg> empOrgFormatData = empOrgData.map(new Function<Result, EhrEmpOdsOrg>() {
            @Override
            public EhrEmpOdsOrg call(Result result) throws Exception {
                final String family = "DPM_ODS_HR_DL_EMP_DEPT";
                String emp_id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("emp_id")));
                String sbg_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("sbg_code")));
                String l1 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l1")));
                String l2 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l2")));
                String l3 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l3")));
                String l4 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l4")));
                if (StringUtils.isBlank(l3) && StringUtils.isNotEmpty(l4)) {
                    if (l4.indexOf("(武漢)") == 0) {
                        String temp = l4.substring(l4.indexOf(")") + 1);
                        String[] a = temp.split("/");
                        if (a.length < 2) {
                            l3 = temp;
                        } else {
                            l3 = a[0] + "/" + a[1] + "/" + a[2];
                        }
                    } else {
                        l3 = l4;
                    }
                }
                String l5 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l5")));
                String group_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("group_code")));
                String department_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("department_code")));
                return new EhrEmpOdsOrg(emp_id, sbg_code, l1, l2, l3, l4, l5, group_code, department_code);
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrEmpOdsOrg> take5 = empOrgFormatData.take(5);
        for (EhrEmpOdsOrg ehrEmpOdsOrg : take5) {
            System.out.println(ehrEmpOdsOrg);
        }
        System.out.println("==============================>>>empOrgFormatData Source End<<<==============================");

        /**
         * 获取机能处信息，静态数据，用于取得level_code，使用l3关联functionDepartment得出leveFunctionDepartment字段
         */
        JavaRDD<Result> mbuData = DPHbase.rddRead("dpm_ods_personnel_hr_mbu_definition", new Scan());
        JavaRDD<EhrOdsMbu> mbuFormatData = mbuData.map((Function<Result, EhrOdsMbu>) result -> {
            final String family = "DPM_ODS_PERSONNEL_HR_MBU_DEFINITION";
            String Department = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("department")));
            String Product = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("product")));
            String FunctionDepartment = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("function_department")));
            String LevelFunctionDepartment = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("level_code")));
            String Site = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("site_code")));
            String factoryCode = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("factory_code")));

            return new EhrOdsMbu(Department, Product, FunctionDepartment, LevelFunctionDepartment, Site, factoryCode);
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrOdsMbu> take7 = mbuFormatData.take(5);
        for (EhrOdsMbu ehrodsmbu : take7) {
            System.out.println(ehrodsmbu);
        }


        System.out.println("==============================>>>mbuFormatData Source End<<<==============================");

        /*
         * ====================================================================
         * 描述:
         *     获取指定时间，若无指定时间则取昨天，取ods的dpm_ods_hr_dl_emp_info表
         *     盐:每天00点时间戳:empid
         * ====================================================================
         */

        JavaRDD<Result> empInfoData = DPHbase.saltRddRead("dpm_ods_hr_dl_emp_info", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());

        //清洗数据
        JavaRDD<EhrOdsEmpinfo> empInfoFormatData = empInfoData.map((Function<Result, EhrOdsEmpinfo>) result -> {
            final String family = "DPM_ODS_HR_DL_EMP_INFO";
            String emp_id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("emp_id")));
            String human_resource_category_text = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("human_resource_category_text")));
            String person_post = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("person_post")));
            String org_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_code")));
            String resign_date = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("resign_date")));
            //用更新时间去重
            try {
                Long update_dt = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_dt"))));
                return new EhrOdsEmpinfo(emp_id, human_resource_category_text, person_post, org_code, resign_date, update_dt);
            } catch (Exception e) {
                return null;
            }
        }).filter(r -> {
            return r != null && StringUtils.isNotEmpty(r.getResign_date());
//            return r != null && "A0238181".equals(r.getEmp_id());
        }).keyBy(r -> {
            return r.getEmp_id();
        }).coalesce(40, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return v1.getUpdate_dt() >= v2.getUpdate_dt() ? v1 : v2;
        }).map(r -> {
            //还原RDD
            return r._2;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        List<EhrOdsEmpinfo> takeEmpInfo = empInfoFormatData.take(5);
        for (EhrOdsEmpinfo outputTemp : takeEmpInfo) {
            System.out.println(outputTemp);
        }

        System.out.println("==============================>>>empInfo source  End<<<==============================");

        //取离职数据转存dwd表
        JavaRDD<DwdTurnoverOutput> mainRdd = empInfoFormatData
                .mapPartitions((FlatMapFunction<Iterator<EhrOdsEmpinfo>, DwdTurnoverOutput>) iterator -> {
                    List<DwdTurnoverOutput> data = new ArrayList<>();
                    iterator.forEachRemaining(empinfo -> {
                        DwdTurnoverOutput output = new DwdTurnoverOutput();
                        output.setEmp_id(empinfo.getEmp_id());
                        output.setOrg_code(empinfo.getOrg_code());
                        output.setTime_of_separation(empinfo.getResign_date());
                        data.add(output);
                    });
                    return data.iterator();
                });

        /**
         * 按工号分组与组织机构数据合并得到sitecode，如果sitecode不存在的数据会被筛掉
         */
        mainRdd = mainRdd.keyBy(s -> s.getOrg_code()).join(orgFormatData.keyBy(r -> r.getOrg_code()).coalesce(20, false).reduceByKey((v1, v2) -> v1)).map((Function<Tuple2<String, Tuple2<DwdTurnoverOutput, EhrOdsOrg>>, DwdTurnoverOutput>) tuple2 -> {
            DwdTurnoverOutput output = tuple2._2._1();
            EhrOdsOrg org = tuple2._2._2();
            if (org != null) {
                output.setSite_code(org.getPlant());
            }
            return output;
        });
        List<DwdTurnoverOutput> takeMain2 = mainRdd.take(5);
        for (DwdTurnoverOutput outputTemp : takeMain2) {
            System.out.println(outputTemp);
        }


        System.out.println("==========================>>>mainRdd calculate site_code End<<<==========================");

        /**
         * 根据考勤班组信息关联机能处信息，按考勤组织按l3分组，机能处按functionDepartMent EhrOdsMbu EhrEmpOdsOrg
         */
        JavaRDD<EhrEmpOdsOrg> dimensionRdd1 = empOrgFormatData.keyBy(r -> r.getL3().trim()).repartition(40).leftOuterJoin(mbuFormatData.keyBy(r -> r.getFunctionDepartment().trim()).repartition(40).reduceByKey((v1, v2) -> v1)).map((Function<Tuple2<String, Tuple2<EhrEmpOdsOrg, Optional<EhrOdsMbu>>>, EhrEmpOdsOrg>) tuple2 -> {
            EhrEmpOdsOrg org = tuple2._2._1;
            EhrOdsMbu mbu = tuple2._2._2.orElse(null);
            if (mbu != null) {
                org.setLevel_code(mbu.getLevelFunctionDepartment());

            }
            return org;
        });

        JavaRDD<EhrEmpOdsOrg> dimensionRdd = dimensionRdd1.keyBy(r -> r.getL4().trim()).repartition(40).leftOuterJoin(mbuFormatData.keyBy(r -> r.getDepartment().trim()).repartition(40).reduceByKey((v1, v2) -> v1)).map((Function<Tuple2<String, Tuple2<EhrEmpOdsOrg, Optional<EhrOdsMbu>>>, EhrEmpOdsOrg>) tuple2 -> {
            EhrEmpOdsOrg org = tuple2._2._1;
            EhrOdsMbu mbu = tuple2._2._2.orElse(null);
            if (mbu != null) {
                org.setFactory_code(mbu.getFactory_code());
            }
            return org;
        });


        System.out.println("==============================>>>level_code source End<<<==============================");


        mainRdd = mainRdd.keyBy(s -> s.getEmp_id()).coalesce(20, false).leftOuterJoin(dimensionRdd.keyBy(r -> r.getEmp_id()).reduceByKey((v1, v2) -> {
            if (v1.getFactory_code() != null) {
                return v1;
            } else {
                return v2;
            }
        })).coalesce(20, false).map((Function<Tuple2<String, Tuple2<DwdTurnoverOutput, Optional<EhrEmpOdsOrg>>>, DwdTurnoverOutput>) tuple2 -> {
            DwdTurnoverOutput output = tuple2._2._1;
            EhrEmpOdsOrg org = tuple2._2._2.orElse(null);
            if (org != null) {
                output.setLevel_code(org.getLevel_code());
                output.setFactory_code(org.getFactory_code());
                output.setDepartment(org.getL4());
            }
            return output;
        });

//        DPSparkApp.getSession().createDataFrame(mainRdd,DwdTurnoverOutput.class).createOrReplaceTempView("mainRdd");
//        DPSparkApp.getSession().createDataFrame(dimensionRdd,EhrEmpOdsOrg.class).createOrReplaceTempView("dimensionRdd");
//
//
//        DPSparkApp.getSession().sql("select * from dimensionRdd where factory_code = 'DT(I)' or factory_code ='DT(II)' ").show();
//        DPSparkApp.getSession().sql("select * from dimensionRdd where emp_id = 'G5268459'or emp_id = 'W4209341' or emp_id = 'F1000612' ").show();
//        DPSparkApp.getSession().sql("select * from mainRdd left join dimensionRdd on dimensionRdd.emp_id=mainRdd.emp_id").show();
//
//        DPSparkApp.getSession().sql("select* from (select * from mainRdd left join dimensionRdd on dimensionRdd.emp_id=mainRdd.emp_id) where dimensionRdd.factory_code != null ").show();


        List<DwdTurnoverOutput> take8 = mainRdd.take(5);
        for (DwdTurnoverOutput output : take8) {
            System.out.println(output);
        }
        System.out.println("==============================>>>level_code calculate End<<<==============================");


        // todo cj
        System.out.println("==========================>>>计算 humresource_type <<<==========================");
        /**
         * 3.HumresourceCode字段计算逻辑   淘汰逻辑  不需要了解
         * 通过dpm_ods_hr_hr_emp_info的PersonPost 去匹配dpm_ods_personnel_hr_job_directory的JobCoding字段
         *
         * 例子: PersonPost类似于这样的数据       A(TTC0200)電子產品測試工程       我们扣取括号中的字符得到  TTC0200   后去比较
         *      dpm_ods_personnel_hr_job_directory的JobCoding字段进行匹配
         *
         *
         * I.  如果能匹配到就使用dpm_ods_personnel_hr_job_directory的
         *      DirectClassification 和 JobType 字段去得到人力类型的四种类型
         *   DirectClassification=DL1 -------------    DL1
         *   DirectClassification=DL2 and  JobType=固定崗位 ---------------  DL2F
         *   DirectClassification=DL2 and  JobType=變動崗位 ---------------  DL2V
         *   DirectClassification=IDL1  ---------------- IDL
         *   DirectClassification=IDL2  ---------------- IDL
         *
         * II. 如果找不到匹配就取dpm_ods_hr_hr_emp_info的humanresource_category_text这个字段
         *   直接人力  -------------  DL
         *   间接人力  -------------  IDL
         */

        // 添加L5的process
         mainRdd = mainRdd.map(r -> {
            if ("L5".equals(r.getLevel_code())) {
                String department = r.getDepartment();
                if (department != null) {
                    //衝壓 Stamping/成型 Molding/塗裝 Painting/組裝 Ass'y
                    if (department.contains("衝壓")) {
                        r.setProcess_code("Stamping");
                    } else if (department.contains("成型")) {
                        r.setProcess_code("Molding");
                    } else if (department.contains("塗裝")) {
                        r.setProcess_code("Painting");
                    } else if (department.contains("組裝")) {
                        r.setProcess_code("Assy");
                    } else {
                        r.setProcess_code("");
                    }
                } else {
                    r.setProcess_code("");
                }
            }
            return r;
         });

        //新逻辑在toPeopleIDL01() 里面  淘汰逻辑为 toPeopleIDL()
        JavaRDD<Result> empinfoData = DPHbase.saltRddRead("dpm_ods_hr_dl_emp_info", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<EhrOdsEmpinfo> ehrOdsEmpinfoJavaRDD = toPeopleIDL01(empinfoData);


        JavaRDD<Tuple7<String, String, String, String, String, String, String>> map3 = mainRdd.keyBy(r -> r.getEmp_id()).leftOuterJoin(ehrOdsEmpinfoJavaRDD.keyBy(r -> r.getEmp_id())).map(r -> {
            EhrOdsEmpinfo ehrOdsEmpinfo = r._2._2.orElse(null);
            String positiontype = ehrOdsEmpinfo.getPositiontype();
            if (positiontype == null) {
                positiontype = "DL";
            }
            return new Tuple7<>(r._2._1.getSite_code(), r._2._1.getLevel_code(), r._2._1.getFactory_code(), r._2._1.getProcess_code(),
                    r._2._1.getEmp_id(), r._2._1.getTime_of_separation(), positiontype);
        });

//        JavaRDD<Tuple7<String, String, String, String, String, String, String>> map3 = toPeopleIDL(mainRdd);

        long count2 = map3.count();
        System.out.println(count2);
        System.out.println("==========================>>>计算 humresource_type END<<<==========================");
        //输出到hbase

        JavaRDD<Put> toWriteData = map3.mapPartitions(r -> {
            SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            Long updateDt = System.currentTimeMillis();
            while (r.hasNext()) {
                //site_code+level_code+line_code+platform+part_no
                Tuple7<String, String, String, String, String, String, String> next = r.next();
                String timeStamp = String.valueOf(formatWorkDt.parse(next._6()).getTime());
                String baseRowKey = timeStamp + ":" + next._5() + ":" + String.valueOf(next._1());
                //dpm_dwd_safety_detail
                Put put = new Put(Bytes.toBytes(baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("site_code"), Bytes.toBytes(next._1()== null ? "" : next._1()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("level_code"), Bytes.toBytes(next._2()== null ? "" : next._2()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("factory_code"), Bytes.toBytes(next._3()== null ? "" : next._3()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("process_code"), Bytes.toBytes(next._4()== null ? "" : next._4()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("emp_id"), Bytes.toBytes(next._5()== null ? "" : next._5()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("time_of_separation"), Bytes.toBytes(next._6()== null ? "" : next._6()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("humresource_type"), Bytes.toBytes(next._7()== null ? "" : next._7()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("update_dt"), Bytes.toBytes(updateDt.toString()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("update_by"), Bytes.toBytes("system"));
                put.addColumn(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("data_from"), Bytes.toBytes("turnover-dwd"));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        toWriteData.take(5).forEach(r -> System.out.println(r));
        try {
            DPHbase.rddWrite("dpm_dwd_personnel_separation", toWriteData);
            System.out.println("==============================>>>toWriteData Calculate End<<<==============================");
        } catch (Exception e) {
            System.out.println("===============================>>>>>>>>>>>>>>>>>>>Write No Data Or API Err<<<<<<<<<<<<<<<<<<<<====================");
        }
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }


    public static JavaRDD<Tuple7<String, String, String, String, String, String, String>> toPeopleIDL(JavaRDD<DwdTurnoverOutput> mainRdd) throws Exception {

        JavaRDD<Tuple6<String, String, String, String, String, String>> map1 = mainRdd.groupBy(r -> r.getEmp_id().trim()).map(r -> {
            DwdTurnoverOutput next = r._2().iterator().next();
            return new Tuple6<>(
                    next.getSite_code(),
                    next.getLevel_code(),
                    next.getFactory_code(),
                    next.getProcess_code(),
                    next.getEmp_id(),
                    next.getTime_of_separation());
        });


        System.out.println(mainRdd.count() + "-**-" + map1.count());
        //dpm_ods_hr_dl_emp_info
        //dpm_ods_personnel_hr_job_directory
        JavaRDD<Tuple3<String, String, String>> jobDirectory = DPHbase.rddRead("dpm_ods_personnel_hr_job_directory", new Scan(), true).map(r -> {
            return new Tuple3<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_JOB_DIRECTORY"), Bytes.toBytes("job_coding"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_JOB_DIRECTORY"), Bytes.toBytes("direct_classification"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PERSONNEL_HR_JOB_DIRECTORY"), Bytes.toBytes("job_type")))
            );
        });
        JavaPairRDD<String, Tuple3<String, String, String>> empInfo = DPHbase.rddRead("dpm_ods_hr_dl_emp_info", new Scan(), true).map(r -> {
            return new Tuple3<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_HR_DL_EMP_INFO"), Bytes.toBytes("emp_id"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_HR_DL_EMP_INFO"), Bytes.toBytes("person_post"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_HR_DL_EMP_INFO"), Bytes.toBytes("human_resource_category_text")))
            );
        }).keyBy(r -> r._1());

        System.out.println("==============================>>1><<<==============================");

        JavaRDD<Tuple8<String, String, String, String, String, String, String, String>> map2 = map1.keyBy(r -> r._5()).leftOuterJoin(empInfo).groupByKey().map(r -> {
            Tuple2<Tuple6<String, String, String, String, String, String>, Optional<Tuple3<String, String, String>>> next = r._2.iterator().next();

            //取personpost括号中的值   example: A(TTC0200)電子產品測試工程
            String personPost = next._2.orElse(null)._2();
            String tmp = "";
            if (personPost == null || personPost == "") {
                if (next._2.orElse(null)._3().equals("直接人力")) {
                    return new Tuple8<>(next._1._1(), next._1._2(), next._1._3(), next._1._4(), next._1._5(), next._1._6(), next._2.orElse(null)._2(), "DL");
                } else {
                    return new Tuple8<>(next._1._1(), next._1._2(), next._1._3(), next._1._4(), next._1._5(), next._1._6(), next._2.orElse(null)._2(), "IDL");
                }
            } else {
                System.out.println("notnull");
                try {
                    if (personPost.contains("(") && personPost.contains(")")) {
                        tmp = personPost.substring(personPost.indexOf("(") + 1, personPost.indexOf(")"));
                    } else {
                        tmp = personPost;
                    }
                } catch (Exception e) {
                    System.err.println(">>>>Err ehrOdsEmpinfo Person_post=====>>>>>>>" + personPost);
                    return null;
                }
                if (next._2.orElse(null)._3().equals("直接人力")) {
                    return new Tuple8<>(next._1._1(), next._1._2(), next._1._3(), next._1._4(), next._1._5(), next._1._6(), tmp, "DL");
                } else {
                    return new Tuple8<>(next._1._1(), next._1._2(), next._1._3(), next._1._4(), next._1._5(), next._1._6(), tmp, "IDL");
                }
            }
        });
        map2.take(5).forEach(r -> System.out.println(r));

        System.out.println("==============================>2>><<<==============================");

        JavaRDD<Tuple7<String, String, String, String, String, String, String>> map3 = map2.keyBy(r -> r._7()).leftOuterJoin(jobDirectory.keyBy(r -> r._1())).map(r -> {
            if (r._1.trim() == null || r._1.trim() == "") {
                return new Tuple7<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(), r._2._1._5(), r._2._1._6(), r._2._1._8());
            } else {
                String JobType = "";
                String DirectClassification = "";
                if (r._2._2.orElse(null) == null) {
                    DirectClassification = "";
                } else {
                    DirectClassification = r._2._2.orElse(null)._2();
                }
                if (r._2._2.orElse(null) == null) {
                    JobType = "";
                } else {
                    JobType = r._2._2.orElse(null)._3();
                }
//                DirectClassification=DL1 -------------    DL1
//                DirectClassification=DL2 and  JobType=固定崗位 ---------------  DL2F
//                DirectClassification=DL2 and  JobType=變動崗位 ---------------  DL2V
//                DirectClassification=IDL1  ---------------- IDL
//                DirectClassification=IDL2  ---------------- IDL
                if ("DL1".equals(DirectClassification)) {
                    return new Tuple7<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(), r._2._1._5(), r._2._1._6(), "DL1");
                } else if ("IDL1".equals(DirectClassification)) {
                    return new Tuple7<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(), r._2._1._5(), r._2._1._6(), "IDL");
                } else if ("IDL2".equals(DirectClassification)) {
                    return new Tuple7<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(), r._2._1._5(), r._2._1._6(), "IDL");
                } else if ("DL2".equals(DirectClassification) && "DL2F".equals(JobType)) {
                    return new Tuple7<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(), r._2._1._5(), r._2._1._6(), "DL2F");
                } else if ("DL2".equals(DirectClassification) && "DL2V".equals(JobType)) {
                    return new Tuple7<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(), r._2._1._5(), r._2._1._6(), "DL2V");
                } else {
                    return new Tuple7<>(r._2._1._1(), r._2._1._2(), r._2._1._3(), r._2._1._4(), r._2._1._5(), r._2._1._6(), r._2._1._8());
                }
            }
        });
        return map3;
    }


    public static JavaRDD<EhrOdsEmpinfo> toPeopleIDL01(JavaRDD<Result> empinfoData) {
        BatchGetter batchGetter = MetaGetter.getBatchGetter();
        JavaRDD<EhrOdsEmpinfo> empInfoFormatData = empinfoData.map(new Function<Result, EhrOdsEmpinfo>() {
            @Override
            public EhrOdsEmpinfo call(Result result) throws Exception {
                final String family = "DPM_ODS_HR_DL_EMP_INFO";
                String emp_id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("emp_id"))).trim();
                String human_resource_category_text = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("human_resource_category_text"))).trim();
                String person_post = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("person_post"))).trim();
                String org_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_code"))).trim();
                String positiontype = batchGetter.resultGetColumn(result, family, "positiontype");
                //用更新时间去重
                try {
                    Long update_dt = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_dt"))));
                    return new EhrOdsEmpinfo(emp_id, human_resource_category_text, person_post, org_code, update_dt, positiontype);
                } catch (Exception e) {
                    return null;
                }
            }
        }).filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return r.getEmp_id();
        }).coalesce(40, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return v1.getUpdate_dt() >= v2.getUpdate_dt() ? v1 : v2;
        }).map(r -> {
            //还原RDD
            return r._2;
        });
        return empInfoFormatData;
    }
}
