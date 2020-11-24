package com.foxconn.dpm.sprint1_2.ods_dwd;


import com.foxconn.dpm.sprint1_2.dwd_dws.beans.TurnoverBean;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description:
 * <p>
 * 计算日人力工时数据
 * 输入表：抽取ods表
 * 1.(dpm_ods_hr_hr_emp_kaoqin表，主表，salt),取当月1日至当前时间的数据
 * 2.(dpm_ods_hr_dl_emp_info，维度表，不加盐)，取全表数据
 * 3.(dpm_ods_personnel_hr_job_directory，维度表，不加盐)，取全表数据
 * 4.(dpm_ods_hr_hr_emp_jiaban，高表，salt)，取当月1日至当前时间的数据
 * 5.(dpm_ods_hr_hr_emp_qingjia，高表，salt)，取当月1日至当前时间的数据
 * 6.(dpm_ods_hr_dl_emp_dept，维度表，不加盐)，取全表数据
 * 7.(dpm_ods_hr_hr_org，维度表，不加盐)，取全表数据
 * <p>
 * 说明：
 * 1，主要考勤数据
 * 2，用于取HumresourceCode 人力类别字段的数据
 * 4，用于计算加班数据
 * 5，用于计算请假数据
 * <p>
 * 输出表：放入dwd表 dpm_dwd_personnel_emp_workhours_day  日人员出勤资料
 *
 * @author FL cj
 * @version 1.0
 * @timestamp 2020/1/2
 */
public class EhrDwd extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");

        /**
         * =====================================
         * 取数据
         * 1.(W表，主表，salt),取当月1日至当前时间的数据
         * 2.(dpm_ods_hr_dl_emp_info，维度表，不加盐)，取全表数据
         * 3.(dpm_ods_personnel_hr_job_directory，维度表，不加盐)，取全表数据
         * 4.(dpm_ods_hr_hr_emp_jiaban，高表，salt)，取当月1日至当前时间的数据
         * 5.(dpm_ods_hr_hr_emp_qingjia，高表，salt)，取当月1日至当前时间的数据
         * 6.(dpm_ods_hr_dl_emp_dept，维度表，salt)，取全表数据
         * 7.(dpm_ods_hr_hr_org，维度表，不加盐)，取全表数据
         *
         * 说明：
         * 1，主要考勤数据
         * 2，3用于取HumresourceCode 人力类别字段的数据
         * 4，用于计算加班数据
         * 5，用于计算请假数据
         * =====================================
         */
        //初始化时间
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

        /*
         * ====================================================================
         * 描述:
         *      每次算昨天的人力工时，因为界面上每天只显示昨天的人力工时
         * ====================================================================
         */
        JavaRDD<Result> kaoqinData = DPHbase.saltRddRead("dpm_ods_hr_hr_emp_kaoqin", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Result> jiabanData = DPHbase.saltRddRead("dpm_ods_hr_dl_emp_jiaban", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Result> qingjiaData = DPHbase.saltRddRead("dpm_ods_hr_hr_emp_qingjia", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        //盐:每天00点时间戳:empid
        JavaRDD<Result> empOrgData = DPHbase.saltRddRead("dpm_ods_hr_dl_emp_dept", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Result> empinfoData = DPHbase.saltRddRead("dpm_ods_hr_dl_emp_info", yesterdayStamp, todayStamp, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());

        /*
         * ====================================================================
         * 描述:
         *      empinfoData需要去重，本身数据量比较大容易产生脏数据
         * ====================================================================
         */
        //全表  rowkey就是部门编码
        JavaRDD<Result> jobDirectoryData = DPHbase.rddRead("dpm_ods_personnel_hr_job_directory", new Scan());
        //全表 rowkey就是组织编码
        JavaRDD<Result> orgData = DPHbase.rddRead("dpm_ods_hr_hr_org", new Scan());
        //全表
        Scan scan = new Scan();
        scan.withStartRow("!".getBytes());
        scan.withStopRow("~".getBytes());
        JavaRDD<Result> mbuData = DPHbase.rddRead("dpm_ods_personnel_hr_mbu_definition", scan);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        //转换：转换为dto对象
        JavaRDD<EhrOdsKaoqin> kaoQinFormatData = kaoqinData.map(new Function<Result, EhrOdsKaoqin>() {
            @Override
            public EhrOdsKaoqin call(Result result) throws Exception {
                final String family = "DPM_ODS_HR_HR_EMP_KAOQIN";
                String EmpId = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("emp_id")));
                String KQDate = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("kq_date")));
                String LateTimes = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("late_times")));
                String LeaveEarlyTimes = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("leave_early_times")));
                String AbsentTimes = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("absent_times")));
                String OTLateTimes = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ot_late_times")));
                String OTLeaveEarlyTimes = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ot_leave_early_times")));
                String OTAbsentTimes = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ot_absent_times")));
                String USStartTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("uss_start_time")));
                String URStartTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ur_start_time")));
                String USEndTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("us_end_time")));
                String UREndTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ur_end_time")));
                String DSStartTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ds_start_time")));
                String DRStartTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("dr_start_time")));
                String DSEndTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ds_end_time")));
                String DREndTime = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("dr_end_time")));
                String update_dt = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_dt")));


                return new EhrOdsKaoqin(EmpId, KQDate, LateTimes, LeaveEarlyTimes, AbsentTimes, OTLateTimes, OTLeaveEarlyTimes, OTAbsentTimes, USStartTime, URStartTime, USEndTime, UREndTime, DSStartTime, DRStartTime, DSEndTime, DREndTime, "",update_dt);
            }
        }).filter(b -> {
            return b.getKq_date() != null && !"".equals(b.getKq_date()) && b.getEmp_id() != null && !"".equals(b.getEmp_id());
        }).keyBy(r -> {
            return new Tuple2<>(r.getEmp_id(), r.getKq_date());
        }).coalesce(10, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return Long.valueOf(v1.getUpdate_dt()) >= Long.valueOf(v2.getUpdate_dt()) ? v1 : v2;
        }).map(r -> {
            //还原RDD
            return r._2();
        }).persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("------------------9----------------");

        long count3 = kaoQinFormatData.filter(r -> r.getEmp_id().matches("^W.*")).count();
        long count4 = kaoQinFormatData.filter(r -> r.getEmp_id().matches("^C.*")).count();
        long count5 = kaoQinFormatData.filter(r -> r.getEmp_id().matches("^F.*")).count();
        System.out.println(count3+"----"+count4+"-----"+count5);




        try {
            System.out.println(kaoQinFormatData.count());
            List<EhrOdsKaoqin> take = kaoQinFormatData.take(5);
            for (EhrOdsKaoqin ehrOdsKaoqin : take) {
                System.out.println(ehrOdsKaoqin);
            }
        } catch (Exception e) {

        }
        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>kaoQinFormatData Source End<<<==============================");
        JavaRDD<EhrOdsQingjia> qingjiaFormatData = qingjiaData.map(new Function<Result, EhrOdsQingjia>() {
            @Override
            public EhrOdsQingjia call(Result result) throws Exception {
                final String family = "DPM_ODS_HR_HR_EMP_QINGJIA";
                String emp_id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("emp_id")));
                String leave_type = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("leave_type")));
                String start_date = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("start_date")));
                String start_time = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("start_time")));
                String end_date = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("end_date")));
                String end_time = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("end_time")));
                String total_hours = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("total_hours")));
                String sign_flag = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("sign_flag")));
                String is_modify = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("is_modify")));
                String is_agree = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("is_agree")));
                return new EhrOdsQingjia(emp_id, leave_type, start_date, start_time, end_date, end_time, total_hours, sign_flag, is_modify, is_agree);
            }
        }).filter(new Function<EhrOdsQingjia, Boolean>() {
            @Override
            public Boolean call(EhrOdsQingjia ehrOdsQingjia) throws Exception {
                if ("3".equals(ehrOdsQingjia.getSign_flag())) {
                    return true;
                } else {
                    return false;
                }
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrOdsQingjia> take1 = qingjiaFormatData.take(5);
        for (EhrOdsQingjia ehrOdsQingjia : take1) {
            System.out.println(ehrOdsQingjia);
        }
        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>qingjiaFormatData Source End<<<==============================");
        JavaRDD<EhrOdsJiaban> jiabanFormatData = jiabanData.map(new Function<Result, EhrOdsJiaban>() {
            @Override
            public EhrOdsJiaban call(Result result) throws Exception {
                final String family = "DPM_ODS_HR_DL_EMP_JIABAN";
                String EmpID = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("emp_id")));
                String OTDate = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ot_date")));
                String OTHours = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("ot_hours")));
                return new EhrOdsJiaban(EmpID, OTDate, OTHours);
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrOdsJiaban> take2 = jiabanFormatData.take(5);
        for (EhrOdsJiaban ehrOdsJiaban : take2) {
            System.out.println(ehrOdsJiaban);
        }
        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>jiabanFormatData Source End<<<==============================");
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
        empInfoFormatData.persist(StorageLevel.MEMORY_AND_DISK());

        List<EhrOdsEmpinfo> take = empInfoFormatData.take(5);
        for (EhrOdsEmpinfo ehrOdsEmpinfo : take) {
            System.out.println(ehrOdsEmpinfo);
        }
        System.out.println("------------------8----------------");

        System.out.println(empinfoData.count());
        System.out.println(empInfoFormatData.count());
        System.out.println("==============================>>>empInfoFormatData Source End<<<==============================");
        JavaRDD<EhrOdsJobDirectory> jDirectoryFormatData = jobDirectoryData.map(new Function<Result, EhrOdsJobDirectory>() {
            @Override
            public EhrOdsJobDirectory call(Result result) throws Exception {
                final String family = "DPM_ODS_PERSONNEL_HR_JOB_DIRECTORY";
                String JobCoding = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("job_coding"))).trim();
                String JobName = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("job_name"))).trim();
                String Process = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("process_code"))).trim();
                String DirectClassification = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("direct_classification"))).trim();
                String JobType = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("job_type"))).trim();
                return new EhrOdsJobDirectory(JobCoding, JobName, Process, DirectClassification, JobType);
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrOdsJobDirectory> take4 = jDirectoryFormatData.take(5);
        for (EhrOdsJobDirectory ehrOdsJobDirectory : take4) {
            System.out.println(ehrOdsJobDirectory);
        }
        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>jDirectoryFormatData Source End<<<==============================");
        JavaRDD<EhrEmpOdsOrg> empOrgFormatData = empOrgData.map(new Function<Result, EhrEmpOdsOrg>() {
            @Override
            public EhrEmpOdsOrg call(Result result) throws Exception {
                final String family = "DPM_ODS_HR_DL_EMP_DEPT";
                String emp_id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("emp_id"))).trim();
                String sbg_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("sbg_code"))).trim();
                String l1 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l1"))).trim();
                String l2 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l2"))).trim();
                String l3 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l3"))).trim();
                String l4 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l4"))).trim();
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
                String l5 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("l5"))).trim();
                String group_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("group_code"))).trim();
                String department_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("department_code"))).trim();
                Long update_dt = 0L;
                try {
                    update_dt = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_dt"))));
                } catch (Exception e) {
                }
                return new EhrEmpOdsOrg(emp_id, sbg_code, l1, l2, l3, l4, l5, group_code, department_code, update_dt);
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrEmpOdsOrg> take5 = empOrgFormatData.take(5);
        for (EhrEmpOdsOrg ehrEmpOdsOrg : take5) {
            System.out.println(ehrEmpOdsOrg);
        }
        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>empOrgFormatData Source End<<<==============================");
        JavaRDD<EhrOdsOrg> orgFormatData = orgData.map(new Function<Result, EhrOdsOrg>() {
            @Override
            public EhrOdsOrg call(Result result) throws Exception {
                final String family = "DPM_ODS_HR_HR_ORG";
                String bg = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("bg")));
                String plant = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("plant")));
                String dept = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("dept")));
                String bu = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("bu")));
                String org_fee_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_fee_code")));
                String org_name = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_name")));
                String org_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("org_code")));
                return new EhrOdsOrg(bg, plant, dept, bu, org_fee_code, org_name, org_code);
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrOdsOrg> take6 = orgFormatData.take(5);
        for (EhrOdsOrg ehrOdsOrg : take6) {
            System.out.println(ehrOdsOrg);
        }
        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>orgFormatData Source End<<<==============================");

        JavaRDD<EhrOdsMbu> mbuFormatData = mbuData.map((Function<Result, EhrOdsMbu>) result -> {
            final String family = "DPM_ODS_PERSONNEL_HR_MBU_DEFINITION";
            String Department = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("department"))).trim();
            String Product = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("product"))).trim();
            String FunctionDepartment = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("function_department"))).trim();
            String LevelFunctionDepartment = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("level_code"))).trim();
            String Site = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("site_code"))).trim();
            String factoryCode = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("factory_code")));

            return new EhrOdsMbu(Department, Product, FunctionDepartment, LevelFunctionDepartment, Site, factoryCode);

        }).persist(StorageLevel.MEMORY_AND_DISK());
        List<EhrOdsMbu> takeMbu = mbuFormatData.take(5);
        for (EhrOdsMbu ehrodsmbu : takeMbu) {
            System.out.println(ehrodsmbu);
        }

        System.out.println("==============================>>>mbuFormatData Source End<<<==============================");

        /**
         * ===========================================================================
         * 清洗：按工号分组，得到基本数据（除HumresourceCode人力类别，WorkoverTimeHours加班时长，LeaveHours请假时长,PersonPost）
         * ===========================================================================
         */
        JavaPairRDD<String, DwdEhrOutput> mainRddPartA = kaoQinFormatData.mapToPair(new PairFunction<EhrOdsKaoqin, String, DwdEhrOutput>() {
            @Override
            public Tuple2<String, DwdEhrOutput> call(EhrOdsKaoqin ehrOdsKaoqin) throws Exception {
                DwdEhrOutput output = new DwdEhrOutput();
                //判定考勤日期
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
                String workDT = "";
                try {
                    workDT = outputFormat.format(inputFormat.parse(ehrOdsKaoqin.getKq_date()));
                } catch (ParseException e) {
                    //ignore
                }
                output.setWorkDT(workDT);
                output.setEmpID(ehrOdsKaoqin.getEmp_id());
                //
                if (StringUtils.isNotEmpty(ehrOdsKaoqin.getUss_start_time()) && StringUtils.isNotEmpty(ehrOdsKaoqin.getDs_end_time())) {
                    Calendar ca = Calendar.getInstance();
                    Date workStartTime = inputFormat.parse(ehrOdsKaoqin.getUss_start_time());
                    ca.setTime(workStartTime);
                    int startTimeDay = ca.get(Calendar.DAY_OF_MONTH);
                    Date workEndTime = inputFormat.parse(ehrOdsKaoqin.getDs_end_time());
                    ca.setTime(workEndTime);
                    int endTimeDay = ca.get(Calendar.DAY_OF_MONTH);
                    if (startTimeDay != endTimeDay) {
                        output.setWorkShifitClass("N");
                    } else {
                        output.setWorkShifitClass("D");
                    }
                } else {
                    // done cj 特殊情况默认休息
                    output.setWorkShifitClass("O");
                }
                //判断上班时间AttendanceWorkhours
                /* 判断这些字段是否为0 全为为0 正常上班
                 * late_times
                 * leave_early_times
                 * absent_times
                 * ot_late_times
                 * ot_leave_early_times
                 * ot_absent_times
                 */

                if (StringUtils.isNotEmpty(ehrOdsKaoqin.getUss_start_time())) {
                    output.setAttendanceWorkhours("8");
                } else {
                    //逻辑改变   先注释万一又变回来了呢？
//                    if("0".equals(ehrOdsKaoqin.getLate_times()) && "0".equals(ehrOdsKaoqin.getLeave_early_times())
//                            && "0".equals(ehrOdsKaoqin.getOt_absent_times()) && "0".equals(ehrOdsKaoqin.getAbsent_times())
//                            && "0".equals(ehrOdsKaoqin.getOt_late_times()) && "0".equals(ehrOdsKaoqin.getOt_leave_early_times())){
//                        output.setAttendanceWorkhours("8");
//                    }else {
//                        output.setAttendanceWorkhours("0");
//                    }
                    //新逻辑
                    output.setAttendanceWorkhours("0");
                }
                /**
                 * 判断请假时间，
                 * 如果当天无实际打卡信息，
                 * 即上段應上班時間有值，但上段实际上班时间和下段实际下班时间无值，则判断当天是跨天情况的全天请假
                 * 当天的请假时间直接置为8，
                 * 因为跨天请假，只有在请假当天会有一条请假记录。其他时间无请假记录。
                 */

                if (StringUtils.isNotEmpty(ehrOdsKaoqin.getUss_start_time())
                        && StringUtils.isBlank(ehrOdsKaoqin.getUr_start_time())
                        && StringUtils.isBlank(ehrOdsKaoqin.getDr_end_time())) {
                    output.setLeaveHours("8");
                }
                /**
                 * 判断有无打卡记录
                 */
                if (StringUtils.isNotEmpty(ehrOdsKaoqin.getUr_start_time())) {
                    output.setOnduty_states("1");
                } else {
                    output.setOnduty_states("0");
                }
                return new Tuple2<String, DwdEhrOutput>(ehrOdsKaoqin.getEmp_id(), output);
            }
        });
        try {
            List<Tuple2<String, DwdEhrOutput>> take7 = mainRddPartA.take(5);
            for (Tuple2<String, DwdEhrOutput> stringDwdEhrOutputTuple2 : take7) {
                System.out.println(stringDwdEhrOutputTuple2);
            }
        } catch (Exception e) {

        }
        System.out.println("----------------7-----------------");

        System.out.println(mainRddPartA.count());
        System.out.println("==============================>>>mainRddPartA Calculate End<<<==============================");

        /**
         * ===========================================================================
         * 计算：按PersonPost分组，得到empinfo数据
         * 与job_directory匹配得到JobCoding字段
         * 下面注释代码为计算人力类型的代码，因为逻辑改变遗弃，可以参考
         * ===========================================================================
         */


        System.out.println("==============================>>>dimensionRddC Calculate End<<<==============================");
        /**
         * ===========================================================================
         * 合并：将人力类别humanresourcecode赋值到mainRddA中
         * 下面代码为计算人力类型的新代码代码 ， 需要了解
         * ===========================================================================
         */
        mainRddPartA = mainRddPartA.cogroup(empInfoFormatData.keyBy(r -> r.getEmp_id())).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsEmpinfo>>>, String, DwdEhrOutput>() {
            @Override
            public Iterator<Tuple2<String, DwdEhrOutput>> call(Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsEmpinfo>>> tuple2) throws Exception {
                Iterable<DwdEhrOutput> mainRddPartAIterable = tuple2._2()._1();
                Iterable<EhrOdsEmpinfo> mainRddPartATempIterable = tuple2._2()._2();
                List<Tuple2<String, DwdEhrOutput>> data = new ArrayList<>();
                mainRddPartAIterable.forEach(main -> {
                    mainRddPartATempIterable.forEach(temp -> {
                        main.setHumresourceCode(temp.getPositiontype());
                    });
                    data.add(new Tuple2<>(main.getEmpID(), main));
                });
                return data.iterator();
            }
        });


        try {
            List<Tuple2<String, DwdEhrOutput>> take10 = mainRddPartA.take(5);
            if (take10 != null) {
                for (Tuple2<String, DwdEhrOutput> stringDwdEhrOutputTuple2 : take10) {
                    System.out.println(stringDwdEhrOutputTuple2);
                }

            } else {
                System.out.println("====================>>>>>>NO DATA<<<<<<=====================");
            }
        } catch (Exception e) {
            System.out.println("====================>>>>>>NO DATA<<<<<<=====================");
        }
        System.out.println(mainRddPartA.count());
        List<Tuple2<String, DwdEhrOutput>> takeMainRdd2 = mainRddPartA.take(10);
        for (Tuple2<String, DwdEhrOutput> stringEhrOdsEmpinfoTuple2 : takeMainRdd2) {
            System.out.println(stringEhrOdsEmpinfoTuple2);
        }
        System.out.println("==============================>>>before emporg mainRddPartA Calculate End<<<==============================");

        System.out.println("---------------------------");

        /**
         * ===========================================================================
         * 清洗：按工号分组，得到org数据
         * ===========================================================================
         */
        JavaPairRDD dimensionRddD = empOrgFormatData.keyBy(empOrg -> empOrg.getEmp_id()).coalesce(20, false).reduceByKey((v1, v2) -> v1.getUpdate_dt() > v2.getUpdate_dt() ? v1 : v2);
        try {
            List<Tuple2<String, EhrEmpOdsOrg>> take11 = dimensionRddD.take(10);
            for (Tuple2<String, EhrEmpOdsOrg> stringDwdEhrOutputTuple2 : take11) {
                System.out.println(stringDwdEhrOutputTuple2);
            }
        } catch (Exception e) {
            System.out.println("====================>>>>>>dimensionRddD NO DATA<<<<<<=====================");
        }
        System.out.println("=====emporg for empid======" + dimensionRddD.count());
        /**
         * ===========================================================================
         * 合并：将org数据合并到mainRddA中，赋值组织机构信息，（不包含厂区字段）
         * ===========================================================================
         */
        mainRddPartA = mainRddPartA.cogroup(dimensionRddD).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrEmpOdsOrg>>>, String, DwdEhrOutput>() {
            @Override
            public Iterator<Tuple2<String, DwdEhrOutput>> call(Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrEmpOdsOrg>>> tuple2) throws Exception {
                Iterable<DwdEhrOutput> mainIterable = tuple2._2()._1();
                Iterable<EhrEmpOdsOrg> orgsIterable = tuple2._2()._2();
                List<Tuple2<String, DwdEhrOutput>> data = new ArrayList<>();
                mainIterable.forEach(main -> {
                    orgsIterable.forEach(org -> {
                        main.setBG(org.getSbg_code());
                        main.setSBG(org.getL1());
                        main.setBU(org.getL2());
                        main.setMBU(org.getL3());
                        main.setDepartment(org.getL4());
                        main.setSubDepartment(org.getL5());
                        main.setGroupCode(org.getGroup_code());
                        main.setDepartmentCode(org.getDepartment_code());
                    });
                    data.add(new Tuple2<>(main.getEmpID(), main));
                });
                return data.iterator();
            }
        });

        System.out.println("-------------4--------------");

        try {
            List<Tuple2<String, DwdEhrOutput>> take11 = mainRddPartA.take(10);
            for (Tuple2<String, DwdEhrOutput> stringDwdEhrOutputTuple2 : take11) {
                System.out.println(stringDwdEhrOutputTuple2);
            }
        } catch (Exception e) {
            System.out.println("====================>>>>>>dimensionRddD NO DATA<<<<<<=====================");
        }

        System.out.println("==============================>>>dimensionRddD Calculate End<<<==============================");

        /**
         * 赋值level_code字段,用MBU字段与MBU数据中得FunctionDepartement字段关联得处LevelFunctionDepartment作为level_code
         *
         *
         *
         */
        mainRddPartA = mainRddPartA.map(t -> t._2()).keyBy(r -> r.getMBU().trim()).cogroup(mbuFormatData.keyBy(r -> r.getFunctionDepartment().trim())).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsMbu>>>, String, DwdEhrOutput>() {
            @Override
            public Iterator<Tuple2<String, DwdEhrOutput>> call(Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsMbu>>> tuple2) throws Exception {
                Iterable<DwdEhrOutput> mainIterable = tuple2._2()._1();
                Iterable<EhrOdsMbu> mbuIterable = tuple2._2()._2();
                List<Tuple2<String, DwdEhrOutput>> data = new ArrayList<>();
                mainIterable.forEach(main -> {
                    mbuIterable.forEach(mbu -> {
                        main.setLevelCode(mbu.getLevelFunctionDepartment());
                        //main.setFactoryCode(mbu.getFactory_code());
                    });
                    data.add(new Tuple2<>(main.getEmpID(), main));
                });
                return data.iterator();
            }
        });

        /**
         *用Departement的关联 MBU数据中 Departement
         */
        mainRddPartA = mainRddPartA.map(t -> t._2()).keyBy(r -> r.getDepartment().trim()).cogroup(mbuFormatData.keyBy(r -> r.getDepartment().trim())).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsMbu>>>, String, DwdEhrOutput>() {
            @Override
            public Iterator<Tuple2<String, DwdEhrOutput>> call(Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsMbu>>> tuple2) throws Exception {
                Iterable<DwdEhrOutput> mainIterable = tuple2._2()._1();
                Iterable<EhrOdsMbu> mbuIterable = tuple2._2()._2();
                List<Tuple2<String, DwdEhrOutput>> data = new ArrayList<>();
                mainIterable.forEach(main -> {
                    mbuIterable.forEach(mbu -> {
                        main.setFactoryCode(mbu.getFactory_code());
                    });
                    data.add(new Tuple2<>(main.getEmpID(), main));
                });
                return data.iterator();
            }
        });

        System.out.println(mbuFormatData.keyBy(r -> r.getFunctionDepartment()).count());
        System.out.println(mainRddPartA.count());
        System.out.println("==============================>>>level_code Calculate End<<<==============================");


        /**
         * ===========================================================================
         * 赋值厂区字段： 修改关联逻辑，用l4关联site_code
         * 1.将orgformatdata 按org_code分组，得到org数据
         * 2.将empinfo 按org_code分组，
         * 3.合并上诉数据，得到厂区字段信息
         * 4.返回按工号分组的只含有厂区字段的output对象
         * 5.将包含厂区字段的output与mainRDD合并
         * ===========================================================================
         */

        System.out.println(mainRddPartA.count());
        System.out.println("==============================>>>mainRddATemp2 Calculate End<<<==============================");
        /**
         * ===========================================================================
         * 赋值加班数据：
         * 1.将mainRDD数据按工号+","+时间分组
         * 2.将jiabanRdd按工号+","+时间分组
         * 3.合并上诉数据，将加班信息赋值到mainrdd中
         * ===========================================================================
         */
        JavaPairRDD factRddA = jiabanFormatData.mapToPair(new PairFunction<EhrOdsJiaban, String, DwdEhrOutput>() {
            @Override
            public Tuple2<String, DwdEhrOutput> call(EhrOdsJiaban jiabanInfo) throws Exception {
                DwdEhrOutput output = new DwdEhrOutput();
                output.setWorkoverTimeHours(jiabanInfo.getOt_hours());
                output.setEmpID(jiabanInfo.getEmp_id());
                output.setWorkDT(jiabanInfo.getOt_date());
                return new Tuple2<>(jiabanInfo.getEmp_id() + "," + jiabanInfo.getOt_date(), output);
            }
        });
        mainRddPartA = mainRddPartA.mapToPair(new PairFunction<Tuple2<String, DwdEhrOutput>, String, DwdEhrOutput>() {
            @Override
            public Tuple2<String, DwdEhrOutput> call(Tuple2<String, DwdEhrOutput> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1() + "," + tuple2._2().getWorkDT(), tuple2._2());
            }
        });
        mainRddPartA = mainRddPartA.cogroup(factRddA).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<DwdEhrOutput>>>, String, DwdEhrOutput>() {
            @Override
            public Iterator<Tuple2<String, DwdEhrOutput>> call(Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<DwdEhrOutput>>> tuple2) throws Exception {
                Iterable<DwdEhrOutput> mainIterable = tuple2._2()._1();
                Iterable<DwdEhrOutput> jiabanIterable = tuple2._2()._2();
                List<Tuple2<String, DwdEhrOutput>> data = new ArrayList<>();
                mainIterable.forEach(main -> {
                    jiabanIterable.forEach(jiaban -> {
                        main.setWorkoverTimeHours(jiaban.getWorkoverTimeHours());
                    });
                    data.add(new Tuple2<>(tuple2._1(), main));
                });
                return data.iterator();
            }
        });

        System.out.println("-------------1--------------");

        try {
            List<Tuple2<String, DwdEhrOutput>> take14 = mainRddPartA.take(5);
            for (Tuple2<String, DwdEhrOutput> stringDwdEhrOutputTuple2 : take14) {
                System.out.println(stringDwdEhrOutputTuple2);
            }
        } catch (Exception e) {

        }
        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>factRddA Calculate End<<<==============================");
        /**
         * ===========================================================================
         * 赋值请假数据：
         * 1.将qingjiadata按工号+时间分组
         * 2.将考勤数据按工号+时间分组
         * 3.合并上诉数据，得到按工号+时间分组的包含当天请假时长的数据
         * ===========================================================================
         */
        JavaPairRDD qjDataGroupByEmpAndTime = qingjiaFormatData.mapToPair(new PairFunction<EhrOdsQingjia, String, EhrOdsQingjia>() {
            @Override
            public Tuple2<String, EhrOdsQingjia> call(EhrOdsQingjia ehrOdsQingjia) throws Exception {
                return new Tuple2<>(ehrOdsQingjia.getEmp_id() + "," + ehrOdsQingjia.getStart_date(), ehrOdsQingjia);
            }
        });
        JavaPairRDD kaoqGroupByEmpAndTime = kaoQinFormatData.mapToPair(new PairFunction<EhrOdsKaoqin, String, EhrOdsKaoqin>() {
            @Override
            public Tuple2<String, EhrOdsKaoqin> call(EhrOdsKaoqin ehrOdsKaoqin) throws Exception {
                //判定考勤日期
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
                String workDT = "";
                try {
                    workDT = outputFormat.format(inputFormat.parse(ehrOdsKaoqin.getKq_date()));
                } catch (Exception e) {
                    //ignore
                }
                return new Tuple2<>(ehrOdsKaoqin.getEmp_id() + "," + workDT, ehrOdsKaoqin);
            }
        });
        //合并信息
        JavaPairRDD qijiaByDayData = qjDataGroupByEmpAndTime.cogroup(kaoqGroupByEmpAndTime).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<EhrOdsQingjia>, Iterable<EhrOdsKaoqin>>>, String, EhrOdsQingjiaByDay>() {
            @Override
            public Iterator<Tuple2<String, EhrOdsQingjiaByDay>> call(Tuple2<String, Tuple2<Iterable<EhrOdsQingjia>, Iterable<EhrOdsKaoqin>>> tuple2) throws Exception {
                Iterable<EhrOdsQingjia> qjIterable = tuple2._2()._1();
                Iterable<EhrOdsKaoqin> kqIterable = tuple2._2()._2();
                List<Tuple2<String, EhrOdsQingjiaByDay>> data = new ArrayList<>();
                qjIterable.forEach(qj -> {
                    //判断是否跨天
                    if (qj.getStart_date().equals(qj.getEnd_date())) {
                        //没跨天，就直接取当天请假时长即可
                        EhrOdsQingjiaByDay output = new EhrOdsQingjiaByDay();
                        output.setEmpId(qj.getEmp_id());
                        output.setWorkDate(qj.getStart_date());
                        output.setLeaveHours(qj.getTotal_hours());
                        data.add(new Tuple2<>(qj.getEmp_id() + "," + qj.getStart_date(), output));
                    } else {
                        //跨天需要分拆为多条数据
                        try {
                            SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
                            SimpleDateFormat timeFormat = new SimpleDateFormat("HHmm");
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
                            Calendar calendar = Calendar.getInstance();
                            Date startDate = inputFormat.parse(qj.getStart_date());
                            Date endDate = inputFormat.parse(qj.getEnd_date());
                            //处理第一天和最后一天的请假数据（因为不一定是8小时）
                            EhrOdsQingjiaByDay firstDayData = new EhrOdsQingjiaByDay();
                            Date finalStartDate = startDate;
                            kqIterable.forEach(kq -> {
                                try {
                                    //找到请假当天的考勤数据
                                    if (inputFormat.parse(kq.getUss_start_time()).getTime() == finalStartDate.getTime()) {
                                        //请假开始时间减去考勤开始时间即为请假第一天的请假时长
                                        String firstDayQjTime = String.valueOf(8 - (Integer.valueOf(qj.getStart_time()) - Integer.valueOf(timeFormat.format(dateFormat.parse(kq.getUss_start_time())))) / 100);
                                        firstDayData.setLeaveHours(firstDayQjTime);
                                    }
                                } catch (Exception e) {

                                }
                            });
                            //没找到对应考勤数据则默认取8小时
                            if (StringUtils.isBlank(firstDayData.getLeaveHours())) {
                                firstDayData.setLeaveHours("8");
                            }
                            firstDayData.setWorkDate(qj.getStart_date());
                            firstDayData.setEmpId(qj.getEmp_id());
                            data.add(new Tuple2<>(firstDayData.getEmpId() + "," + firstDayData.getWorkDate(), firstDayData));

                            //处理最后一天的数据
                            EhrOdsQingjiaByDay lastDayData = new EhrOdsQingjiaByDay();
                            Date finalEndDate = endDate;
                            kqIterable.forEach(kq -> {
                                try {
                                    //找到请假最后一天的考勤数据
                                    if (inputFormat.parse(kq.getDs_end_time()).getTime() == finalEndDate.getTime()) {
                                        //8-考勤结束时间减去请假结束时间即为请假最后一天的请假时长
                                        String lastDayQjTime = String.valueOf(8 - (Integer.valueOf(timeFormat.format(dateFormat.parse(kq.getDs_end_time()))) - Integer.valueOf(qj.getEnd_time())) / 100);
                                        lastDayData.setLeaveHours(lastDayQjTime);
                                    }
                                } catch (Exception e) {

                                }
                            });
                            //没找到对应考勤数据则默认取8小时
                            if (StringUtils.isBlank(lastDayData.getLeaveHours())) {
                                lastDayData.setLeaveHours("8");
                            }
                            lastDayData.setWorkDate(qj.getEnd_date());
                            lastDayData.setEmpId(qj.getEmp_id());
                            data.add(new Tuple2<>(lastDayData.getEmpId() + "," + lastDayData.getWorkDate(), lastDayData));
                            //循环除第一天和最后一天的每一天，时长固定8小时
                            calendar.setTime(startDate);
                            calendar.add(Calendar.DAY_OF_YEAR, 1);
                            startDate = calendar.getTime();
                            calendar.setTime(endDate);
                            calendar.add(Calendar.DAY_OF_YEAR, -1);
                            endDate = calendar.getTime();
                            Calendar tag = Calendar.getInstance();
                            tag.setTime(startDate);
                            int counter = 0; //计数器，大于31就停止
                            while (tag.getTime().before(endDate)) {
                                if (counter > 31) {
                                    break;
                                }
                                EhrOdsQingjiaByDay dayData = new EhrOdsQingjiaByDay();
                                dayData.setEmpId(qj.getEmp_id());
                                dayData.setWorkDate(inputFormat.format(tag.getTime()));
                                dayData.setLeaveHours("8");
                                data.add(new Tuple2<>(dayData.getEmpId() + "," + dayData.getWorkDate(), dayData));
                                tag.add(Calendar.DAY_OF_YEAR, 1); //todo 跨年问题
                                counter++;
                            }
                        } catch (Exception e) {

                            //ignore
                        }
                    }
                });
                return data.iterator();
            }
        });

        /**
         * ===========================================================================
         * 合并请假信息到主信息中
         * 1.合并主数据（已按工号+时间分组）与请假数据（已按工号+时间分组），得到包含每天请假时长的完整信息
         * ===========================================================================
         */
        mainRddPartA = mainRddPartA.cogroup(qijiaByDayData).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsQingjiaByDay>>>, String, DwdEhrOutput>() {
            @Override
            public Iterator<Tuple2<String, DwdEhrOutput>> call(Tuple2<String, Tuple2<Iterable<DwdEhrOutput>, Iterable<EhrOdsQingjiaByDay>>> tuple2) throws Exception {
                Iterable<DwdEhrOutput> mainIterable = tuple2._2()._1();
                Iterable<EhrOdsQingjiaByDay> qjIterable = tuple2._2()._2();
                List<Tuple2<String, DwdEhrOutput>> data = new ArrayList<>();
                mainIterable.forEach(main -> {
                    qjIterable.forEach(qj -> {
                        main.setLeaveHours(qj.getLeaveHours());
                    });
                    data.add(new Tuple2<>(tuple2._1(), main));
                });
                return data.iterator();
            }
        });

        System.out.println("-----------end----------------");

        try {
            List<Tuple2<String, DwdEhrOutput>> take13 = mainRddPartA.take(5);
            for (Tuple2<String, DwdEhrOutput> stringDwdEhrOutputTuple2 : take13) {
                System.out.println(stringDwdEhrOutputTuple2);
            }
        } catch (Exception e) {

        }
//        持久化HDFS  查看测试数据使用
//        JavaRDD<String> map112 = mainRddPartA.map(r -> {
//
//            return r._2().toString();
//        });
//        map112.coalesce(1, true).saveAsTextFile("/dpuserdata/41736e50-883d-42e0-a484-0633759b92/chenjian/test");


        JavaRDD<Tuple2<String, DwdEhrOutput>> mainRddPartAEnd = mainRddPartA.map(r -> {
            String department = r._2.getDepartment();
            if ("L5".equals(r._2.getLevelCode())) {
                if (department != null) {
                    //衝壓 Stamping/成型 Molding/塗裝 Painting/組裝 Ass'y
                    if (department.contains("衝壓")) {
                        r._2.setProcessCode("Stamping");
                    } else if (department.contains("成型")) {
                        r._2.setProcessCode("Molding");
                    } else if (department.contains("塗裝")) {
                        r._2.setProcessCode("Painting");
                    } else if (department.contains("組裝")) {
                        r._2.setProcessCode("Assy");
                    } else {
                        r._2.setDepartmentCode("");
                    }
                } else {
                    r._2.setDepartmentCode("");
                }
            }
            String fengli="\\((.*?)\\)";			//提取括号里面内容的正则表达式
            Pattern pafengli=Pattern.compile(fengli);
            Matcher matfengli = pafengli.matcher(department);
            if(matfengli.find())
            {
                String site_code = matfengli.group(1);  //group为捕获组
                switch (site_code) {
                    case "重慶":
                        r._2.setSiteCode("CQ");
                        break;
                    case "武漢":
                        r._2.setSiteCode("WH");
                        break;
                    case "龍華":
                        r._2.setSiteCode("LH");
                        break;
                    case "煙臺":
                        r._2.setSiteCode("YT");
                        break;
                    case "福田":
                        r._2.setSiteCode("FT");
                        break;
                    case "昆山":
                        r._2.setSiteCode("KS");
                        break;
                    case "成都":
                        r._2.setSiteCode("CD");
                        break;
                    case "衡陽":
                        r._2.setSiteCode("HY");
                        break;
                    case "鄭州":
                        r._2.setSiteCode("ZZ");
                        break;
                    case "長沙":
                        r._2.setSiteCode("CS");
                        break;
                    case "太原":
                        r._2.setSiteCode("TY");
                        break;
                    case "晉城":
                        r._2.setSiteCode("JC");
                        break;
                    default:
                        if(r._2.getMBU().equals("DTSA/MBU/MMD") || r._2.getMBU().equals("DTSA/MBU/EMD") || r._2.getMBU().equals("DTSA/MBU/SMD")){
                            r._2.setSiteCode("WH");
                        }else {
                            r._2.setSiteCode("LH");
                        }
                        break;
                }
            }
            else {
                if(r._2.getMBU().equals("DTSA/MBU/MMD") || r._2.getMBU().equals("DTSA/MBU/EMD") || r._2.getMBU().equals("DTSA/MBU/SMD")){
                    r._2.setSiteCode("WH");
                }else {
                    r._2.setSiteCode("LH");
                }
            }
            return r;
        });

        mainRddPartAEnd.take(5).forEach(r -> System.out.println(r));
        System.out.println("=============l5prossess==================");
        mainRddPartAEnd.filter(r -> "L5".equals(r._2.getLevelCode())).take(5).forEach(r -> System.out.println(r));

        System.out.println(mainRddPartAEnd.count());
        System.out.println("==============================>>>qijiaByDayData Calculate End<<<==============================");
        //数据准备完毕，转换为PUT对象插入到hbase
        JavaRDD toWriteData = mainRddPartAEnd.mapPartitions(it -> {
            SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
            final String family = "DPM_DWD_PERSONNEL_EMP_WORKHOURS";
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
            final Long updateDt = System.currentTimeMillis();
            final String dataFrom = "ehr-ods";
            final String updateBy = "system";
            ArrayList<Put> puts = new ArrayList<>();
            while (it.hasNext()) {
                try {
                    DwdEhrOutput output = it.next()._2();
                    //已按照工号+时间分组，直接取value转换为PUT对象
                    String rowkey = String.valueOf(formatWorkDt.parse(output.getWorkDT()).getTime()) + ":" + output.getSiteCode()  + ":" + output.getEmpID() ;
                    rowkey = consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("site_code"), Bytes.toBytes(output.getSiteCode()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("level_code"), Bytes.toBytes(output.getLevelCode()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("bg"), Bytes.toBytes(output.getBG()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sbg"), Bytes.toBytes(output.getSBG() == null ? "" : output.getSBG()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("bu"), Bytes.toBytes(output.getBU()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mbu"), Bytes.toBytes(output.getMBU() == null ? "" : output.getMBU()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("department"), Bytes.toBytes(output.getDepartment() == null ? "" : output.getDepartment()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("team"), Bytes.toBytes(output.getSubDepartment() == null ? "" : output.getSubDepartment()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("department_code"), Bytes.toBytes(output.getDepartmentCode() == null ? "" : output.getDepartmentCode()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("group_code"), Bytes.toBytes(output.getGroupCode() == null ? "" : output.getGroupCode()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("work_dt"), Bytes.toBytes(output.getWorkDT() == null ? "" : output.getWorkDT()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("work_shift"), Bytes.toBytes(output.getWorkShifitClass() == null ? "" : output.getWorkShifitClass()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("humresource_type"), Bytes.toBytes(output.getHumresourceCode() == null ? "" : output.getHumresourceCode()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("emp_id"), Bytes.toBytes(output.getEmpID()));
                    //=====Float类型处理，如果转换异常说明脏数据
                    String attendanceWorkhours = StringUtils.isNotEmpty(output.getAttendanceWorkhours()) ? output.getAttendanceWorkhours() : "0f";
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("attendance_workhours"), Bytes.toBytes(Float.valueOf(attendanceWorkhours).toString()));
                    //=============end====================

                    //=====Float类型处理，如果转换异常说明脏数据
                    String workoverTimeHours = StringUtils.isNotEmpty(output.getWorkoverTimeHours()) ? output.getWorkoverTimeHours() : "0f";
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("overtime_hours"), Bytes.toBytes(Float.valueOf(workoverTimeHours).toString()));
                    //====================================
                    //=====Float类型处理，如果转换异常说明脏数据
                    String leaveHours = StringUtils.isNotEmpty(output.getLeaveHours()) ? output.getLeaveHours() : "0f";
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("leave_hours"), Bytes.toBytes(Float.valueOf(leaveHours).toString()));
                    //=============end====================
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("onduty_states"), Bytes.toBytes(output.getOnduty_states()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("factory_code"), Bytes.toBytes(output.getFactoryCode()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("process_code"), Bytes.toBytes(output.getProcessCode()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_dt"), Bytes.toBytes(updateDt.toString()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_by"), Bytes.toBytes(updateBy));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("data_from"), Bytes.toBytes(dataFrom));
                    puts.add(put);
                } catch (Exception e) {
                    System.out.println("数据异常,必要参数缺失:" + e.getMessage());
                }
            }
            return puts.iterator();

        });

        try {
            System.out.println(toWriteData.count());
            List<Put> take15 = toWriteData.take(5);
            for (Put put : take15) {
                System.out.println(put);
            }
        } catch (Exception e) {

        }

        System.out.println(simpleDateFormat.format(System.currentTimeMillis()));
        System.out.println("==============================>>>toWriteData Calculate End<<<==============================");
        try {
            DPHbase.rddWrite("dpm_dwd_personnel_emp_workhours", toWriteData);
            System.out.println("写入完毕");
        } catch (Exception e) {
            System.out.println("===============================>>>>>>>>>>>>>>>>>>>Write No Data Or API Err<<<<<<<<<<<<<<<<<<<<====================");
        }


        System.out.println("==============================>>>Programe End<<<==============================");
    }

    private void calculateBackDataAttendanceInfo(Map<String, Object> map) throws Exception {

        String back_month_day = (String) map.get("BACK_MONTH_DAY");
        if (back_month_day != null) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Calendar instance = Calendar.getInstance();
            instance.setTime(simpleDateFormat.parse(back_month_day));
            instance.set(Calendar.HOUR_OF_DAY, 0);
            instance.set(Calendar.MINUTE, 0);
            instance.set(Calendar.SECOND, 0);
            long startTimestamp = instance.getTime().getTime();
            instance.add(Calendar.DAY_OF_YEAR, 1);
            long endTimestamp = instance.getTime().getTime();
            calculateDayAttendanceInfo(String.valueOf(startTimestamp), String.valueOf(endTimestamp));
        }


    }

    public void calculateTwosDayAttendanceInfo() throws Exception {

        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date());
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        instance.add(Calendar.DAY_OF_YEAR, 1);
        String endTimestamp = String.valueOf(instance.getTime().getTime());
        instance.add(Calendar.DAY_OF_YEAR, -2);
        String startTimestamp = String.valueOf(instance.getTime().getTime());

        calculateDayAttendanceInfo(startTimestamp, endTimestamp);
    }

    public void calculateDayAttendanceInfo(String startTimestamp, String endTimestamp) throws Exception {

    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
