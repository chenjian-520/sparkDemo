package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.PersonnelBean;
import com.foxconn.dpm.sprint1_2.dwd_dws.beans.TurnoverBean;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.ScanTableDto;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.catalyst.expressions.In;
import scala.*;

import java.lang.Long;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description:
 * 离职率计算，
 * 输入表：抽取dwd表 dpm_dwd_personnel_emp_workhours 获取在职人员总数。 dpm_dwd_personnel_separation_day    获取离职人员总数
 * 输出表：放入dws表 dpm_dws_personnel_overview_dd 人力概要状态表(出勤率和离职率)
 * 计算逻辑为：统计应考勤人事，考勤人事，离职人数。
 *
 * @author FL cj
 * @version 1.0
 * @timestamp 2020/1/10
 */
public class TurnoverCalculate extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter metaGetter = MetaGetter.getSql();

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
         * ==================================================================
         * 获取数据：
         * 1.dpm_dwd_personnel_emp_workhours 获取在职人员总数
         * 2.dpm_dwd_personnel_separation_day    获取离职人员总数
         * ==================================================================
         */

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(yesterdayStamp));
        scan.withStopRow(Bytes.toBytes(todayStamp));

        JavaRDD<PersonnelBean> PersonnelRDD = DPHbase.rddRead("dpm_dwd_personnel_separation", scan, true).map(r -> {
            return new PersonnelBean(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("level_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("factory_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("process_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("emp_id"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("humresource_type"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("time_of_separation"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_SEPARATION"), Bytes.toBytes("update_dt")))
            );
        }).keyBy(r -> r.getEmp_id()).coalesce(10, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return Long.valueOf(v1.getUpdate_dt()) >= Long.valueOf(v2.getUpdate_dt()) ? v1 : v2;
        }).map(r -> {
            //还原RDD
            return r._2;
        });

        Dataset<Row> dataFrame1 = DPSparkApp.getSession().createDataFrame(PersonnelRDD, PersonnelBean.class);
        dataFrame1.show();
        dataFrame1.createOrReplaceTempView("empTurnover");


        JavaRDD<TurnoverBean> TurnoverJavaPairRDD = DPHbase.saltRddRead("dpm_dwd_personnel_emp_workhours", yesterdayStamp, todayStamp, new Scan(), true).map(r -> {
            return new Tuple10<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("level_code"))),
                    toFactoryCode(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("factory_code")))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("process_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("humresource_type"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("work_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("emp_id"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("onduty_states"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("update_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PERSONNEL_EMP_WORKHOURS"), Bytes.toBytes("work_shift")))
            );
        }).keyBy(r -> {
            return new Tuple2<>(r._7(), r._6());
        }).coalesce(10, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return Long.valueOf(v1._9()) >= Long.valueOf(v2._9()) ? v1 : v2;
        }).map(r -> {
            //还原RDD
            return new TurnoverBean(r._2._1(), r._2._2(), r._2._3(), r._2._4(), r._2._5(), r._2._6(), r._2._7(), r._2._8(), r._2._10());
        });

        Dataset<Row> dataFrame = DPSparkApp.getSession().createDataFrame(TurnoverJavaPairRDD, TurnoverBean.class);
        System.out.println("***" + dataFrame.count() + "***");
        dataFrame.createTempView("empWorkHours");
        DPSparkApp.getSession().sql("select * from empWorkHours").show();
        DPSparkApp.getSession().sql("select count(*) from empTurnover ").show();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String todayFormatStr = simpleDateFormat.format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(0))));
        String str = todayFormatStr + " 20:02:00.000";

        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long time = simpleDateFormat1.parse(str).getTime();

        Dataset<Row> dateset = null;
        if (System.currentTimeMillis() >= time) {
            System.out.println("晚上考勤");
            dateset = DPSparkApp.getSession().sql(metaGetter.Get("turnover_dws_N.sql"));
        } else {
            System.out.println("白天考勤");
            dateset = DPSparkApp.getSession().sql(metaGetter.Get("turnover_dws.sql"));
        }

        dateset.show(20);
//        dateset.createOrReplaceTempView("testview");
//        DPSparkApp.getSession().sql("select site_code,sum(ttl_incumbents_qty) ttl_incumbents_qty from testview where work_dt='2020-07-01' group by site_code").show();

        JavaRDD<Put> toWriteData = dateset.toJavaRDD().mapPartitions(iterator -> {
            List<Put> datas = new ArrayList<>();
            SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
            final String family = "DPM_DWS_PERSONNEL_OVERVIEW_DD";
            iterator.forEachRemaining(row -> {
                String site_code = row.getString(0);
                String level_code = row.getString(1);
                String factory_code = row.getString(2);
                String process_code = row.getString(3);
                String humresource_type = row.getString(4);
                String work_dt = row.getString(5);
                String ttl_incumbents_qty = row.getLong(6) + "";
                String separation_qty = row.getLong(7) + "";
                String act_attendance_qty = row.getLong(8) + "";
                String plan_attendance_qty = row.getLong(9) + "";
                try {
                    String rowkey = String.valueOf(formatWorkDt.parse(work_dt).getTime()) + ":" + site_code + ":" + level_code + ":" + factory_code + ":" + process_code + ":" + humresource_type;
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("work_dt"), Bytes.toBytes(work_dt));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("site_code"), Bytes.toBytes(site_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("level_code"), Bytes.toBytes(level_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("factory_code"), Bytes.toBytes(factory_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("process_code"), Bytes.toBytes(process_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("humresource_type"), Bytes.toBytes(humresource_type));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ttl_incumbents_qty"), Bytes.toBytes(ttl_incumbents_qty));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("separation_qty"), Bytes.toBytes(separation_qty));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("act_attendance_qty"), Bytes.toBytes(act_attendance_qty));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("plan_attendance_qty"), Bytes.toBytes(plan_attendance_qty));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("data_granularity"), Bytes.toBytes("level"));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_by"), Bytes.toBytes("cj"));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("data_from"), Bytes.toBytes("workhours_day"));
                    datas.add(put);
                } catch (Exception e) {
                    //ignore
                    System.out.println("数据异常,必要参数缺失");
                }
            });
            return datas.iterator();
        });
        //plan_attendance_qty  应出勤人力（排配人力包含班次考虑）
        try {
            System.out.println(toWriteData.count());
            List<Put> take15 = toWriteData.take(5);
            for (Put put : take15) {
                System.out.println(put);
            }
        } catch (Exception e) {
        }
        System.out.println("==============================>>>toWriteData Calculate End<<<==============================");
        DPHbase.rddWrite("dpm_dws_personnel_overview_dd", toWriteData);
        System.out.println("==============================>>>Programe End<<<==============================");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }

    public static String toFactoryCode(String factoryCode) {
        switch (factoryCode) {
            case "DT(I)":
                return "DT1";
            case "DT(II)":
                return "DT2";
            default:
                return factoryCode;
        }
    }
}
