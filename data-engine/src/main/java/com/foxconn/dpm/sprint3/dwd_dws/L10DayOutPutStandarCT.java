package com.foxconn.dpm.sprint3.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.UphDwdCt;
import com.foxconn.dpm.sprint1_2.dwd_dws.beans.UphPartnoOutput;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Description:  com.dl.spark.ehr.dws
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/16
 */
public class L10DayOutPutStandarCT extends DPSparkBase {

    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");

        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;
        String tomorrow = null;
        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1);
            today = batchGetter.getStDateDayAdd(0);
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
            tomorrow = batchGetter.getStDateDayStampAdd(1);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }
        /**
         * 取得dws中的表数据
         * dpm_dws_production_output_day 加盐表，基础数据表
         * rowkey格式 addsalt:work_dt:Site:LevelCode:Line+UUID
         * 按天取数据,并根据该数据构造基础输出数据
         */
        JavaRDD<Result> pdOutputData = DPHbase.saltRddRead("dpm_dws_production_output_dd", yesterdayStamp, tomorrow, new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<UphPartnoOutput> mainRdd = pdOutputData.filter(r -> {
            return batchGetter.checkColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "platform");
        }).filter(r -> {
            return ("process".equals(batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
                    &&
                    "ASSEMBLY1".equals(batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "process_code"))
            )

                    ;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    batchGetter.resultGetColumns(r, "DPM_DWS_PRODUCTION_OUTPUT_DD",
                            "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "part_no", "platform", "work_dt", "work_shift", "workorder_type", "sku"
                    ).toArray(new String[0])
            );
        }).reduceByKey((kv1, kv2) -> {
            return Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_DWS_PRODUCTION_OUTPUT_DD", "update_dt"))
                    >
                    Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_DWS_PRODUCTION_OUTPUT_DD", "update_dt"))
                    ?
                    kv1
                    :
                    kv2;
        }).map(t -> {
            return t._2;
        }).map(new Function<Result, UphPartnoOutput>() {
            @Override
            public UphPartnoOutput call(Result result) throws Exception {
                final String family = "DPM_DWS_PRODUCTION_OUTPUT_DD";
                String site_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("site_code")));
                String level_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("level_code")));
                String factory_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("factory_code")));
                String process_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("process_code")));
                String area_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("area_code")));
                String line_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("line_code")));
                String part_no = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("part_no")));
                String platform = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("platform")));
                String work_dt = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("work_dt")));
                String work_shift = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("work_shift")));
                String output_qty = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("output_qty")));
                String customer = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("customer")));
                return new UphPartnoOutput(site_code, level_code, factory_code, process_code, area_code, line_code, part_no, platform, work_dt, work_shift, "", output_qty, "", customer);
            }
        });
        /**
         * 取得dwd中的表数据
         * dpm_dwd_production_standary_ct 不加盐表，ct表
         * rowkey格式 site_code+level_code+ plantform+part_no
         * 取全表数据
         */
        Scan scan = new Scan();
        scan.withStartRow("!".getBytes());
        scan.withStopRow("~".getBytes());
        JavaRDD<Result> ctData = DPHbase.rddRead("dpm_dwd_production_standary_ct", scan, true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<UphDwdCt> ctFormatData = ctData.map(new Function<Result, UphDwdCt>() {
            @Override
            public UphDwdCt call(Result result) throws Exception {
                final String family = "DPM_DWD_PRODUCTION_STANDARY_CT";
                String site_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("site_code")));
                String level_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("level_code")));
                String factory_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("factory_code")));
                String process_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("process_code")));
                String line_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("line_code")));
                String platform = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("platform")));
                String part_no = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("part_no")));
                String cycle_time = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("cycle_time")));
                Long update_dt = null;
                try {
                    update_dt = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_dt"))));
                } catch (Exception e) {
                }
                String update_by = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_by")));
                String data_from = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("data_from")));
                return new UphDwdCt(site_code, level_code, factory_code, process_code, line_code, platform, part_no, cycle_time, update_dt, update_by, data_from);
            }
        })
                .filter(r -> r.getUpdate_dt() != null)
                .keyBy(r -> batchGetter.getStrArrayOrg(",", "-", r.getSite_code(), r.getLevel_code(), r.getFactory_code(), r.getProcess_code(), r.getLine_code(), r.getPlatform(), r.getPart_no()/*, r.getCycle_time()*/))
                .reduceByKey((v1, v2) -> v1.getUpdate_dt() >= v2.getUpdate_dt() ? v1 : v2)
                .map(t -> t._2());
        LoadKpiTarget.getLineDateset();
        //转换为sql进行计算
        SparkSession session = DPSparkApp.getSession();
        session.createDataFrame(mainRdd, UphPartnoOutput.class).where(new Column("level_code").contains("L10")).createOrReplaceTempView("partnoOutput");
        session.createDataFrame(ctFormatData, UphDwdCt.class).where(new Column("level_code").contains("L10")).createOrReplaceTempView("uphCt");
        session.sql("select * from partnoOutput").show();
        session.sql("select * from uphCt").show();
        Dataset<Row> ds = session.sql(sqlGetter.Get("sprint_three_dayoutput_standar_ct.sql"));
        ds.show();

        try {

            for (Row row : ds.toJavaRDD().collect()) {
                System.out.println(row);
            }
        } catch (Exception e) {

        }

        //输出到dws目标表
        final Long update_dt = System.currentTimeMillis();
        JavaRDD<Put> toWriteData = ds.toJavaRDD().mapPartitions(iterator -> {
            List<Put> datas = new ArrayList<>();
            SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
            final String family = "DPM_DWS_PRODUCTION_PARTNO_DD";
            iterator.forEachRemaining(row -> {
                String site_code = batchGetter.replaceEmpStr(row.getString(0));
                String level_code = batchGetter.replaceEmpStr(row.getString(1));
                String factory_code = batchGetter.replaceEmpStr(row.getString(2));
                String process_code = batchGetter.replaceEmpStr(row.getString(3));
                String area_code = batchGetter.replaceEmpStr(row.getString(4));
                String line_code = batchGetter.replaceEmpStr(row.getString(5));
                String part_no = batchGetter.replaceEmpStr(row.getString(6));
                String plantform = batchGetter.replaceEmpStr(row.getString(7));
                String work_dt = batchGetter.replaceEmpStr(row.getString(8));
                String work_shift = batchGetter.replaceEmpStr(row.getString(9));
                String output_qty = batchGetter.replaceEmpStr(row.getString(10));
                String cycle_time = batchGetter.replaceEmpStr(row.getString(11));
                String customer = batchGetter.replaceEmpStr(row.getString(12));
                try {
                    String rowkey = String.valueOf(formatWorkDt.parse(work_dt).getTime()) + ":" + site_code + ":" + level_code + ":" + line_code + ":" + plantform + ":" + work_shift + ":" + UUID.randomUUID().toString().replace("-", "");
                    rowkey = consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("site_code"), Bytes.toBytes(site_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("level_code"), Bytes.toBytes(level_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("factory_code"), Bytes.toBytes(factory_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("process_code"), Bytes.toBytes(process_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("area_code"), Bytes.toBytes(area_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("line_code"), Bytes.toBytes(line_code));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("part_no"), Bytes.toBytes(part_no));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("platform"), Bytes.toBytes(plantform));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("work_dt"), Bytes.toBytes(work_dt));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("work_shift"), Bytes.toBytes(work_shift));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("output_qty"), Bytes.toBytes(output_qty));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ct"), Bytes.toBytes(cycle_time));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_dt"), Bytes.toBytes(update_dt.toString()));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_by"), Bytes.toBytes("F20DL"));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("data_from"), Bytes.toBytes("uph-dwd"));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("data_granularity"), Bytes.toBytes("process"));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("customer"), Bytes.toBytes(customer));
                    datas.add(put);
                } catch (Exception e) {
                    //ignore
                    System.out.println("数据异常,必要参数缺失");
                }
            });
            return datas.iterator();
        });
        System.out.println("==============================>>>toWriteData Calculate End<<<==============================");
        System.out.println(toWriteData.count());
        DPHbase.rddWrite("dpm_dws_production_partno_dd", toWriteData);
        System.out.println("==============================>>>Programe End<<<==============================");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
