package com.foxconn.dpm.sprint1_2.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.DailyRepair;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @program: ehr->DailyRepairCaculate
 * @description: DWD層dpm_dwd_production_repair到DWS層dpm_dws_production_repair_station_dd
 * customer=HP抓fail_station（PRETEST,POST RUNIN）統計個數到total count
 * customer=Lenovo抓fail_station（Testing）統計個數到total count
 * @author: Axin
 * @create: 2020-01-15 21:34
 **/
public class DailyRepairCaculate extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        String param_work_dt = (String) map.get("work_dt");
        final String target_work_dt = param_work_dt == null ? batchGetter.getStDateDayAdd(-1, "-") : param_work_dt;
        final String tomorrow_work_dt = param_work_dt == null ? batchGetter.getStDateDayAdd(0, "-") : batchGetter.getStDateDayStrAdd(param_work_dt, 1, "-");
        System.out.println(target_work_dt);
        final String family = "DPM_DWD_PRODUCTION_REPAIR";
        JavaRDD<Result> map1 = DPHbase.rddRead("dpm_dwd_production_repair", new Scan(), true).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "site_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "level_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "area_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "line_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "work_shift"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "work_dt"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "part_no"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "customer"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "platform"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "wo"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "sn"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "repair_out_dt"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "fail_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "fail_desc"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "repair_out_dt"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "fail_station"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "repair_code"),
                    batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_REPAIR", "repair_code_desc")
            );

        }).filter(t -> {
            return target_work_dt.equals(batchGetter.resultGetColumn(t._2, "DPM_DWD_PRODUCTION_REPAIR", "work_dt"))
                    ;
        }).reduceByKey((r1, r2) -> {
            return Long.valueOf(batchGetter.resultGetColumn(r1, "DPM_DWD_PRODUCTION_REPAIR", "update_dt"))
                    >
                    Long.valueOf(batchGetter.resultGetColumn(r2, "DPM_DWD_PRODUCTION_REPAIR", "update_dt"))
                    ?
                    r1
                    :
                    r2
                    ;
        }).map(t -> {
            return t._2;
        });
        JavaRDD<DailyRepair> productionRDD = map1.map(r -> {
            return new DailyRepair(
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("site_code"))),//1
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("level_code"))),//2 factory_code，process_code，sku
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("area_code"))),//3
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("line_code"))),//4
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("work_shift"))),//5
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("work_dt"))),//6
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("part_no"))),//7
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("customer"))),//8
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("platform"))),//9
                    Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("fail_station")))//10
            );
        })/*.filter(r -> {
            return
                    ("HP".equals(r.getCustomer()) || "LENOVO".equals(r.getCustomer())) &&
                    "L10".equals(r.getLevel_code()) &&
                            "WH".equals(r.getSite_code());
        })*/;

        SparkSession session = DPSparkApp.getSession();
        session.createDataFrame(productionRDD, DailyRepair.class).createOrReplaceTempView("dailyRepair");
        Dataset<Row> ds = session.sql(sqlGetter.Get("daily_repair.sql"));

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
           /* for (DailyRepair dailyRepair : productionRDD.collect()) {
                System.out.println(dailyRepair.toString());
            }*/
            System.out.println("DISTINCT:" + productionRDD.count());
            ds.show(10);
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        JavaRDD<Put> toWriteData = ds.toJavaRDD().mapPartitions(iterator -> {
            List<Put> datas = new ArrayList<>();
            ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
            SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
            String updateTime = String.valueOf(System.currentTimeMillis());
            String toWriteFamily = "DPM_DWS_PRODUCTION_REPAIR_STATION_DD";
            while (iterator.hasNext()) {
                Row row = iterator.next();
                String site_code = row.getString(0);
                String level_code = row.getString(1);
                String area_code = row.getString(2);
                String line_code = row.getString(3);
                String plantform = row.getString(4);
                String customer = row.getString(5);
                String work_dt = row.getString(6);
                String work_shift = row.getString(7);
                String part_no = row.getString(8);
                String fail_station = row.getString(9);
                String total_count = String.valueOf(row.getLong(10));
                //addsalt+work_dt+site_code+level_code+line_code+part_no+fail_station
                String rowkey = formatWorkDt.parse(work_dt).getTime() + ":" + site_code + ":" + level_code + ":" + line_code + ":" + part_no + ":" + fail_station;
                rowkey = consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
                String data_granularity = level_code + "/" + area_code + "/" + line_code;//level/factory/process/area/line
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("site_code"), Bytes.toBytes(site_code));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("level_code"), Bytes.toBytes(level_code));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("factory_code"), Bytes.toBytes(""));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("process_code"), Bytes.toBytes(""));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("area_code"), Bytes.toBytes(area_code));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("line_code"), Bytes.toBytes(line_code));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("work_dt"), Bytes.toBytes(work_dt));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("work_shift"), Bytes.toBytes(work_shift));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("sku"), Bytes.toBytes(""));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("part_no"), Bytes.toBytes(part_no));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("platform"), Bytes.toBytes(plantform));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("fail_station"), Bytes.toBytes(fail_station));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("total_count"), Bytes.toBytes(total_count));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("customer"), Bytes.toBytes(customer));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("data_granularity"), Bytes.toBytes(data_granularity));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("update_dt"), Bytes.toBytes(updateTime));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("update_by"), Bytes.toBytes("Axin"));
                put.addColumn(Bytes.toBytes(toWriteFamily), Bytes.toBytes("data_from"), Bytes.toBytes("dpm_dwd_production_repair_dd"));
                if(put == null){
                    continue;
                }
                datas.add(put);
            }
            return datas.iterator();
        });
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            System.out.println("PUT:" + toWriteData.count());
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPHbase.rddWrite("dpm_dws_production_repair_station_dd", toWriteData);
        System.out.println("==============================>>>Programe End<<<==============================");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
