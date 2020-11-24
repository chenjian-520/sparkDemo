package com.foxconn.dpm.sprint4.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.ManualNormalization;
import com.foxconn.dpm.sprint3.ods_dwd.udf.GenerateRowKey;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author HS
 * @className UpLoadProcessDayOutPut
 * @description TODO
 * @date 2020/6/3 9:33
 */
public class UpLoadProcessDayOutPut extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        String startDay = (String) map.get("startDay");
        String endDay = (String) map.get("endDay");
        LoadKpiTarget.getLineDateset();
        if (null == startDay || "".equals(startDay)) {
            //WH HS 此处修改为前4天，避免延迟传送
            startDay = batchGetter.getStDateDayAdd(-8, "-");
        }
        if (null == endDay || "".equals(endDay)) {
            endDay = batchGetter.getStDateDayAdd(1, "-");
        }

        System.out.println("开始时间：" + startDay);
        System.out.println("结束时间：" + endDay);

        clearProcessDayOutPut(startDay, endDay);
    }

    public void clearProcessDayOutPut(String startDay, String endDay) throws Exception {

        String startDayStamp = String.valueOf(yyyy_MM_dd.parse(startDay).getTime());
        String endDayStamp = String.valueOf(yyyy_MM_dd.parse(endDay).getTime() + 1);

        System.out.println(startDayStamp + "_" + endDayStamp);

        JavaRDD<ArrayList<String>> filtedRdd = DPHbase.saltRddRead("dpm_ods_production_manual_output_day", startDayStamp, endDayStamp, new Scan(), true).keyBy(r -> {
            return batchGetter.getStrArrayOrg("=", "N/A", batchGetter.resultGetColumns(r, "DPM_ODS_PRODUCTION_MANUAL_OUTPUT_DAY",
                    "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "work_dt", "work_shift", "customer", "key", "work_order", "comment"
            ).toArray(new String[0]));
        }).reduceByKey((kv1, kv2) -> {
            return Long.valueOf(batchGetter.resultGetColumn(kv1, "DPM_ODS_PRODUCTION_MANUAL_OUTPUT_DAY", "update_dt"))
                    >
                    Long.valueOf(batchGetter.resultGetColumn(kv2, "DPM_ODS_PRODUCTION_MANUAL_OUTPUT_DAY", "update_dt"))
                    ?
                    kv1
                    :
                    kv2
                    ;
        }).map(t -> {
            return batchGetter.resultGetColumns(t._2, "DPM_ODS_PRODUCTION_MANUAL_OUTPUT_DAY", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "part_no", "sku", "platform", "workorder_type", "work_dt", "output_qty", "normalized_output_qty", "data_granularity", "machine_id", "customer", "update_dt", "update_by", "data_from", "work_shift", "key");
        }).mapPartitions(batch -> {
            ArrayList<ArrayList<String>> lines = new ArrayList<>();
            while (batch.hasNext()) {
                ArrayList<String> next = batch.next();
                for (int i = 0; i < next.size(); i++) {
                    if ("".equals(next.get(i))) {
                        next.set(i, "N/A");
                    }
                }
                /**
                 * "site_code";
                 * "level_code";
                 * "factory_code";
                 * "process_code";
                 * "area_code";
                 * "line_code";
                 * "part_no";
                 * "sku";
                 * "platform";
                 * "workorder_type";
                 * "work_dt";
                 * "output_qty";
                 * "normalized_output_qty";
                 * "data_granularity";
                 * "machine_id";
                 * "customer";
                 * "update_dt";
                 * "update_by";
                 * "data_from";
                 * "work_shift";
                 * "key"
                 */
                next.set(11, String.valueOf(batchGetter.formatInteger(next.get(11))));
                next.set(12, "0");//normalized_output_qty
                next.set(13, "process");//normalized_output_qty

                switch (next.get(0).concat(next.get(1))) {
                    case "WHL5":
                    case "WHL6":
                    case "CQL5":
                    case "CQL6":
                        next.set(6, next.get(20));//key == >>part_no
                        break;
                    case "WHL10":
                    case "CQL10":
                        if ("CQ".equals(next.get(0))) {
                            next.set(6, next.get(20));//key == >>part_no
                        } else if ("WH".equals(next.get(0))) {
                            next.set(8, next.get(20));//key == >>platform
                        }
                        break;
                }

                String processCode = next.get(3);
                try {
                    switch (processCode) {
                        case "塗裝":
                        case "涂装":
                            processCode = "UpLoad_Painting";
                            break;
                        case "成型":
                            processCode = "UpLoad_Molding";
                            break;
                        case "衝壓":
                        case "沖壓":
                            processCode = "UpLoad_Stamping";
                            break;
                        case "组装":
                        case "組裝":
                            processCode = "UpLoad_Assy";
                            break;
                    }
                } catch (Exception e) {
                    processCode = "N/A";
                }
                next.set(3, processCode);//process_code
                String area_code = next.get(4);
                try {
                    switch (area_code) {
                        case "成型":
                            area_code = "Post Mold";
                            break;
                        case "裝配":
                        case "装配":
                            area_code = "Assy";
                            break;

                    }
                } catch (Exception e) {

                }
                next.set(4, area_code);
                next.remove(20);//key
                next.remove(14);//machine_id
                next.add(0, "N/A");//ROWKEY TEMP
                lines.add(next);
            }
            /**
             * ROWKEY
             * "site_code";
             * "level_code";
             * "factory_code";
             * "process_code";
             * "area_code";
             * "line_code";
             * "part_no";
             * "sku";
             * "platform";
             * "workorder_type";
             * "work_dt";
             * "output_qty";
             * "normalized_output_qty";
             * "data_granularity";
             * "customer";
             * "update_dt";
             * "update_by";
             * "data_from";
             * "work_shift";
             */
            return lines.iterator();
        });


        JavaRDD<Row> formatedRdd = filtedRdd.filter(r -> {
            return r != null;
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg("=", "N/A",
                    /*
                     * site_code
                     * level_code
                     * factory_code
                     * process_code
                     * area_code
                     * line_code
                     * "N/A" part_no
                     * "N/A" sku
                     * "N/A" platform
                     * "N/A" workorder_type
                     * work_dt
                     *customer
                     * work_shift
                     */
                    r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7), r.get(8), r.get(9), r.get(10), r.get(11), r.get(15), r.get(19)
            );
        }).reduceByKey((r1, r2) -> {
            r1.set(12, String.valueOf(batchGetter.formatInteger(r1.get(12)) + batchGetter.formatInteger(r2.get(12))));
            return r1;
        }).map(t -> {
            return t._2;
        }).mapPartitions(batch -> {
            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            while (batch.hasNext()) {
                Row dpm_dws_production_output_dd = beanGetter.creDeftSchemaRow("dpm_dws_production_output_dd", batch.next());

                rows.add(dpm_dws_production_output_dd);
            }
            return rows.iterator();
        });
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            for (Row row : formatedRdd.take(5)) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        System.out.println("==============================>>>formatedRdd End<<<==============================");



        /*
         * ====================================================================
         *  约当系数表
         * ====================================================================
         */
        Scan normalization_scan = new Scan();
        JavaRDD<Result> manual_normalization_Rdd = DPHbase.saltRddRead("dpm_dim_production_normalized_factor", "!", "~", normalization_scan, true);
        JavaRDD<ManualNormalization> format_manual_normalization_Rdd = manual_normalization_Rdd.filter(result -> {
            return batchGetter.checkColumns(result, "DPM_DIM_PRODUCTION_NORMALIZED_FACTOR", "key", "level_code", "normalization", "normalization_bto", "normalization_cto", "update_dt");
        }).map(r -> {
            return batchGetter.resultGetColumns(r, "DPM_DIM_PRODUCTION_NORMALIZED_FACTOR", "key", "level_code", "normalization", "normalization_bto", "normalization_cto", "update_dt", "site_code");
        }).filter(r -> {
            return r != null && StringUtils.isNotBlank(r.get(6));
        }).keyBy(r -> {
            return r.get(0) + r.get(1) + r.get(6);
        }).reduceByKey((v1, v2) -> {

            return Long.valueOf(v1.get(5)) > Long.valueOf(v2.get(5)) ? v1 : v2;

        }).map(t -> {
            return t._2;
        }).map(r -> {
            return new ManualNormalization(r.get(0).trim(), r.get(1), batchGetter.formatDouble(r.get(2)), batchGetter.formatDouble(r.get(3)), batchGetter.formatDouble(r.get(4)), r.get(6));
        }).distinct();


        try {

            for (ManualNormalization manualNormalization : format_manual_normalization_Rdd.take(10)) {

                System.out.println(manualNormalization.toString());
            }

        } catch (Exception e) {

        }


        System.out.println("manualNormalization========================>>>");


        sqlContext.udf().register("generate_row_key", new GenerateRowKey(), DataTypes.StringType);
        sqlContext.createDataFrame(formatedRdd, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_dws_production_output_dd")).createOrReplaceTempView("dpm_dws_dsn_day_output");
        sqlContext.createDataFrame(format_manual_normalization_Rdd, ManualNormalization.class).createOrReplaceTempView("dpm_dim_production_normalized_factor");

        Dataset<Row> calculatedFrame = sqlContext.sql(sqlGetter.Get("sprint_four_dpm_dws_dsn_day_output_calculate.sql").replace("$ETL_TIME$", String.valueOf(System.currentTimeMillis())));

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            for (Row row : calculatedFrame.javaRDD().collect()) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        JavaRDD<Put> putJavaRDD = calculatedFrame.toJavaRDD().mapPartitions(batch -> {

            ArrayList<Put> puts = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            while (batch.hasNext()) {
                Put put = beanGetter.getPut("dpm_dws_production_output_dd", "DPM_DWS_PRODUCTION_OUTPUT_DD", batch.next());
                puts.add(put);
            }
            return puts.iterator();
        });

        try {
            DPHbase.rddWrite("dpm_dws_production_output_dd", putJavaRDD);
        } catch (Exception e) {

        }
        System.out.println("==============================>>>Programe End<<<==============================");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
