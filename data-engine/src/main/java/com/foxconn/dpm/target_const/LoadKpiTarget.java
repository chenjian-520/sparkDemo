package com.foxconn.dpm.target_const;

import com.foxconn.dpm.sprint1_2.dwd_dws.L6Oee1DwdToDws;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author HS
 * @className LoadKpiTarget
 * @description TODO
 * @date 2020/4/29 9:25
 */
public class LoadKpiTarget implements Serializable {
    private static BatchGetter batchGetter = MetaGetter.getBatchGetter();

    /*
     * ====================================================================
     * 描述: dpm_ods_production_target_values
     *      表结构
     *          - KPI
     *          - 组织层级
     *          - 小于等于当前时间点的最新数据
     *
     *
     * 返回值：目标值表rdd
     * ====================================================================
     */
    public static JavaRDD<ArrayList<String>> loadProductionTarget(String... targetLastDay) throws Exception {
        String temp = batchGetter.getStDateDayAdd(0, "-");
        if (targetLastDay != null && targetLastDay.length == 1) {
            temp = targetLastDay[0];
        }

        final String usedTargetLastDay = temp;

        Scan scan = new Scan();
        scan.withStartRow("!".getBytes());
        scan.withStopRow("~".getBytes());
        JavaRDD<Result> targetRSRdd = DPHbase.rddRead("dpm_ods_production_target_values", scan, true);

        if (targetRSRdd == null) {
//            System.out.println("read err");
            return null;
        }
//        System.out.println("==============================>>>resultJavaRDD End<<<==============================");

        JavaRDD<ArrayList<String>> filtedTargetRDD = targetRSRdd.filter(r -> {
            return batchGetter.checkColumns(r, "DPM_ODS_PRODUCTION_TARGET_VALUE", "day_id", "update_dt");
        }).filter(r -> {
            return batchGetter.dateStrCompare(
                    batchGetter.resultGetColumn(r, "DPM_ODS_PRODUCTION_TARGET_VALUE", "day_id"),
                    usedTargetLastDay, "yyyy-MM-dd",
                    "<=");
        }).mapPartitions(batch -> {
            ArrayList<ArrayList<String>> lines = new ArrayList<>();
            while (batch.hasNext()) {
                //24 customer
                ArrayList<String> line = batchGetter.resultGetColumns(batch.next(), "DPM_ODS_PRODUCTION_TARGET_VALUE",
                        "day_id", "group_code", "site_code", "bu_code", "factory_code", "process_code", "line_code", "machine_id", "online_dl_upph", "offline_var_dl_upph", "offline_fix_dl_headcount", "idl", "schedule_adherence", "otd", "oee1", "oee2", "fpy", "uph", "turnover", "attendance", "safety", "update_dt", "update_by", "data_from", "customer"
                );
                String processCode = line.get(5);
                try {
                    switch (processCode) {
                        case "塗裝":
                        case "涂装":
                            processCode = "Painting";
                            break;
                        case "成型":
                            processCode = "Molding";
                            break;
                        case "衝壓":
                        case "沖壓":
                            processCode = "Stamping";
                            break;
                        case "组装":
                        case "組裝":
                        case "Assembly":
                            processCode = "Assy";
                            break;
                    }
                } catch (Exception e) {
                    processCode = "N/A";
                }
                line.set(5, processCode);
                for (int i = 8; i < 21; i++) {
                    if(line.get(i).contains(",")){
                        line.set(i, line.get(i).replace(",", "."));
                    }
                    if (!"".equals(line.get(i))) {
                        if (line.get(i).contains("%") && line.get(i).matches("\\d+[\\.]?\\d*[%]?")) {
                            try {
                                line.set(i, String.valueOf(Float.valueOf(line.get(i).replace("%", "")) / 100));
                            } catch (Exception e) {
                                line.set(i, "0");
                            }
                        }
                    } else {
                        if ("NaN".equals(line.get(i)) || "".equals(line.get(i)) || "/".equals(line.get(i))) {
                            line.set(i, "0");
                        }
                    }
                }
                for (int i = 0; i < 8; i++) {
                    String org = line.get(i);
                    if ("/".equals(org) || "".equals(org) || "-".equals(org)) {
                        line.set(i, "all");
                    }
                }
                String org = line.get(24);
                if ("/".equals(org) || "".equals(org) || "-".equals(org)) {
                    line.set(24, "all");
                }
                lines.add(line);
            }
            return lines.iterator();
        }).mapToPair(new PairFunction<ArrayList<String>, String, ArrayList<String>>() {
            @Override
            public Tuple2<String, ArrayList<String>> call(ArrayList<String> r) throws Exception {
                //按照组织层级给key
                return new Tuple2<String, ArrayList<String>>(
                        batchGetter.getStrArrayOrg(",", "-",
                                r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7), r.get(24)
                        )
                        ,
                        r
                );
            }
        }).reduceByKey((rv1, rv2) -> {
            //根据单行数据日期以及上传时间取最新一条
            return Integer.valueOf(rv1.get(0).replace("-", "")) > Integer.valueOf(rv2.get(0).replace("-", ""))
                    ?
                    rv1 : (
                    (Integer.valueOf(rv1.get(0).replace("-", "")).equals(Integer.valueOf(rv2.get(0).replace("-", ""))))
                            ? (Long.valueOf(rv1.get(21)) > Long.valueOf(rv2.get(21)) ? rv1 : rv2) : rv2
            );
        }).map(t -> {
            return t._2;
        }).cache();


        /*try {
            //获得某组织层级的最新一条数据
            for (ArrayList<String> strings : filtedTargetRDD.take(50)) {
                System.out.println(strings);
            }
        } catch (Exception e) {

        }*/
//        System.out.println("==============================>>>filtedTargetRDD End<<<==============================");

        JavaRDD<Row> rowJavaRDD = filtedTargetRDD.mapPartitions(batchData -> {
            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            while (batchData.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_target_values", batchData.next()));
            }
            return rows.iterator();
        });
        SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_target_values"));
        dataFrame.createOrReplaceTempView("dpm_ods_production_target_values");

        //"day_id",
        // "group_code", "site_code", "bu_code", "factory_code", "process_code", "line_code", "machine_id",
        //7
        // "online_dl_upph", "offline_var_dl_upph","offline_fix_dl_headcount", "idl", "schedule_adherence",
        //12
        // "otd", "oee1", "oee2", "fpy", "uph",
        //17
        // "turnover", "attendance", "safety", "update_dt", "update_by",
        //22
        // "data_from  customer"
        HashMap<String, ArrayList<String>> nowNewerTargetLocal = new HashMap<>();
        filtedTargetRDD.mapToPair(new PairFunction<ArrayList<String>, String, ArrayList<String>>() {
            @Override
            public Tuple2<String, ArrayList<String>> call(ArrayList<String> r) throws Exception {
                //按照组织层级给key
                return new Tuple2<String, ArrayList<String>>(
                        batchGetter.getStrArrayOrg("=", "all",
                                r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6), r.get(7), r.get(24)
                        )
                        ,
                        r
                );
            }
        }).collectAsMap().forEach(new BiConsumer<String, ArrayList<String>>() {
            @Override
            public void accept(String k, ArrayList<String> v) {
                nowNewerTargetLocal.put(k, v);
            }
        });
        class GetAimTargetByKey implements UDF2<String, Integer, Float> {
            //层级Key使用等号进行分隔
            @Override
            public Float call(String levelKey, Integer targetIndex) throws Exception {
                int targetKeyLength = levelKey.split("=").length;
                if (targetKeyLength < 8){
                    for (int i = 0; i < 8 - targetKeyLength; i++){
                        levelKey = levelKey.concat("=all");
                    }
                }
                ArrayList<String> collect = nowNewerTargetLocal.get(levelKey);
                if (collect == null || collect.size() == 0) {
                    return 0f;
                } else {
                    try {
                        return Float.valueOf((String.valueOf(collect.get(targetIndex))));
                    } catch (Exception e) {
                        return 0f;
                    }
                }
            }
        }
        DPSparkApp.getSession().udf().register("get_aim_target_by_key", new GetAimTargetByKey(), DataTypes.FloatType);

        return filtedTargetRDD;
    }

    //构造 大线  和 小线 的对应关系临时表
    public static Dataset<Row> getLineDateset() {

        ArrayList<StructField> structField = new ArrayList();
        structField.add(DataTypes.createStructField("line", DataTypes.StringType, true));
        structField.add(DataTypes.createStructField("line_XF", DataTypes.StringType, true));
        StructType structType = new StructType(structField.toArray(new StructField[0]));
        List<String> list = L6Oee1DwdToDws.lineCode();
        Map<String, String> valuemap = new HashMap<>();
        for (String a : list) {
            String[] split = a.split(",");
            valuemap.put(split[1], split[0]);
        }

        JavaRDD<Row> JavaRow = DPSparkApp.getContext().parallelize(list).map(r -> {
            String[] split = r.split(",");
            ArrayList<String> values = new ArrayList();
            values.add(split[0]);
            values.add(split[1]);
            return new GenericRowWithSchema(values.toArray(), structType);
        });

        class LineTotranf implements UDF1<String, String> {
            @Override
            public String call(String s) throws Exception {
                return valuemap.get(s) == null ? s : valuemap.get(s);
            }
        }
        DPSparkApp.getSession().udf().register("LineTotranfView", new LineTotranf(), DataTypes.StringType);
        return DPSparkApp.getSession().createDataFrame(JavaRow, structType);
    }


    public static Boolean levelOutPutFilter(String type, String group, String site_code, String level_code, String customer, String station_code, String isFail) {
        if ("line=packing".equals(type)) {
            switch (level_code) {
                case "L5":
                    return
                            (customer.toUpperCase().equals("HP") && station_code.trim().toUpperCase().equals("PACKING") && isFail.equals("0"))
                                    ||
                                    (customer.toUpperCase().equals("DELL") && station_code.trim().toUpperCase().equals("PACKING") && isFail.equals("0"))
                                    ||
                                    ((customer.toUpperCase().equals("LENOVO") || customer.toUpperCase().equals("LENOVO_CODE")) && station_code.trim().toUpperCase().equals("PALLETIZATION") && isFail.equals("0"));
                case "L6":
                    return (
                            (
                                    station_code.trim().equals("PACKING") && isFail.equals("0") ? true : false
                                            &&
                                            "WH".equals(site_code)
                            )
                                    ||
                                    (
                                            station_code.trim().equals("PACKING") && isFail.equals("0") ? true : false
                                                    &&
                                                    "CQ".equals(site_code)
                                    )
                    );
                case "L10":
                    return (
                            (
                                    station_code.trim().equals("SCAN COUNTRY KIT") && isFail.equals("0") ? true : false
                                            &&
                                            "WH".equals(site_code)
                            )
                                    ||
                                    (
                                            station_code.trim().equals("PACKING") && isFail.equals("0") ? true : false
                                                    &&
                                                    "CQ".equals(site_code)
                                    )
                    );
                default:
                    return false;
            }
        } else if ("process=ASSEMBLY1".equals(type)) {
            switch (level_code) {
                case "L10":
                    return station_code.trim().equals("ASSEMBLY1") && isFail.equals("0") ? true : false;
                default:
                    return false;
            }
        }
        return null;
    }
}
