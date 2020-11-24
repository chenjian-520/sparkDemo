package com.foxconn.dpm.sprint5.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmAdsProductionOeeDay;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsProductionOeeEquipmentLineDD;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * @author HS
 * @className L5ProductionOeeSprintFive
 * @description TODO
 * @date 2020/4/22 13:08
 */
public class L5ProductionOeeSprintFive extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();

    HashMap<String, Float> oee2Targets = new HashMap<>();
    String etl_time = String.valueOf(System.currentTimeMillis());
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //dpm_ods_production_target_values

        loadTargets(batchGetter.getStDateDayAdd(-1, "-"));
        System.out.println(oee2Targets.toString());
        //日OEE
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineDDJavaPairRDD = calculateProductionOee(batchGetter.getStDateDayStampAdd(-8, "-"), batchGetter.getStDateDayStampAdd(1, "-"));
        calculateDayProductionOee(productionOeeEquipmentLineDDJavaPairRDD);
        clearTable();


        //周OEE
        loadTargets(batchGetter.getStDateWeekAdd(-1, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineWeekJavaPairRDD = calculateProductionOee(batchGetter.getStDateWeekStampAdd(-1, "-")._1, batchGetter.getStDateWeekStampAdd(0, "-")._1);
        calculateWeekProductionOee(productionOeeEquipmentLineWeekJavaPairRDD);
        clearTable();

        //周OEE
        loadTargets(batchGetter.getStDateWeekAdd(0, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineWeekJavaPairRDD2 = calculateProductionOee(batchGetter.getStDateWeekStampAdd(0, "-")._1, batchGetter.getStDateWeekStampAdd(1, "-")._1);
        calculateWeekProductionOee(productionOeeEquipmentLineWeekJavaPairRDD2);
        clearTable();


        //月
        loadTargets(batchGetter.getStDateMonthAdd(-1, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineMonthJavaPairRDD = calculateProductionOee(batchGetter.getStDateMonthStampAdd(-1, "-")._1, batchGetter.getStDateMonthStampAdd(0, "-")._1);
        calculateMonthProductionOee(productionOeeEquipmentLineMonthJavaPairRDD);
        clearTable();

        //月
        loadTargets(batchGetter.getStDateMonthAdd(0, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineMonthJavaPairRDD2 = calculateProductionOee(batchGetter.getStDateMonthStampAdd(0, "-")._1, batchGetter.getStDateMonthStampAdd(1, "-")._1);
        calculateMonthProductionOee(productionOeeEquipmentLineMonthJavaPairRDD2);
        clearTable();

        //季
        loadTargets(batchGetter.getStDateQuarterAdd(-1, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineQuarterJavaPairRDD = calculateProductionOee(batchGetter.getStDateQuarterStampAdd(-1, "-")._1, batchGetter.getStDateQuarterStampAdd(0, "-")._1);
        calculateQuarterProductionOee(productionOeeEquipmentLineQuarterJavaPairRDD);
        clearTable();

        //季
        loadTargets(batchGetter.getStDateQuarterAdd(0, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineQuarterJavaPairRDD2 = calculateProductionOee(batchGetter.getStDateQuarterStampAdd(0, "-")._1, batchGetter.getStDateQuarterStampAdd(1, "-")._1);
        calculateQuarterProductionOee(productionOeeEquipmentLineQuarterJavaPairRDD2);
        clearTable();

        //年

        loadTargets(batchGetter.getStDateYearAdd(-1, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineYearJavaPairRDD = calculateProductionOee(batchGetter.getStDateYearStampAdd(-1, "-")._1, batchGetter.getStDateYearStampAdd(0, "-")._1);
        calculateYearProductionOee(productionOeeEquipmentLineYearJavaPairRDD);
        clearTable();

        //年

        loadTargets(batchGetter.getStDateYearAdd(0, "-")._2);
        System.out.println(oee2Targets.toString());
        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> productionOeeEquipmentLineYearJavaPairRDD2 = calculateProductionOee(batchGetter.getStDateYearStampAdd(0, "-")._1, batchGetter.getStDateYearStampAdd(1, "-")._1);
        calculateYearProductionOee(productionOeeEquipmentLineYearJavaPairRDD2);
        clearTable();
    }

    public JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> calculateProductionOee(String startDay, String endDay) throws Exception {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        long startStamp = Long.valueOf(startDay);
        long endStamp = Long.valueOf(endDay);

        System.out.println(startDay + "_____" + endDay);
        System.out.println(startStamp + "_____" + endStamp);

        JavaRDD<Result> production_oee_equipment_line_dd = DPHbase.saltRddRead("dpm_dws_production_oee_equipment_line_dd", startDay, endDay, new Scan(), true);

        JavaRDD<Result> filter = production_oee_equipment_line_dd.filter(result -> {
            //必须字段过滤
            return batchGetter.checkColumns(result, "DPM_DWS_PRODUCTION_OEE_EQUIPMENT_LINE_DD", "work_dt", "level_code", "factory_code", "time_efficiency_rate", "performance_efficiency_rate", "bad_losses", "yield_rate", "actual_output_qty", "good_product_qty", "update_dt")
                    &&
                    batchGetter.getFilterRangeTimeStampHBeans(result, "DPM_DWS_PRODUCTION_OEE_EQUIPMENT_LINE_DD", "work_dt", "yyyy-MM-dd", startStamp, endStamp)
                    &&
                    "L5".equals(Bytes.toString(result.getValue("DPM_DWS_PRODUCTION_OEE_EQUIPMENT_LINE_DD".getBytes(), "level_code".getBytes())))
                    &&
                    (
                            "DT2".equals(Bytes.toString(result.getValue("DPM_DWS_PRODUCTION_OEE_EQUIPMENT_LINE_DD".getBytes(), "factory_code".getBytes())))
                                    ||
                                    "DT1".equals(Bytes.toString(result.getValue("DPM_DWS_PRODUCTION_OEE_EQUIPMENT_LINE_DD".getBytes(), "factory_code".getBytes())))
                    )
                    &&
                    batchGetter.matchesData(batchGetter.resultGetColumn(result, "DPM_DWS_PRODUCTION_OEE_EQUIPMENT_LINE_DD", "work_dt"))
                    ;
        });

        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> dwsProductionOeeEquipmentLineDDJavaPairRDD = filter.mapPartitions(bathData -> {
            ArrayList<DpmDwsProductionOeeEquipmentLineDD> dpmDwsProductionOeeEquipmentLineDDS = new ArrayList<>();
            while (bathData.hasNext()) {
                dpmDwsProductionOeeEquipmentLineDDS.add(batchGetter.<DpmDwsProductionOeeEquipmentLineDD>getBeanDeftInit(new DpmDwsProductionOeeEquipmentLineDD(), batchGetter.resultGetColumns(bathData.next(), "DPM_DWS_PRODUCTION_OEE_EQUIPMENT_LINE_DD", "work_dt", "work_shift", "site_code", "level_code", "factory_code", "process_code", "area_code", "line_code", "machine_id", "equipment_total_qty", "total_work_time", "equipment_work_qty", "plan_noproduction_time", "equipment_maintenance", "official_holiday", "plan_downtime_total", "npi_time", "breakdown_time", "die_failure_time", "short_halt_time", "test_equipment_time", "co_time", "stamping_change_material", "molding_clean", "incoming_material_adverse", "wait_material", "energy_error", "equipment_error", "labour_error", "other_error", "noplan_down_time", "loss_time", "operation_time", "plan_output_qty", "actual_output_qty", "normalized_output_qty", "good_product_qty", "output_hours", "time_efficiency_rate", "performance_efficiency_rate", "yield_rate", "oee1_actual", "loading_time", "rate", "npi_time_per", "breakdown_time_per", "die_failure_time_per", "short_halt_time_per", "test_equipment_time_per", "co_time_per", "stamping_change_material_per", "molding_clean_per", "incoming_material_adverse_per", "wait_material_per", "energy_error_per", "equipment_error_per", "labour_error_per", "other_error_per", "time_efficiency_rate2", "performance_efficiency_rate2", "bad_losses", "oee2", "data_granularity", "update_dt", "update_by", "data_from")));
            }
            return dpmDwsProductionOeeEquipmentLineDDS.iterator();
        }).keyBy(b -> {
            /*
             * 塗裝 成型 衝壓 涂装 沖壓 组装 組裝
             */
            String processCode = b.getProcess_code();
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
                        processCode = "Assy";
                        break;
                }
            } catch (Exception e) {
                processCode = "N/A";
            }

            String area_code = "NULL";
            if ("WH".equals(b.getSite_code()) && "L5".equals(b.getLevel_code()) && "Molding".equals(b.getProcess_code())) {
                area_code = b.getArea_code();
                switch (area_code) {
                    case "裝配":
                    case "装配":
                        area_code = "Assy";
                        break;
                }
                b.setArea_code(area_code);
                b.setLine_code("NULL");
            }else{
                b.setArea_code("NULL");
            }

            return batchGetter.getStrArrayOrg(",", "N/A",
                    b.getWork_dt(),//work_date
                    //"晚班".equals(b.getWork_shift()) ? "N" : ("白班".equals(b.getWork_shift()) ? "D" : b.getWork_shift()),
                    "N/A",//workshift_code
                    b.getSite_code(),//site_code
                    b.getLevel_code(),//level_code
                    b.getFactory_code(),//factory_code
                    processCode,//process_code
                    area_code,//area
                    b.getLine_code(),//line
                    "N/A"//machine_id
            );
        }).reduceByKey((v1, v2) -> {
            return v1.getUpdate_dt() > v2.getUpdate_dt() ? v1 : v2;
        });

        return dwsProductionOeeEquipmentLineDDJavaPairRDD;
    }


    public void calculateDayProductionOee(JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> dwsProductionOeeEquipmentLineDDJavaPairRDD) throws Exception {


        JavaRDD<DpmAdsProductionOeeDay> calculatedOee = dwsProductionOeeEquipmentLineDDJavaPairRDD.reduceByKey((kv1, kv2) -> {
            return kv1.add(kv2);
        }).map(r -> {
            String[] org = r._1.split(",");
            Float loading_time = floatValueOf(r._2.getLoading_time());
            Float npi_time = floatValueOf(r._2.getNpi_time());
            Float breakdown_time = floatValueOf(r._2.getBreakdown_time());
            Float die_failure_time = floatValueOf(r._2.getDie_failure_time());
            Float short_halt_time = floatValueOf(r._2.getShort_halt_time());
            Float test_equipment_time = floatValueOf(r._2.getTest_equipment_time());
            Float co_time = floatValueOf(r._2.getCo_time());
            Float stamping_change_material = floatValueOf(r._2.getStamping_change_material());
            Float molding_clean = floatValueOf(r._2.getMolding_clean());
            Float incoming_material_adverse = floatValueOf(r._2.getIncoming_material_adverse());
            Float wait_material = floatValueOf(r._2.getWait_material());
            Float energy_error = floatValueOf(r._2.getEnergy_error());
            Float equipment_error = floatValueOf(r._2.getEquipment_error());
            Float labour_error = floatValueOf(r._2.getLabour_error());
            Float other_error = floatValueOf(r._2.getOther_error());
            Float output_hours = floatValueOf(r._2.getOutput_hours());
            Float operation_time = floatValueOf(r._2.getOperation_time());
            Float good_product_qty = r._2.getGood_product_qty();
            Float actual_output_qty = r._2.getActual_output_qty();
            Float total_work_time = floatValueOf(r._2.getTotal_work_time());
            Float oee2Target = oee2Targets.get(org[2] + "," + org[3] + "," + org[4] + "," + org[5] + "," + org[6]);
            float oee1_actual = 0f;
            try {
                oee1_actual = batchGetter.formatFloat((good_product_qty / actual_output_qty) * (operation_time / total_work_time) * (output_hours / operation_time) * 100);
            } catch (Exception e) {
                oee1_actual = 0f;
            }
            float oee2_actual = 0f;
            try {
                oee2_actual = batchGetter.formatFloat((loading_time / loading_time -
                        (
                                npi_time / loading_time
                                        +
                                        breakdown_time / loading_time
                                        +
                                        die_failure_time / loading_time
                                        +
                                        short_halt_time / loading_time
                                        +
                                        test_equipment_time / loading_time
                                        +
                                        co_time / loading_time
                                        +
                                        stamping_change_material / loading_time
                                        +
                                        molding_clean / loading_time
                                        +
                                        incoming_material_adverse / loading_time
                                        +
                                        wait_material / loading_time
                                        +
                                        energy_error / loading_time
                                        +
                                        equipment_error / loading_time
                                        +
                                        labour_error / loading_time
                                        +
                                        other_error / loading_time
                        )
                        -
                        (
                                loading_time / loading_time - output_hours / operation_time
                                        +
                                        loading_time / loading_time - good_product_qty / actual_output_qty
                        )
                ) * 100);
            } catch (Exception e) {
                oee2_actual = 0f;
            }
            return new DpmAdsProductionOeeDay(
                    System.currentTimeMillis() + "-" + UUID.randomUUID().toString(),
                    org[0],
                    org[1],
                    org[2],
                    org[3],
                    org[4],
                    org[5],
                    org[6],
                    org[7],
                    org[8],
                    oee1_actual,
                    oee2_actual,
                    oee2Target == null ? 0.0f : oee2Target,
                    etl_time,
                    loading_time, npi_time, breakdown_time, die_failure_time, short_halt_time, test_equipment_time, co_time, stamping_change_material, molding_clean, incoming_material_adverse, wait_material, energy_error, equipment_error, labour_error, other_error, output_hours, operation_time, good_product_qty, actual_output_qty,
                    total_work_time,
                    loading_time
            );
        });


        Dataset<Row> calculatedOeeDF = sqlContext.createDataFrame(calculatedOee, DpmAdsProductionOeeDay.class);
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            calculatedOeeDF.show(1000);
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_day", calculatedOeeDF.toJavaRDD(), MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_oee_day"), calculatedOeeDF.schema());
    }

    public void calculateWeekProductionOee(JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> dwsProductionOeeEquipmentLineDDJavaPairRDD) throws Exception {

        JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> map = dwsProductionOeeEquipmentLineDDJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>, String, DpmDwsProductionOeeEquipmentLineDD>() {
            @Override
            public Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> call(Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> t) throws Exception {
                // KEY
                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                String[] org = t._1.split(",");
                String dateWeek = String.valueOf(batchGetter.getDateWeek(org[0]));
                return new Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>(dateWeek + t._1.substring(t._1.indexOf(","), t._1.length()), t._2);
            }
        }).reduceByKey((kv1, kv2) -> {
            return kv1.add(kv2);
        });
        JavaRDD<Row> calculatedOee = map.mapPartitions(batchData -> {

            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            while (batchData.hasNext()) {
                Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> r = batchData.next();
                ArrayList<Object> objects = new ArrayList<>();

                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                //getTime_efficiency_rate getPerformance_efficiency_rate getBad_losses getYield_rate
                //getActual_output_qty getGood_product_qty

                String[] org = r._1.split(",");

                Float loading_time = floatValueOf(r._2.getLoading_time());
                Float npi_time = floatValueOf(r._2.getNpi_time());
                Float breakdown_time = floatValueOf(r._2.getBreakdown_time());
                Float die_failure_time = floatValueOf(r._2.getDie_failure_time());
                Float short_halt_time = floatValueOf(r._2.getShort_halt_time());
                Float test_equipment_time = floatValueOf(r._2.getTest_equipment_time());
                Float co_time = floatValueOf(r._2.getCo_time());
                Float stamping_change_material = floatValueOf(r._2.getStamping_change_material());
                Float molding_clean = floatValueOf(r._2.getMolding_clean());
                Float incoming_material_adverse = floatValueOf(r._2.getIncoming_material_adverse());
                Float wait_material = floatValueOf(r._2.getWait_material());
                Float energy_error = floatValueOf(r._2.getEnergy_error());
                Float equipment_error = floatValueOf(r._2.getEquipment_error());
                Float labour_error = floatValueOf(r._2.getLabour_error());
                Float other_error = floatValueOf(r._2.getOther_error());
                Float output_hours = floatValueOf(r._2.getOutput_hours());
                Float operation_time = floatValueOf(r._2.getOperation_time());
                Float good_product_qty = r._2.getGood_product_qty();
                Float actual_output_qty = r._2.getActual_output_qty();
                Float total_work_time = floatValueOf(r._2.getTotal_work_time());


                Float oee2Target = oee2Targets.get(org[2] + "," + org[3] + "," + org[4] + "," + org[5] + "," + org[6]);
                objects.add(System.currentTimeMillis() + "-" + UUID.randomUUID().toString());
                objects.add(Integer.valueOf(org[0]));
                objects.add(org[1]);
                objects.add(org[2]);
                objects.add(org[3]);
                objects.add(org[4]);
                objects.add(org[5]);
                objects.add(org[7]);
                objects.add(org[8]);
                objects.add(
                        batchGetter.formatFloat((good_product_qty / actual_output_qty) * (operation_time / total_work_time) * (output_hours / operation_time) * 100));
                objects.add(
                        batchGetter.formatFloat(
                                (loading_time / loading_time -
                                        (
                                                npi_time / loading_time
                                                        +
                                                        breakdown_time / loading_time
                                                        +
                                                        die_failure_time / loading_time
                                                        +
                                                        short_halt_time / loading_time
                                                        +
                                                        test_equipment_time / loading_time
                                                        +
                                                        co_time / loading_time
                                                        +
                                                        stamping_change_material / loading_time
                                                        +
                                                        molding_clean / loading_time
                                                        +
                                                        incoming_material_adverse / loading_time
                                                        +
                                                        wait_material / loading_time
                                                        +
                                                        energy_error / loading_time
                                                        +
                                                        equipment_error / loading_time
                                                        +
                                                        labour_error / loading_time
                                                        +
                                                        other_error / loading_time
                                        )
                                        -
                                        (
                                                loading_time / loading_time - output_hours / operation_time
                                                        +
                                                        loading_time / loading_time - good_product_qty / actual_output_qty
                                        )
                                ) * 100));
                objects.add(oee2Target == null ? 0.0f : oee2Target);
                objects.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_oee_week", objects));
            }
            return rows.iterator();
        });

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : calculatedOee.take(5)) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_week", calculatedOee, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_oee_week"),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_oee_week"));
    }

    public void calculateMonthProductionOee(JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> dwsProductionOeeEquipmentLineDDJavaPairRDD) throws Exception {

        JavaRDD<Row> calculatedOee = dwsProductionOeeEquipmentLineDDJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>, String, DpmDwsProductionOeeEquipmentLineDD>() {
            @Override
            public Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> call(Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> t) throws Exception {
                // KEY
                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                String[] org = t._1.split(",");
                String dateMonth = org[0].replace("-", "").substring(0, 6);
                return new Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>(dateMonth + t._1.substring(t._1.indexOf(","), t._1.length()), t._2);
            }
        }).reduceByKey((kv1, kv2) -> {
            return kv1.add(kv2);
        }).mapPartitions(batchData -> {

            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            while (batchData.hasNext()) {
                Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> r = batchData.next();
                ArrayList<Object> objects = new ArrayList<>();

                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                //getTime_efficiency_rate getPerformance_efficiency_rate getBad_losses getYield_rate
                //getActual_output_qty getGood_product_qty

                String[] org = r._1.split(",");

                Float loading_time = floatValueOf(r._2.getLoading_time());
                Float npi_time = floatValueOf(r._2.getNpi_time());
                Float breakdown_time = floatValueOf(r._2.getBreakdown_time());
                Float die_failure_time = floatValueOf(r._2.getDie_failure_time());
                Float short_halt_time = floatValueOf(r._2.getShort_halt_time());
                Float test_equipment_time = floatValueOf(r._2.getTest_equipment_time());
                Float co_time = floatValueOf(r._2.getCo_time());
                Float stamping_change_material = floatValueOf(r._2.getStamping_change_material());
                Float molding_clean = floatValueOf(r._2.getMolding_clean());
                Float incoming_material_adverse = floatValueOf(r._2.getIncoming_material_adverse());
                Float wait_material = floatValueOf(r._2.getWait_material());
                Float energy_error = floatValueOf(r._2.getEnergy_error());
                Float equipment_error = floatValueOf(r._2.getEquipment_error());
                Float labour_error = floatValueOf(r._2.getLabour_error());
                Float other_error = floatValueOf(r._2.getOther_error());
                Float output_hours = floatValueOf(r._2.getOutput_hours());
                Float operation_time = floatValueOf(r._2.getOperation_time());
                Float good_product_qty = r._2.getGood_product_qty();
                Float actual_output_qty = r._2.getActual_output_qty();
                Float total_work_time = floatValueOf(r._2.getTotal_work_time());

                Float oee2Target = oee2Targets.get(org[2] + "," + org[3] + "," + org[4] + "," + org[5] + "," + org[6]);
                objects.add(System.currentTimeMillis() + "-" + UUID.randomUUID().toString());
                objects.add(Integer.valueOf(org[0]));
                objects.add(org[1]);
                objects.add(org[2]);
                objects.add(org[3]);
                objects.add(org[4]);
                objects.add(org[5]);
                objects.add(org[7]);
                objects.add(org[8]);
                objects.add(batchGetter.formatFloat((good_product_qty / actual_output_qty) * (operation_time / total_work_time) * (output_hours / operation_time) * 100));
                objects.add(
                        batchGetter.formatFloat((
                        loading_time / loading_time -
                                (
                                        npi_time / loading_time
                                                +
                                                breakdown_time / loading_time
                                                +
                                                die_failure_time / loading_time
                                                +
                                                short_halt_time / loading_time
                                                +
                                                test_equipment_time / loading_time
                                                +
                                                co_time / loading_time
                                                +
                                                stamping_change_material / loading_time
                                                +
                                                molding_clean / loading_time
                                                +
                                                incoming_material_adverse / loading_time
                                                +
                                                wait_material / loading_time
                                                +
                                                energy_error / loading_time
                                                +
                                                equipment_error / loading_time
                                                +
                                                labour_error / loading_time
                                                +
                                                other_error / loading_time
                                )
                                -
                                (
                                        loading_time / loading_time - output_hours / operation_time
                                                +
                                                loading_time / loading_time - good_product_qty / actual_output_qty
                                )
                ) * 100));
                objects.add(oee2Target == null ? 0.0f : oee2Target);
                objects.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_oee_month", objects));
            }
            return rows.iterator();
        });

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : calculatedOee.take(5)) {
                System.out.println(row);
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_month", calculatedOee, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_oee_month"),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_oee_month"));
    }

    public void calculateQuarterProductionOee(JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> dwsProductionOeeEquipmentLineDDJavaPairRDD) throws Exception {
        JavaRDD<Row> calculatedOee = dwsProductionOeeEquipmentLineDDJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>, String, DpmDwsProductionOeeEquipmentLineDD>() {
            @Override
            public Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> call(Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> t) throws Exception {
                // KEY
                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                String[] org = t._1.split(",");
                String dateQuarter = org[0].substring(0, 4) + String.valueOf(batchGetter.getTargetDateQuarter(org[0], "-"));
                return new Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>(dateQuarter + t._1.substring(t._1.indexOf(","), t._1.length()), t._2);
            }
        }).mapPartitions(batchData -> {

            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            while (batchData.hasNext()) {
                Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> r = batchData.next();
                ArrayList<Object> objects = new ArrayList<>();

                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                //getTime_efficiency_rate getPerformance_efficiency_rate getBad_losses getYield_rate
                //getActual_output_qty getGood_product_qty

                String[] org = r._1.split(",");
                Float loading_time = floatValueOf(r._2.getLoading_time());
                Float npi_time = floatValueOf(r._2.getNpi_time());
                Float breakdown_time = floatValueOf(r._2.getBreakdown_time());
                Float die_failure_time = floatValueOf(r._2.getDie_failure_time());
                Float short_halt_time = floatValueOf(r._2.getShort_halt_time());
                Float test_equipment_time = floatValueOf(r._2.getTest_equipment_time());
                Float co_time = floatValueOf(r._2.getCo_time());
                Float stamping_change_material = floatValueOf(r._2.getStamping_change_material());
                Float molding_clean = floatValueOf(r._2.getMolding_clean());
                Float incoming_material_adverse = floatValueOf(r._2.getIncoming_material_adverse());
                Float wait_material = floatValueOf(r._2.getWait_material());
                Float energy_error = floatValueOf(r._2.getEnergy_error());
                Float equipment_error = floatValueOf(r._2.getEquipment_error());
                Float labour_error = floatValueOf(r._2.getLabour_error());
                Float other_error = floatValueOf(r._2.getOther_error());
                Float output_hours = floatValueOf(r._2.getOutput_hours());
                Float operation_time = floatValueOf(r._2.getOperation_time());
                Float good_product_qty = r._2.getGood_product_qty();
                Float actual_output_qty = r._2.getActual_output_qty();
                Float total_work_time = floatValueOf(r._2.getTotal_work_time());
                Float oee2Target = oee2Targets.get(org[2] + "," + org[3] + "," + org[4] + "," + org[5] + "," + org[6]);
                objects.add(System.currentTimeMillis() + "-" + UUID.randomUUID().toString());
                objects.add((Integer.valueOf(org[0].substring(0, 4)) * 10) + Integer.valueOf(org[0].substring(4, 5)));
                objects.add(org[1]);
                objects.add(org[2]);
                objects.add(org[3]);
                objects.add(org[4]);
                objects.add(org[5]);
                objects.add(org[7]);
                objects.add(org[8]);
                objects.add(batchGetter.formatFloat((good_product_qty / actual_output_qty) * (operation_time / total_work_time) * (output_hours / operation_time) * 100));
                objects.add(
                        batchGetter.formatFloat((
                        loading_time / loading_time -
                                (
                                        npi_time / loading_time
                                                +
                                                breakdown_time / loading_time
                                                +
                                                die_failure_time / loading_time
                                                +
                                                short_halt_time / loading_time
                                                +
                                                test_equipment_time / loading_time
                                                +
                                                co_time / loading_time
                                                +
                                                stamping_change_material / loading_time
                                                +
                                                molding_clean / loading_time
                                                +
                                                incoming_material_adverse / loading_time
                                                +
                                                wait_material / loading_time
                                                +
                                                energy_error / loading_time
                                                +
                                                equipment_error / loading_time
                                                +
                                                labour_error / loading_time
                                                +
                                                other_error / loading_time
                                )
                                -
                                (
                                        loading_time / loading_time - output_hours / operation_time
                                                +
                                                loading_time / loading_time - good_product_qty / actual_output_qty
                                )
                ) * 100));
                objects.add(oee2Target == null ? 0.0f : oee2Target);
                objects.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_oee_quarter", objects));
            }
            return rows.iterator();
        });
        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : calculatedOee.take(5)) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_quarter", calculatedOee, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_oee_quarter"),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_oee_quarter"));
    }

    public void calculateYearProductionOee(JavaPairRDD<String, DpmDwsProductionOeeEquipmentLineDD> dwsProductionOeeEquipmentLineDDJavaPairRDD) throws Exception {

        JavaRDD<Row> calculatedOee = dwsProductionOeeEquipmentLineDDJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>, String, DpmDwsProductionOeeEquipmentLineDD>() {
            @Override
            public Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> call(Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> t) throws Exception {
                // KEY
                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                String[] org = t._1.split(",");
                String year = org[0].substring(0, 4);
                return new Tuple2<String, DpmDwsProductionOeeEquipmentLineDD>(year + t._1.substring(t._1.indexOf(","), t._1.length()), t._2);
            }
        }).reduceByKey((kv1, kv2) -> {
            return kv1.add(kv2);
        }).mapPartitions(batchData -> {

            ArrayList<Row> rows = new ArrayList<>();
            BeanGetter beanGetter = MetaGetter.getBeanGetter();

            while (batchData.hasNext()) {
                Tuple2<String, DpmDwsProductionOeeEquipmentLineDD> r = batchData.next();
                ArrayList<Object> objects = new ArrayList<>();

                // b.getWork_dt(), b.getWork_shift(), b.getSite_code(), b.getLevel_code(), b.getFactory_code(), b.getProcess_code(),
                // b.getArea_code(), b.getLine_code(), b.getMachine_id()
                //getTime_efficiency_rate getPerformance_efficiency_rate getBad_losses getYield_rate
                //getActual_output_qty getGood_product_qty

                String[] org = r._1.split(",");
                Float loading_time = floatValueOf(r._2.getLoading_time());
                Float npi_time = floatValueOf(r._2.getNpi_time());
                Float breakdown_time = floatValueOf(r._2.getBreakdown_time());
                Float die_failure_time = floatValueOf(r._2.getDie_failure_time());
                Float short_halt_time = floatValueOf(r._2.getShort_halt_time());
                Float test_equipment_time = floatValueOf(r._2.getTest_equipment_time());
                Float co_time = floatValueOf(r._2.getCo_time());
                Float stamping_change_material = floatValueOf(r._2.getStamping_change_material());
                Float molding_clean = floatValueOf(r._2.getMolding_clean());
                Float incoming_material_adverse = floatValueOf(r._2.getIncoming_material_adverse());
                Float wait_material = floatValueOf(r._2.getWait_material());
                Float energy_error = floatValueOf(r._2.getEnergy_error());
                Float equipment_error = floatValueOf(r._2.getEquipment_error());
                Float labour_error = floatValueOf(r._2.getLabour_error());
                Float other_error = floatValueOf(r._2.getOther_error());
                Float output_hours = floatValueOf(r._2.getOutput_hours());
                Float operation_time = floatValueOf(r._2.getOperation_time());
                Float good_product_qty = r._2.getGood_product_qty();
                Float actual_output_qty = r._2.getActual_output_qty();
                Float total_work_time = floatValueOf(r._2.getTotal_work_time());
                Float oee2Target = oee2Targets.get(org[2] + "," + org[3] + "," + org[4] + "," + org[5] + "," + org[6]);
                objects.add(System.currentTimeMillis() + "-" + UUID.randomUUID().toString());
                objects.add(Integer.valueOf(org[0]));
                objects.add(org[1]);
                objects.add(org[2]);
                objects.add(org[3]);
                objects.add(org[4]);
                objects.add(org[5]);
                objects.add(org[7]);
                objects.add(org[8]);
                objects.add(batchGetter.formatFloat((good_product_qty / actual_output_qty) * (operation_time / total_work_time) * (output_hours / operation_time) * 100));
                objects.add(
                        batchGetter.formatFloat((
                        loading_time / loading_time -
                                (
                                        npi_time / loading_time
                                                +
                                                breakdown_time / loading_time
                                                +
                                                die_failure_time / loading_time
                                                +
                                                short_halt_time / loading_time
                                                +
                                                test_equipment_time / loading_time
                                                +
                                                co_time / loading_time
                                                +
                                                stamping_change_material / loading_time
                                                +
                                                molding_clean / loading_time
                                                +
                                                incoming_material_adverse / loading_time
                                                +
                                                wait_material / loading_time
                                                +
                                                energy_error / loading_time
                                                +
                                                equipment_error / loading_time
                                                +
                                                labour_error / loading_time
                                                +
                                                other_error / loading_time
                                )
                                -
                                (
                                        loading_time / loading_time - output_hours / operation_time
                                                +
                                                loading_time / loading_time - good_product_qty / actual_output_qty
                                )
                ) * 100));
                objects.add(oee2Target == null ? 0.0f : oee2Target);
                objects.add(etl_time);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ads_production_oee_year", objects));
            }
            return rows.iterator();
        });

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            for (Row row : calculatedOee.take(5)) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_production_oee_year", calculatedOee, MetaGetter.getBeanGetter().creDeftSchemaMap("dpm_ads_production_oee_year"),
                MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ads_production_oee_year"));
    }

    public void clearTable() {
        sqlContext.dropTempTable("dpm_ods_production_target_values");
        sqlContext.clearCache();
    }


    public void loadTargets(String lastDay) throws Exception {
        oee2Targets.clear();
        LoadKpiTarget.loadProductionTarget(lastDay).filter(r -> {
            return ("WH".equals(r.get(2)) || "CQ".equals(r.get(2))) && "L5".equals(r.get(3)) && !"all".equals(r.get(4)) && !"all".equals(r.get(5)) && !"all".equals(r.get(6))/* && "all".equals(r.get(7))*/;
        }).mapToPair(new PairFunction<ArrayList<String>, String, Float>() {
            @Override
            public Tuple2<String, Float> call(ArrayList<String> r) throws Exception {
                return new Tuple2<String, Float>(
                        batchGetter.getStrArrayOrg(",", "N/A",
                                r.get(2), r.get(3), r.get(4), r.get(5), r.get(6)
                        )
                        , batchGetter.formatFloat(r.get(15)));
            }
        }).collectAsMap().forEach(new BiConsumer<String, Float>() {
            @Override
            public void accept(String k, Float v) {
                oee2Targets.put(k, v * 100);
            }
        });
    }


    public Float floatValueOf(String floatStr) {
        try {
            return Float.valueOf(floatStr);
        } catch (Exception e) {
            return 0.0f;
        }
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
