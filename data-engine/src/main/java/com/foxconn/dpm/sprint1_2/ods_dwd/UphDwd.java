package com.foxconn.dpm.sprint1_2.ods_dwd;

import com.foxconn.dpm.sprint1_2.ods_dwd.beans.DwdUphCtOutput;
import com.foxconn.dpm.sprint1_2.ods_dwd.beans.UphOdsTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Description:  UPH的dwd计算逻辑
 * 抽取表dpm_ods_production_uph_target_lten的数据
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/15
 */
public class UphDwd extends DPSparkBase {

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");


        /**
         * 取得od中的表数据
         * dpm_ods_production_uph_target_lten 不加盐
         * rowkey格式 dayid:platform
         * dayid解析格式yyyy-mm-dd,取最近的dayid一天的数据
         */
        Scan scan = new Scan();
        scan.withStartRow("!".getBytes());
        scan.withStopRow("~".getBytes());
        JavaRDD<Result> uphData = DPHbase.rddRead("dpm_ods_production_uph_target_lten", scan, true).persist(StorageLevel.MEMORY_AND_DISK());
        //清洗，取dayid最近的数据
        JavaRDD<UphOdsTarget> uphFormatData = uphData.map(new Function<Result, UphOdsTarget>() {
            @Override
            public UphOdsTarget call(Result result) throws Exception {
                final String family = "DPM_ODS_PRODUCTION_UPH_TARGET_LTEN";
                String platfrom = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("platform")));
                String line_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("line_code")));
                String update_dt_str = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_dt")));
                String update_by = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_by")));
                String data_from = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("data_from")));
                String uph_target = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("uph_target")));
                String site_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("site_code")));
                if (site_code == null || "".equals(site_code)){
                    site_code = "WH";
                }
                try {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    String day_id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("day_id")));
                    //检验日期格式是否规范
                    format.parse(day_id);
                    Long update_dt = Long.valueOf(update_dt_str);
                    return new UphOdsTarget(day_id, platfrom, line_code, uph_target, update_dt, update_by, data_from, site_code);
                } catch (Exception e) {
                    return null;
                }
            }
        }).filter(f -> f != null).keyBy(r -> r.getPlatfrom() + r.getLine_code()).reduceByKey((v1, v2) -> {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            if (format.parse(v1.getDay_id()).after(format.parse(v2.getDay_id()))) {
                return v1;
            } else {
                return v2;
            }
        }).map(r -> r._2());
        uphFormatData.count();
//        JavaRDD<DwdUphCtOutput> output = uphFormatData.flatMap(new FlatMapFunction<UphOdsTarget, DwdUphCtOutput>() {
//            @Override
//            public Iterator<DwdUphCtOutput> call(UphOdsTarget odsTarget) throws Exception {
//                List<DwdUphCtOutput> data = new ArrayList<>();
//                String cycleTime = "0";
//                try {
//                    cycleTime = String.valueOf(3600.00f / Integer.valueOf(odsTarget.getLineb()));
//                } catch (Exception e) {
//                }
//                //根据line类型，一条ods数据固定拆分6条数据
//                DwdUphCtOutput lineb = new DwdUphCtOutput();
//                lineb.setLine_code("LINE_B");
//                lineb.setPlantform(odsTarget.getPlatfrom());
//                lineb.setCycle_time(cycleTime);
//
//                DwdUphCtOutput linec = new DwdUphCtOutput();
//                linec.setLine_code("LINE_C");
//                linec.setPlantform(odsTarget.getPlatfrom());
//                linec.setCycle_time(cycleTime);
//
//                DwdUphCtOutput lined = new DwdUphCtOutput();
//                lined.setLine_code("LINE_D");
//                lined.setPlantform(odsTarget.getPlatfrom());
//                lined.setCycle_time(cycleTime);
//
//                DwdUphCtOutput linee = new DwdUphCtOutput();
//                linee.setLine_code("LINE_E");
//                linee.setPlantform(odsTarget.getPlatfrom());
//                linee.setCycle_time(cycleTime);
//
//                DwdUphCtOutput linef = new DwdUphCtOutput();
//                linef.setLine_code("LINE_A");
//                linef.setPlantform(odsTarget.getPlatfrom());
//                linef.setCycle_time(cycleTime);
//
//                DwdUphCtOutput lineg = new DwdUphCtOutput();
//                lineg.setLine_code("LINE_B");
//                lineg.setPlantform(odsTarget.getPlatfrom());
//                lineg.setCycle_time(cycleTime);
//
//                data.add(lineb);
//                data.add(linec);
//                data.add(lined);
//                data.add(linee);
//                data.add(linef);
//                data.add(lineg);
//
//                return data.iterator();
//            }
//        }).persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<DwdUphCtOutput> output = uphFormatData.map(new Function<UphOdsTarget, DwdUphCtOutput>() {
            @Override
            public DwdUphCtOutput call(UphOdsTarget odsTarget) throws Exception {
                DwdUphCtOutput target = new DwdUphCtOutput();
                if ("LINE_F".equals(odsTarget.getLine_code())) {
                    target.setLine_code("LINE_A");
                } else if ("LINE_G".equals(odsTarget.getLine_code())) {
                    target.setLine_code("LINE_B");
                } else {
                    target.setLine_code(odsTarget.getLine_code());
                }
                target.setPlantform(odsTarget.getPlatfrom());
                String cycleTime = "0";
                try {
                    cycleTime = String.valueOf(3600.00f / Integer.valueOf(odsTarget.getUph_target()));
                } catch (Exception e) {
                }
                target.setCycle_time(cycleTime);
                target.setSite_code(odsTarget.getSite_code());
                return target;
            }
        }).persist(StorageLevel.MEMORY_AND_DISK());

        try {
            List<DwdUphCtOutput> take6 = output.take(70);
            for (DwdUphCtOutput ot : take6) {
                System.out.println(ot);
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>UphCt Source End<<<==============================");
        final Long update_dt = System.currentTimeMillis();
        JavaRDD<Put> toWriteData = output.mapPartitions(i -> {
            final String family = "DPM_DWD_PRODUCTION_STANDARY_CT";
            List puts = new ArrayList<>();
            i.forEachRemaining(r -> {
                String rowkey = r.getSite_code() + ":" + r.getLevel_code() + ":" + r.getLine_code() + ":" + r.getPlantform() + ":" + r.getPart_no();
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("site_code"), Bytes.toBytes(r.getSite_code()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("level_code"), Bytes.toBytes(r.getLevel_code()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("factory_code"), Bytes.toBytes(r.getFactory_code()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("process_code"), Bytes.toBytes(r.getProcess_code()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("line_code"), Bytes.toBytes(r.getLine_code()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("platform"), Bytes.toBytes(r.getPlantform()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("part_no"), Bytes.toBytes(r.getPart_no()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("cycle_time"), Bytes.toBytes(r.getCycle_time()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_dt"), Bytes.toBytes(update_dt.toString()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("update_by"), Bytes.toBytes("F20DL"));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("data_from"), Bytes.toBytes("DWD"));
                puts.add(put);
            });
            return puts.iterator();
        });
        System.out.println(toWriteData.count());
        System.out.println("==============================>>>toWriteData Calculate End<<<==============================");
        try {
            DPHbase.rddWrite("dpm_dwd_production_standary_ct", toWriteData);
        } catch (Exception e) {
            System.out.println("===========================>>>>>>>>>>>>>>>>>>>Write No Data Or API Err<<<<<<<<<<<<<<<<<<<<================");
        }
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
