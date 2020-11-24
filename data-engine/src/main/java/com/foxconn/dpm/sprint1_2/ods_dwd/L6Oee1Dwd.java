package com.foxconn.dpm.sprint1_2.ods_dwd;

import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.execution.columnar.NULL;
import scala.Tuple11;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-05-26
 * L6oee 计算基础表抽取
 * 抽取 dpm_ods_production_equipment_day_lsix  L6标准CT , 放入dwd表 dpm_dwd_production_standary_ct 标准CT
 */
public class L6Oee1Dwd extends DPSparkBase {
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        System.out.println("==============================>>>Programe Start<<<==============================");
        Date today = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = simpleDateFormat.format(today);

        JavaRDD<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> standaryCtRDD = DPHbase.rddRead("dpm_ods_production_standary_ct_lsix", new Scan(), true).map(r -> {

            String site_code = Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_STANDARY_CT_LIST"), Bytes.toBytes("site_code")));
            if (site_code == null) {
                site_code = "WH";
            }
            return new Tuple11<>(
                    site_code,
                    "L6",
                    "",
                    "",
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_STANDARY_CT_LIST"), Bytes.toBytes("line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_STANDARY_CT_LIST"), Bytes.toBytes("plantform"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_STANDARY_CT_LIST"), Bytes.toBytes("part_no"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_STANDARY_CT_LIST"), Bytes.toBytes("smt_ct"))),
                    String.valueOf(System.currentTimeMillis()),
                    "cj",
                    "dpm_ods_production_standary_ct_lsix"
            );
        });
        standaryCtRDD.take(5).forEach(r -> System.out.println(r));
        System.out.println("读取数据完毕");
        JavaRDD<Put> putJavaRdd = standaryCtRDD.mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            while (r.hasNext()) {
                //site_code+level_code+line_code+platform+part_no
                Tuple11<String, String, String, String, String, String, String, String, String, String, String> line = r.next();
                String baseRowKey = sb.append(line._1()).append(":").append(line._2()).append(":").append(line._5()).append(":").append(line._6()).append(":").append(line._7()).toString();
                //dpm_dwd_safety_detail
                Put put = new Put(Bytes.toBytes(baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("site_code"), Bytes.toBytes(line._1()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("level_code"), Bytes.toBytes(line._2()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("factory_code"), Bytes.toBytes(line._3()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("process_code"), Bytes.toBytes(line._4()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("line_code"), Bytes.toBytes(line._5()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("plantform"), Bytes.toBytes(line._6()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("part_no"), Bytes.toBytes(line._7()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("cycle_time"), Bytes.toBytes(line._8()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("update_dt"), Bytes.toBytes(line._9()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("update_by"), Bytes.toBytes(line._10()));
                put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_STANDARY_CT"), Bytes.toBytes("data_from"), Bytes.toBytes(line._11()));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        // dpm_ods_production_standary_ct_lsix写入Dwd
        standaryCtRDD.take(5).forEach(r -> System.out.println(r));
        DPHbase.rddWrite("dpm_dwd_production_standary_ct", putJavaRdd);
        System.out.println("数据写入dpm_dwd_production_standary_ct完毕");

        System.out.println("==============================>>>Programe End<<<==============================");
        DPSparkApp.stop();
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
