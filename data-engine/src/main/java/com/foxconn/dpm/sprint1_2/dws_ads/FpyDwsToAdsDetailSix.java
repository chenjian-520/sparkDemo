package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.FtyByDayBean;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.springframework.util.StringUtils;
import scala.Tuple19;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * @author wxj
 * @date 2020/1/14 17:29
 * <p>
 * L6FPY 一次良率
 * 输入表： 抽取dws表 dpm_dws_production_pass_station_dd 日partno Line工站過站表 (L6 FPY因子數據  L10 FPY因子數據)
 * 输出表：放入ads表 dpm_ads_quality_fpy_detail_day/week/month/quarter/year  L6FPY
 */
public class FpyDwsToAdsDetailSix extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //业务逻辑
        ftyDetail();
        //释放资源
        DPSparkApp.stop();
    }

    public void ftyDetail() throws Exception {

        String yesterday = batchGetter.getStDateDayAdd(-1, "-");
        String today = batchGetter.getStDateDayAdd(1, "-");
        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
        String todayStamp = batchGetter.getStDateDayStampAdd(1);
        System.out.println("==============================>>>Programe Start<<<==============================");
        JavaRDD<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                rdd = DPHbase.saltRddRead("dpm_dws_production_pass_station_dd", yesterdayStamp, todayStamp, new Scan(), true).map(r -> {
            return new Tuple19<>(
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("site_code")))),//1
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("level_code")))),//2
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("factory_code")))),//3
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("process_code")))),//4
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("area_code")))),//5
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("line_code")))),//6
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("work_dt")))),//7
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("work_shift")))),//8
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("sku")))),//9
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("part_no")))),//10
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("platform")))),//11
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_code")))),//12
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("station_name")))),//13
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("fail_count")))),//14
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("total_count")))),//15
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("customer")))),//16
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("update_dt")))),//17
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("update_by")))),//18
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_PRODUCTION_PASS_STATION_DD"), Bytes.toBytes("data_from"))))//19
            );
        }).keyBy(r -> {
            return batchGetter.getStrArrayOrg(",", "-",
                    r._1(), r._2(), r._3(), r._4(), r._5(), r._6(), r._9(), r._11(), r._16(), r._7(), r._8(), r._10(), r._12()
            );
        }).reduceByKey((kv1, kv2) -> {
            return Long.valueOf(kv1._17()) > Long.valueOf(kv2._17()) ? kv1 : kv2;
        }).map(t -> {
            return t._2;
        }).filter(r ->
                yesterday.equals(r._7()) && "L6".equals(r._2())
                        && ("DELL".equals(r._16()) || "LENOVO".equals(r._16()) || "Lenovo_CODE".equals(r._16()) || "HP".equals(r._16()))
        );
        rdd.take(100).forEach(r -> System.out.println(r));

        //获取fpy的target
        String day = batchGetter.getStDateDayAdd(1, "-");
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("2020-04-15"));
        scan.withStopRow(Bytes.toBytes(day));
        String target = DPHbase.rddRead("dpm_ods_production_target_values", scan, true).map(r -> {
            return new Tuple2<>(
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("fpy")))),//1
                    FtyDwsToAdsSix.emptyStrNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_ODS_PRODUCTION_TARGET_VALUE"), Bytes.toBytes("day_id"))))//2

            );
        }).filter(r -> (!StringUtils.isEmpty(r._1()))).sortBy(r -> r._2(), false, 1).take(1).iterator().next()._1();

        String etl = String.valueOf(System.currentTimeMillis());
        JavaRDD<FtyByDayBean> list = rdd.groupBy(r -> {
            return new Tuple2(r._12(), r._16());
        }).map(r -> {
            //总数
            int pass = 0;
            int ng = 0;
            Iterator<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                    iterator = r._2.iterator();
            while (iterator.hasNext()) {
                Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>
                        next = iterator.next();
                ng += Integer.valueOf(next._14());
                pass += Integer.valueOf(next._15());
            }
            FtyByDayBean ftyBean = new FtyByDayBean();
            ftyBean.setId(UUID.randomUUID().toString());
            ftyBean.setFpy_target(Float.valueOf(target) * 100);
            ftyBean.setEtl_time(etl);
            ftyBean.setLevel_code("L6");
            ftyBean.setSite_code("WH");
            ftyBean.setWork_date(yesterday);
            ftyBean.setPass_qty(pass);
            ftyBean.setNg_qty(ng);
            ftyBean.setStation_code(r._1._1().toString());
            ftyBean.setCustomer_code(r._1._2().toString());
            return ftyBean;
        });

        list.take(100).forEach(r -> System.out.println(r));
        HashMap<String, StructField> schemaList = new HashMap<>(16);
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("level_code", DataTypes.createStructField("level_code", DataTypes.StringType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("work_date", DataTypes.createStructField("work_date", DataTypes.StringType, true));
        schemaList.put("fpy_target", DataTypes.createStructField("fpy_target", DataTypes.FloatType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));
        schemaList.put("ng_qty", DataTypes.createStructField("ng_qty", DataTypes.IntegerType, true));
        schemaList.put("pass_qty", DataTypes.createStructField("pass_qty", DataTypes.IntegerType, true));
        schemaList.put("customer_code", DataTypes.createStructField("customer_code", DataTypes.StringType, true));
        schemaList.put("station_code", DataTypes.createStructField("station_code", DataTypes.StringType, true));
        Class.forName("com.mysql.jdbc.Driver");

        SparkSession sqlContext = DPSparkApp.getSession();
        Dataset<Row> fpySet = sqlContext.createDataFrame(list, FtyByDayBean.class);

        DPMysql.commonOdbcWriteBatch("dp_ads", "dpm_ads_quality_fpy_detail_day", fpySet.toJavaRDD(), schemaList, fpySet.schema());

        System.out.println("==============================>>>Programe End<<<==============================");

    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
