package com.foxconn.dpm.sprint5.ods_dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.foxconn.dpm.sprint5.ods_dwd.bean.DWDProductionOutputStreaming;
import com.foxconn.dpm.sprint5.ods_dwd.bean.ODSProductionlfiveStreaming;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.DlFunction;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import com.tm.dl.javasdk.dpspark.streaming.KafkaSink;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import org.apache.spark.util.CollectionAccumulator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data
 */
//  造kafka测试数据 post接口  ：  http://10.60.136.156:8089/api/dlapiservice/v1/kafka
//         {
//        "topic": "chen_topic",
//        "msg": "{\"dpHbasecolumns\":[{\"columnName\":\"datajson\",\"family\":\"DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE_BAK\",\"value\":\"{\\\"site_code\\\":\\\"WH\\\",\\\"level_code\\\":\\\"L11\\\",\\\"factory_code\\\":\\\"DT1\\\",\\\"process_code\\\":\\\"Assy\\\",\\\"area_code\\\":\\\"11\\\",\\\"line_code\\\":\\\"LINE_A\\\",\\\"work_shift\\\":\\\"D\\\",\\\"work_dt\\\":\\\"2020-07-06\\\",\\\"wo\\\":\\\"111\\\",\\\"workorder_type\\\":\\\"1111\\\",\\\"part_no\\\":\\\"51452sd\\\",\\\"customer\\\":\\\"HP\\\",\\\"model_no\\\":\\\"11111\\\",\\\"sn\\\":\\\"11111\\\",\\\"station_code\\\":\\\"text\\\",\\\"station_name\\\":\\\"text\\\",\\\"is_pass\\\":\\\"1\\\",\\\"is_fail\\\":\\\"1\\\",\\\"scan_by\\\":\\\"cj\\\",\\\"scan_dt\\\":\\\"1594026130000\\\",\\\"update_dt\\\":\\\"1594026130000\\\",\\\"update_by\\\":\\\"sfc\\\",\\\"data_from\\\":\\\"cj\\\"}\"}],\"rowKey\":\"03:1575525600500:DAX1:D624:2a6ae81e-4738-4801-87c6-fcfeb89bdbb0\"}"
//        }

//    运行参数：测试环境
//    {\"dprunclass\":\"com.foxconn.dpm.sprint5.ods_dwd.ProductionSteaming\",\"dpappName\":\"streaming-dwd\",\"dpuserid\":\"8F70E561-ED63-4B88-A343-UIDE89FF9156\",\"dpmaster\":\"local[*]\",\"pmClass\":\"com.tm.dl.javasdk.dpspark.common.ProdPermissionManager\",\"dpType\":\"streaming\",\"dpStreaming\":{\"topics\":\"chen_topic\",\"groupId\":\"chen\",\"batchDuration\":\"4\",\"windowDurationMultiple\":\"4\",\"sliverDurationMultiple\":\"4\"}}

// host 环境QA     sparkSDK   http://10.60.136.145:806/jar/sparksdk/


@Slf4j
public class ProductionSteaming extends DPSparkBase {
    private static ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
    private static SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
    private static StringBuilder sb = new StringBuilder();

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {
        //总体逻辑，每次从流中获取ods中的数据，将缓存数据存到hbase并发送到kafka中，缓存清空
        final Broadcast kafkaProducer = DPSparkApp.getContext().broadcast(KafkaSink.apply(new Properties() {{
            putAll(dpStreaming.getKafkaParams());
        }}));
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(Level.WARN);
        //声明累加器缓存
        CollectionAccumulator<ODSProductionlfiveStreaming> hbaseWriteOdsList = DPSparkApp.getContext().sc().collectionAccumulator();
        CollectionAccumulator<DWDProductionOutputStreaming> kafkaWriteList = DPSparkApp.getContext().sc().collectionAccumulator();

        //接收流式数据
        dpStreaming.startJob(rdd -> {
            //接收ods的数据
            //"{\"dpHbasecolumns\":[{\"columnName\":\"datajson\",\"family\":\"DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE_BAK\",\"value\":\"{\"site_code\":\"WH\",\"level_code\":\"L5\",\"factory_code\":\"DT1\",\"process_code\":\"Assy\",\"area_code\":\"11\",\"line_code\":\"LINE_A\",\"work_shift\":\"D\",\"work_dt\":\"2020-07-06\",\"wo\":\"111\",\"workorder_type\":\"1111\",\"part_no\":\"51452sd\",\"customer\":\"HP\",\"model_no\":\"11111\",\"sn\":\"11111\",\"station_code\":\"text\",\"station_name\":\"text\",\"is_pass\":\"1\",\"is_fail\":\"1\",\"scan_by\":\"cj\",\"scan_dt\":\"1594026130000\",\"update_dt\":\"1594026130000\",\"update_by\":\"sfc\",\"data_from:\"cj\"}}],\"rowKey\":\"03:1575525600500:DAX1:D624:2a6ae81e-4738-4801-87c6-fcfeb89bdbb0\"}"
            rdd.foreachPartition(i -> {
                i.forEachRemaining(record -> {
                    try {
                        JSONObject odsData = JSON.parseObject(record.value().toString());
                        System.out.println(odsData);
                        String sourceEditDate = odsData.getString("rowKey").split("\\:")[1];
                        String datajson = odsData.getJSONArray("dpHbasecolumns").getJSONObject(0).getString("value");

                        ODSProductionlfiveStreaming odsProductionlfive = JSONObject.toJavaObject((JSON) JSON.parse(datajson), ODSProductionlfiveStreaming.class);
                        if ("".equals(odsProductionlfive.getSite_code())) {
                            odsProductionlfive.setSite_code("N/A");
                        }
                        switch (odsProductionlfive.getLevel_code()) {
                            case "BU1001":
                                odsProductionlfive.setSite_code("WH");
                                odsProductionlfive.setLevel_code("L5");
                                break;
                            default:
                                break;
                        }
                        //赋值dwd逻辑
                        DWDProductionOutputStreaming dwdoutput = null;
                        try {
                            dwdoutput = getDwdoutput(odsProductionlfive);
                        } catch (ParseException e) {
                            DPSparkApp.doThrow(e);
                            log.error("处理逻辑有问题！！！");
                        }
                        hbaseWriteOdsList.add(odsProductionlfive);
                        kafkaWriteList.add(dwdoutput);

                    } catch (Exception e) {
                        DPSparkApp.doThrow(e);
                        log.error("数据对象转换异常" + e.toString());
                    }

                });
            });

            try {
                //同时保存到ods表，发送kafka
                List<ODSProductionlfiveStreaming> hbaseWriteListValue = hbaseWriteOdsList.value();
                Dataset<Row> dataFrameOds = DPSparkApp.getSession().createDataFrame(hbaseWriteListValue, ODSProductionlfiveStreaming.class);
                DPHbase.rddWrite("dpm_ods_production_output_day_lfive_bak", dataFrameOds, new DlFunction<GenericRowWithSchema, String>() {
                    @SneakyThrows
                    @Override
                    public String apply(GenericRowWithSchema genericRowWithSchema) {
                        //addsalt+scan_dt+site_code+station_code+is_fail+UUID
                        String rowkey = genericRowWithSchema.getAs("scan_dt").toString() + ":" + genericRowWithSchema.getAs("site_code").toString() + ":" + genericRowWithSchema.getAs("station_code").toString() + ":" + genericRowWithSchema.getAs("is_fail").toString() + ":" + UUID.randomUUID().toString().replace("-", "");
                        return consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
                    }
                });
                //发送到kafka
                kafkaWriteList.value().forEach(dwdoutput -> {
                    ((KafkaSink) kafkaProducer.value()).sendAll("cj_topic", JSON.toJSONString(dwdoutput));
                });
                List<Put> dwdOutput = new ArrayList<>();
                hbaseWriteListValue.forEach(bean -> dwdOutput.add(getPutJavaRdd(bean)));
                log.info("dwdOutput count is " + dwdOutput.size());
                DPHbase.rddWrite("dpm_dwd_production_output_bak", DPSparkApp.getContext().parallelize(dwdOutput));
            } catch (Exception e) {
                log.error("dwd，tleol数据处理异常", e);
                DPSparkApp.doThrow(e);
            } finally {
                hbaseWriteOdsList.reset();
                kafkaWriteList.reset();
            }
        });

    }


    private DWDProductionOutputStreaming getDwdoutput(ODSProductionlfiveStreaming ODSProduction) throws ParseException {
        return new DWDProductionOutputStreaming(
                ODSProduction.getSite_code(),
                ODSProduction.getLevel_code(),
                ODSProduction.getFactory_code(),
                ODSProduction.getProcess_code(),
                ODSProduction.getArea_code(),
                ODSProduction.getLine_code(),
                "",
                ODSProduction.getPart_no(),
                "",
                "",
                ODSProduction.getCustomer(),
                ODSProduction.getWo(),
                ODSProduction.getWorkorder_type(),
                ODSProduction.getWork_dt(),
                ODSProduction.getWork_shift(),
                ODSProduction.getSn(),
                ODSProduction.getStation_code(),
                ODSProduction.getStation_name(),
                ODSProduction.getIs_fail(),
                ODSProduction.getScan_by(),
                ODSProduction.getScan_dt(),
                "1",
                "cj",
                String.valueOf(System.currentTimeMillis()),
                "production_output_day_lfive"
        );
    }


    private Put getPutJavaRdd(ODSProductionlfiveStreaming ODSProduction) {
        //addsalt+WorkDT+Site+LevelCode+Line+IsFail+UUID
        String baseRowKey = null;
        try {
            baseRowKey = sb.append(String.valueOf(formatWorkDt.parse(ODSProduction.getWork_dt()).getTime())).append(":").append(ODSProduction.getSite_code()).append(":").append(ODSProduction.getLevel_code())
                    .append(":").append(ODSProduction.getLine_code()).append(":").append(ODSProduction.getIs_fail()).append(":").append(UUID.randomUUID().toString().replaceAll("-", "")).toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String salt = consistentHashLoadBalance.selectNode(baseRowKey);
        Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("site_code"), Bytes.toBytes(ODSProduction.getSite_code()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("level_code"), Bytes.toBytes(ODSProduction.getLevel_code()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("factory_code"), Bytes.toBytes(ODSProduction.getFactory_code()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("process_code"), Bytes.toBytes(ODSProduction.getProcess_code()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("area_code"), Bytes.toBytes(ODSProduction.getArea_code()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("line_code"), Bytes.toBytes(ODSProduction.getLine_code()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("machine_id"), Bytes.toBytes(""));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("part_no"), Bytes.toBytes(ODSProduction.getPart_no()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("sku"), Bytes.toBytes(""));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("platform"), Bytes.toBytes(""));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("customer"), Bytes.toBytes(ODSProduction.getCustomer()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("wo"), Bytes.toBytes(ODSProduction.getWo()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("workorder_type"), Bytes.toBytes(ODSProduction.getWorkorder_type()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("work_dt"), Bytes.toBytes(ODSProduction.getWork_dt()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("work_shift"), Bytes.toBytes(ODSProduction.getWork_shift()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("sn"), Bytes.toBytes(ODSProduction.getSn()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("station_code"), Bytes.toBytes(ODSProduction.getStation_code()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("station_name"), Bytes.toBytes(ODSProduction.getStation_name()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("is_fail"), Bytes.toBytes(ODSProduction.getIs_fail()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("scan_by"), Bytes.toBytes(ODSProduction.getScan_by()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("scan_dt"), Bytes.toBytes(ODSProduction.getScan_dt()));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("output_qty"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("update_dt"), Bytes.toBytes("cj"));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("update_by"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
        put.addColumn(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT_BAK"), Bytes.toBytes("data_from"), Bytes.toBytes("production_output_day_lfive"));
        sb.delete(0, sb.length());
        return put;
    }

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

    }
}
