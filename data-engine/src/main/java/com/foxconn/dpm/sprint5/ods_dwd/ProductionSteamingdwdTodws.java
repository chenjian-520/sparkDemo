package com.foxconn.dpm.sprint5.ods_dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.foxconn.dpm.sprint5.ods_dwd.bean.DWDProductionOutputStreaming;
import com.foxconn.dpm.sprint5.ods_dwd.bean.DWSProductionOutputStreaming;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.DlFunction;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import com.tm.dl.javasdk.dpspark.streaming.KafkaSink;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.util.CollectionAccumulator;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 */
//  造kafka测试数据 post接口  ：  http://10.60.136.156:8089/api/dlapiservice/v1/kafka
//        {
//        "topic": "chen_topic",
//        "msg": "{\"dpHbasecolumns\":[{\"columnName\":\"datajson\",\"family\":\"DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE_BAK\",\"value\":\"{\\\"site_code\\\":\\\"WH\\\",\\\"level_code\\\":\\\"L11\\\",\\\"factory_code\\\":\\\"DT1\\\",\\\"process_code\\\":\\\"Assy\\\",\\\"area_code\\\":\\\"11\\\",\\\"line_code\\\":\\\"LINE_A\\\",\\\"work_shift\\\":\\\"D\\\",\\\"work_dt\\\":\\\"2020-07-06\\\",\\\"wo\\\":\\\"111\\\",\\\"workorder_type\\\":\\\"1111\\\",\\\"part_no\\\":\\\"51452sd\\\",\\\"customer\\\":\\\"HP\\\",\\\"model_no\\\":\\\"11111\\\",\\\"sn\\\":\\\"11111\\\",\\\"station_code\\\":\\\"text\\\",\\\"station_name\\\":\\\"text\\\",\\\"is_pass\\\":\\\"1\\\",\\\"is_fail\\\":\\\"1\\\",\\\"scan_by\\\":\\\"cj\\\",\\\"scan_dt\\\":\\\"1594026130000\\\",\\\"update_dt\\\":\\\"1594026130000\\\",\\\"update_by\\\":\\\"sfc\\\",\\\"data_from\\\":\\\"cj\\\"}\"}],\"rowKey\":\"03:1575525600500:DAX1:D624:2a6ae81e-4738-4801-87c6-fcfeb89bdbb0\"}"
//        }

//    运行参数：测试环境
//    {\"dprunclass\":\"com.foxconn.dpm.sprint5.ods_dwd.ProductionSteaming\",\"dpappName\":\"streaming-dwd\",\"dpuserid\":\"8F70E561-ED63-4B88-A343-UIDE89FF9156\",\"dpmaster\":\"local[*]\",\"pmClass\":\"com.tm.dl.javasdk.dpspark.common.ProdPermissionManager\",\"dpType\":\"streaming\",\"dpStreaming\":{\"topics\":\"chen_topic\",\"groupId\":\"chen\",\"batchDuration\":\"4\",\"windowDurationMultiple\":\"4\",\"sliverDurationMultiple\":\"4\"}}

 // host 环境QA     sparkSDK   http://10.60.136.145:806/jar/sparksdk/



@Slf4j
public class ProductionSteamingdwdTodws extends DPSparkBase {
    private static ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(20);
    private static SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
    private static final int INTERGALACTIC = 3;

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {
        //总体逻辑，每次从流中获取ods中的数据，将缓存数据存到hbase并发送到kafka中，缓存清空
        final Broadcast kafkaProducer = DPSparkApp.getContext().broadcast(KafkaSink.apply(new Properties() {{
            putAll(dpStreaming.getKafkaParams());
        }}));
//        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
//        Logger.getLogger("org.project-spark").setLevel(Level.WARN);
        //声明累加器缓存
       CollectionAccumulator<DWSProductionOutputStreaming> kafkaWriteListdws = DPSparkApp.getContext().sc().collectionAccumulator();
        org.apache.spark.util.LongAccumulator countCounter = DPSparkApp.getContext().sc().longAccumulator("count");
        //接收流式数据
        dpStreaming.startJob(rdd -> {
            //接收ods的数据
            //"{\"dpHbasecolumns\":[{\"columnName\":\"datajson\",\"family\":\"DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE_BAK\",\"value\":\"{\"site_code\":\"WH\",\"level_code\":\"L5\",\"factory_code\":\"DT1\",\"process_code\":\"Assy\",\"area_code\":\"11\",\"line_code\":\"LINE_A\",\"work_shift\":\"D\",\"work_dt\":\"2020-07-06\",\"wo\":\"111\",\"workorder_type\":\"1111\",\"part_no\":\"51452sd\",\"customer\":\"HP\",\"model_no\":\"11111\",\"sn\":\"11111\",\"station_code\":\"text\",\"station_name\":\"text\",\"is_pass\":\"1\",\"is_fail\":\"1\",\"scan_by\":\"cj\",\"scan_dt\":\"1594026130000\",\"update_dt\":\"1594026130000\",\"update_by\":\"sfc\",\"data_from:\"cj\"}}],\"rowKey\":\"03:1575525600500:DAX1:D624:2a6ae81e-4738-4801-87c6-fcfeb89bdbb0\"}"

//            rdd.foreachPartition(i -> i.forEachRemaining(record -> {
//                ConsumerRecord<String, Object> record1 = record;
//                JSONObject odsData = JSON.parseObject(record1.value().toString());
//                System.out.println(odsData);
//                DWDProductionOutputStreaming dwdProductionOutputStreaming = JSONObject.toJavaObject((JSON) JSON.parse(odsData.toString()), DWDProductionOutputStreaming.class);
//                System.out.println(dwdProductionOutputStreaming.toString());
//                kafkaWriteList.add(dwdProductionOutputStreaming);
//            }));
            JavaRDD<DWDProductionOutputStreaming> javaRDD = rdd.map(record -> {
                JSONObject odsData = JSON.parseObject(record.value().toString());
                DWDProductionOutputStreaming dwdProductionOutputStreaming = JSONObject.toJavaObject((JSON) JSON.parse(odsData.toString()), DWDProductionOutputStreaming.class);
                return dwdProductionOutputStreaming;
            });
            Dataset<Row> dataFrame = DPSparkApp.getSession().createDataFrame(javaRDD, DWDProductionOutputStreaming.class);
            dataFrame.createOrReplaceTempView("outputStreamView");
            //"site_code","level_code","factory_code","process_code","area_code","line_code","part_no","sku","platform","work_dt"
            Dataset<Row> sql = DPSparkApp.getSession().sql("select  site_code,level_code,factory_code,process_code,area_code,line_code,part_no,sku,platform,work_dt,count(*) out_qty  from outputStreamView group by site_code,level_code,factory_code,process_code,area_code,line_code,part_no,sku,platform,work_dt ");
            sql.toJavaRDD().foreach(r -> {
                DWSProductionOutputStreaming dwsOutput = new DWSProductionOutputStreaming();
                dwsOutput.setSite_code(r.getString(0)).setLevel_code(r.getString(1)).setFactory_code(r.getString(2))
                        .setProcess_code(r.getString(3)).setArea_code(r.getString(4)).setLine_code(r.getString(5))
                        .setPart_no(r.getString(6)).setPlatform(r.getString(8)).setWork_dt(r.getString(9)).setOutput_qty(String.valueOf((r.getLong(10))));
                kafkaWriteListdws.add(dwsOutput);
            });
            countCounter.add(1);
            System.out.println("------------------"+countCounter+"------------------");
            //每十个批次写入一次数据
            if((countCounter.value() % INTERGALACTIC) == 0){
                DPSparkApp.getSession().createDataFrame(kafkaWriteListdws.value(),DWSProductionOutputStreaming.class).createOrReplaceTempView("outputViewHH");
                Dataset<Row> dataset = DPSparkApp.getSession().sql("select site_code,level_code,factory_code,process_code,area_code,line_code,part_no,platform,work_dt,sum(output_qty) output_qty from outputViewHH group by site_code,level_code,factory_code,process_code,area_code,line_code,part_no,platform,work_dt ");
                dataset.show();
                dataset.toJSON().foreach(r -> {
                    ((KafkaSink) kafkaProducer.value()).sendAll("text_topic", r);
                });
                try {
                    DPHbase.rddWrite("dpm_dws_production_output_hh", dataset, new DlFunction<GenericRowWithSchema, String>() {
                        @SneakyThrows
                        @Override
                        public String apply(GenericRowWithSchema genericRowWithSchema) {
                            //addsalt+work_dt+site_code+level_code+area_code+line_code+start_time+station_code
                            String rowkey = String.valueOf(formatWorkDt.parse(genericRowWithSchema.getAs("work_dt").toString()).getTime()) + ":" + genericRowWithSchema.getAs("site_code").toString() + ":" + genericRowWithSchema.getAs("level_code").toString() + ":" + genericRowWithSchema.getAs("area_code").toString()
                                    + ":" + genericRowWithSchema.getAs("line_code").toString()+ ":" + genericRowWithSchema.getAs("line_code").toString()+ ":" + genericRowWithSchema.getAs("line_code").toString();
                            return consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    //清空累加器
                    kafkaWriteListdws.reset();
                }
            }
        });
    }


    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

    }
}
