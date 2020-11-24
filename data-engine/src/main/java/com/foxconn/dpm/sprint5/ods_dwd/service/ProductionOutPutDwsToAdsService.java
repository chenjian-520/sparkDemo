package com.foxconn.dpm.sprint5.ods_dwd.service;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.foxconn.dpm.common.bean.KafkaMessage;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.core.base.BaseStreamingService;
import com.foxconn.dpm.sprint5.dwd_dws.ProductionOutputDayToHours;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionOutput;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWSProductionOutputHour;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.KafkaSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @Author HY
 * @Date 2020/7/15 16:44
 * @Description 从kafka取出dws数据清洗到ads
 */
@Slf4j
public class ProductionOutPutDwsToAdsService extends BaseStreamingService {

    private final static String DWD_TABLE_NAME = "dpm_dwd_production_output";
    private final static String DWD_CT_TABLE_NAME = "dpm_dwd_production_standary_ct";

    private final static String DWS_TABLE_NAME = "dpm_dws_production_output_hh";



    @Override
    protected void doStreaming(JavaRDD<ConsumerRecord<String, Object>> javaRdd, Broadcast<KafkaSink<Object, Object>> kafkaProducer) throws Exception {

        log.info("===========ProductionOutPut--DwdToDws--Start===============");
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(Level.WARN);

        //声明累加器缓存
        CollectionAccumulator<DWDProductionOutput> dwdCollection = DPSparkApp.getContext().sc().collectionAccumulator();
        CollectionAccumulator<DWSProductionOutputHour> dwsCollection = DPSparkApp.getContext().sc().collectionAccumulator();

        // 提取kafka的dwd数据
        javaRdd.foreachPartition(rdd ->
            rdd.forEachRemaining(record -> {
                try {
                    JSONObject odsData = JSON.parseObject(record.value().toString());
                    KafkaMessage msg = JSON.parseObject(odsData.toJSONString(), KafkaMessage.class);

                    Map<String, Object> data = msg.getData();
                    DWDProductionOutput dwdProductionOutput = BeanUtil.mapToBean(data, DWDProductionOutput.class, true);
                    dwdCollection.add(dwdProductionOutput);

                }catch (Exception e) {
                    log.error("程序出现异常 {}, {}", record.value(), e);
                    DPSparkApp.doThrow(e);
                }
            })
        );

        // 计算dws数据
        JavaRDD<DWDProductionOutput> parallelize = DPSparkApp.getContext().parallelize(dwdCollection.value());
        JavaRDD<DWSProductionOutputHour> dtoJavaRDD = new ProductionOutputDayToHours().convertDwdToDwsData(parallelize);


        // obj 转 put
        JavaRDD<Put> putJavaRdd = BeanConvertTools.covertBeanToPut(dtoJavaRDD, DWSProductionOutputHour.class);
        // 写入 hbase
        DPHbase.rddWrite(DWS_TABLE_NAME, putJavaRdd);


        // 发送kafka -- 尽量循环。。单条数据不能太大
        dtoJavaRDD.foreach(dwsCollection::add);
        List<Map<String, Object>> collect = dwsCollection.value().stream().map(BeanUtil::beanToMap).collect(Collectors.toList());

        KafkaSink<Object, Object> kafkaSink = kafkaProducer.value();
        collect.forEach(data -> {
            KafkaMessage dwdMsg = new KafkaMessage()
                    .setData(data)
                    .setTableName(DWS_TABLE_NAME)
                    .setFamily(DWS_TABLE_NAME.toUpperCase());
            kafkaSink.sendAll(this.getSendTopic(), JSON.toJSON(dwdMsg));
        });


        log.info( "===========ProductionOutPut--DwdToDws--End===============");
    }

}
