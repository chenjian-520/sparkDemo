package com.foxconn.dpm.sprint5.ods_dwd.service;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.foxconn.dpm.common.bean.KafkaMessage;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.core.base.BaseStreamingService;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionOutput;
import com.foxconn.dpm.sprint5.ods_dwd.bean.ODSProductionOutputDayLfive;
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
 * @Description 从kafka取出原始，存入ods和dwd    【隐形规则是：原始数据就是按ods的表结构传入的】
 */
@Slf4j
public class ProductionOutPutKafkaToOdsService extends BaseStreamingService {

    private final static String ADS_TABLE_NAME_FIVE = "dpm_ods_production_output_day_lfive";
    private final static String ADS_TABLE_NAME_SIX = "dpm_ods_production_output_day_lsix";
    private final static String ADS_TABLE_NAME_TEN = "dpm_ods_production_output_day_lten";

    private final static String DWD_TABLE_NAME = "dpm_dwd_production_output";

    @Override
    protected void doStreaming(JavaRDD<ConsumerRecord<String, Object>> javaRdd,  Broadcast<KafkaSink<Object, Object>> kafkaProducer) throws Exception{
        log.info("===========ProductionOutPut--OdsToDwd--Start===============");
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(Level.WARN);

        //声明累加器缓存
        CollectionAccumulator<ODSProductionOutputDayLfive> odsCollection = DPSparkApp.getContext().sc().collectionAccumulator();
        CollectionAccumulator<DWDProductionOutput> dwdCollection = DPSparkApp.getContext().sc().collectionAccumulator();


        // 提取kafka的ods数据
        javaRdd.foreachPartition(rdd ->
                rdd.forEachRemaining(record -> {
                    try {
                        JSONObject odsData = JSON.parseObject(record.value().toString());
                        KafkaMessage msg = JSON.parseObject(odsData.toJSONString(), KafkaMessage.class);

                        Map<String, Object> data = msg.getData();
                        ODSProductionOutputDayLfive odsProductionOutputDayLfive = BeanUtil.mapToBean(data, ODSProductionOutputDayLfive.class, true);
                        odsCollection.add(odsProductionOutputDayLfive);


                    }catch (Exception e) {
                        log.error("程序出现异常 {}, {}", record.value(), e);
                        DPSparkApp.doThrow(e);
                    }
                })
        );


        try {

            // ods 转 dwd
            odsCollection.value().forEach(data -> {
                DWDProductionOutput dwdProductionOutput = new DWDProductionOutput();
                BeanUtil.copyProperties(data, dwdProductionOutput);
                dwdCollection.add(dwdProductionOutput);
            });

            // TODO 是否持久化ods数据有待商量



            // 将dwd数据存入hbase
            List<DWDProductionOutput> dwdList = dwdCollection.value();
            JavaRDD<DWDProductionOutput> parallelizeRdd = DPSparkApp.getContext().parallelize(dwdList);
            JavaRDD<Put> putJavaRDD = BeanConvertTools.covertBeanToPut(parallelizeRdd, DWDProductionOutput.class);
            DPHbase.rddWrite(DWD_TABLE_NAME, putJavaRDD);

            // 发送kafka -- 尽量循环。。单条数据不能太大
            List<Map<String, Object>> collect = dwdList.stream().map(BeanUtil::beanToMap).collect(Collectors.toList());

            KafkaSink<Object, Object> kafkaSink = kafkaProducer.value();
            collect.forEach(data -> {
                KafkaMessage dwdMsg = new KafkaMessage()
                        .setData(data)
                        .setTableName(DWD_TABLE_NAME)
                        .setFamily(DWD_TABLE_NAME.toUpperCase());
                kafkaSink.sendAll(this.getSendTopic(), JSON.toJSON(dwdMsg));
            });

        } catch (Exception e) {
            log.error("dwd，tleol数据处理异常", e);
            DPSparkApp.doThrow(e);
        } finally {
            odsCollection.reset();
            dwdCollection.reset();
        }

        log.info( "===========ProductionOutPut--OdsToDwd--End===============");
    }





}
