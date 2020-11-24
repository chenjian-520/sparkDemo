package com.foxconn.dpm.sprint5.ods_dwd.service;

import com.foxconn.dpm.core.base.BaseStreamingService;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionOutput;
import com.foxconn.dpm.sprint5.ods_dwd.bean.ODSProductionOutputDayLfive;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.streaming.KafkaSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;


/**
 * @Author HY
 * @Date 2020/7/15 16:44
 * @Description 从kafka取出dwd数据清洗到dws
 */
@Slf4j
public class ProductionOutPutDwdToDwsService extends BaseStreamingService {

    @Override
    protected void doStreaming(JavaRDD<ConsumerRecord<String, Object>> javaRdd, Broadcast<KafkaSink<Object, Object>> kafkaProducer) throws Exception {

        log.info("===========ProductionOutPut--DwsToAds--Start===============");
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(Level.WARN);

        //声明累加器缓存
        CollectionAccumulator<ODSProductionOutputDayLfive> odsCollection = DPSparkApp.getContext().sc().collectionAccumulator();
        CollectionAccumulator<DWDProductionOutput> dwdCollection = DPSparkApp.getContext().sc().collectionAccumulator();





        log.info( "===========ProductionOutPut--DwsToAds--End===============");
    }
}
