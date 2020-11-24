package com.foxconn.dpm.core.base;


import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import com.tm.dl.javasdk.dpspark.streaming.KafkaSink;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * 业务类 基类 .
 *
 * @className: BaseService
 * @author: ws
 * @date: 2020/7/2 11:32
 * @version: 1.0.0
 */
@Slf4j
@Data
public abstract class BaseStreamingService implements Serializable {

    private String sendTopic;


    /**
     * <H2>描述: 模板方法</H2>
     * @date 2020/7/16
     */
    public void execute (Map<String, Object> parameterMap, DPStreaming dpStreaming) throws Exception{

        this.setSendTopic(parameterMap.get("sendTopic") + "");

        Properties properties = new Properties();
        properties.putAll(dpStreaming.getKafkaParams());
        Broadcast<KafkaSink<Object, Object>> broadcast = DPSparkApp.getContext().broadcast(KafkaSink.apply(properties));

        dpStreaming.startJob(rdd -> {
            try {
                doStreaming(rdd, broadcast);
            } catch (Exception e) {
                log.error("程序出现异常 {}, {}", parameterMap, e);
                DPSparkApp.doThrow(e);
            }
        });

    }

    /**
     * 执行sparkStream .
     *  @date 2020/7/16
     **/
    protected abstract void doStreaming(JavaRDD<ConsumerRecord<String, Object>> javaRdd, Broadcast<KafkaSink<Object, Object>> kafkaProducer) throws Exception;



}
