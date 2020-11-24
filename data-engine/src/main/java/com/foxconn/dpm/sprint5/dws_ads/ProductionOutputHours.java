package com.foxconn.dpm.sprint5.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.common.bean.DateBean;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.tools.DateBeanTools;
import com.foxconn.dpm.sprint5.dws_ads.bean.ADSProductionOutputDay;
import com.foxconn.dpm.sprint5.dws_ads.bean.DWSProductionOutputHour;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;

/**
 * @Author HY
 * @Date 2020/6/28 9:32
 * @Description 将小时产量表 dpm_dws_production_output_hh 转入 ADS表 dpm_ads_production_output_day
 */
public class ProductionOutputHours extends DPSparkBase {

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        // 获取传入的时间
        DateBean date = DateBeanTools.buildDateBean(map);
        System.out.println("入参：" + map);


        // 读取hbase表数据
        JavaRDD<Result> rddResult = DPHbase.saltRddRead("dpm_dws_production_output_hh", date.getStartTime(), date.getEndTime(), new Scan(),false);
        System.out.println("获取dpm_dws_production_output_hh的数据count为：" +rddResult.cache().count());


        // Result转换为javaBean
        JavaRDD<DWSProductionOutputHour> dwsProductionOutputJavaRDD = BeanConvertTools.convertResultToBean(rddResult, DWSProductionOutputHour.class);

        dwsProductionOutputJavaRDD.take(1).forEach(data -> System.out.println(data));
        // 转换成ADS对象
        JavaRDD<ADSProductionOutputDay> list = dwsProductionOutputJavaRDD.map(DWSProductionOutputHour::buildAdsProductionOutputDay);
        System.out.println("存入dpm_ads_production_output_day（mysql）的数据count为：" +list.cache().count());
        list.take(1).forEach(data -> System.out.println(data));


        // TODO ： 这里是为了上线设计的临时逻辑，删除上一次的数据， 然后在插入新数据
        DpMysql.deleteByPrimaryKey(list, ADSProductionOutputDay.class);
        // 插入mysql
        DpMysql.insertData(list, ADSProductionOutputDay.class);
        System.out.println("================>>>END<<<================");
    }



    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
