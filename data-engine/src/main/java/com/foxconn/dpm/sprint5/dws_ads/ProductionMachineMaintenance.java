package com.foxconn.dpm.sprint5.dws_ads;

import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.common.tools.HBaseTools;
import com.foxconn.dpm.sprint5.dws_ads.bean.ADSProductionMachineFailureTimeDay;
import com.foxconn.dpm.sprint5.dws_ads.bean.DWSProductionMachineMaintenanceHH;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.spark.api.java.JavaRDD;
import java.util.Map;

/**
 * @Author HY
 * @Date 2020/6/28 9:32
 * @Description
 */
public class ProductionMachineMaintenance extends DPSparkBase {

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        String dwsTableName = "dpm_dws_production_line_maintenance_hh";

        JavaRDD<DWSProductionMachineMaintenanceHH> rdd1 = HBaseTools.getRdd(dwsTableName, DWSProductionMachineMaintenanceHH.class);
        System.out.println("存入" + dwsTableName +"的数据count为：" +rdd1.cache().count());


        // 转换成ADS对象
        JavaRDD<ADSProductionMachineFailureTimeDay> list = rdd1.map(DWSProductionMachineMaintenanceHH::buildADSProductionMachineFailureTimeDay);
        System.out.println("存入dpm_ads_production_Machine_failure_time_day的数据count为：" +list.cache().count());

        // 插入mysql
        DpMysql.insertOrUpdateData(list, ADSProductionMachineFailureTimeDay.class);
        System.out.println("================>>>END<<<================");

    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
