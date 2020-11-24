package com.foxconn.dpm.sprint5.ods_dwd;

import com.foxconn.dpm.common.bean.DateBean;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.tools.DateBeanTools;
import com.foxconn.dpm.sprint5.ods_dwd.bean.DWDProductionMachineMaintenance;
import com.foxconn.dpm.sprint5.ods_dwd.bean.ODSProductionSfcTpmLineMapping;
import com.foxconn.dpm.sprint5.ods_dwd.bean.ODSProductionTpmMachineMaintenance;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Map;

/**
 * @Author HY
 * @Date 2020/6/28 9:32
 * @Description
 */
public class ProductionMachineMaintenance extends DPSparkBase {

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        // 获取传入的时间
        DateBean date = DateBeanTools.buildDateBean(map);
        System.out.println("入参：" + map);

        // 读取hbase表数据
        JavaRDD<Result> machineRddResult = DPHbase.rddRead("dpm_ods_production_tpm_machine_maintenance", new Scan(),true);
        System.out.println("dpm_ods_production_tpm_machine_maintenance: count=" + machineRddResult.cache().cache());

        JavaRDD<Result> sfcRddResult = DPHbase.rddRead("dpm_ods_production_sfc_tpm_line_mapping", new Scan(),true);
        System.out.println("dpm_ods_production_sfc_tpm_line_mapping: count=" + sfcRddResult.cache().cache());

        // Result转换为javaBean
        JavaRDD<ODSProductionTpmMachineMaintenance> machineRdd = BeanConvertTools.convertResultToBean(machineRddResult, ODSProductionTpmMachineMaintenance.class);

        JavaRDD<ODSProductionSfcTpmLineMapping> sfcRdd = BeanConvertTools.convertResultToBean(sfcRddResult, ODSProductionSfcTpmLineMapping.class);


        // 根据tpm_line_code分组 -- left join操作
        JavaPairRDD<String, ODSProductionTpmMachineMaintenance> machinePairRdd = machineRdd.keyBy(ODSProductionTpmMachineMaintenance::groupBy);
        JavaPairRDD<String, ODSProductionSfcTpmLineMapping> sfcPairRdd = sfcRdd.keyBy(ODSProductionSfcTpmLineMapping::groupBy);


        // left join 操作
        JavaRDD<DWDProductionMachineMaintenance> dtwdProductionMachineMaintenanceJavaRDD = machinePairRdd.leftOuterJoin(sfcPairRdd).map(newPair -> {

            Tuple2<ODSProductionTpmMachineMaintenance, Optional<ODSProductionSfcTpmLineMapping>> tuple = newPair._2;
            ODSProductionTpmMachineMaintenance odsProductionTpmMachineMaintenance = tuple._1;
            ODSProductionSfcTpmLineMapping odsProductionSfcTpmLineMappingOptional = tuple._2.isPresent() ? tuple._2.get() : null;

            return buildDWDProductionMachineMaintenance(odsProductionTpmMachineMaintenance, odsProductionSfcTpmLineMappingOptional);
        });

        System.out.println("dpm_ods_production_tpm_machine_maintenance====LEFT JION=====dpm_ods_production_sfc_tpm_line_mapping: count=" + dtwdProductionMachineMaintenanceJavaRDD.cache().cache());


        // obj 转 put
        JavaRDD<Put> putJavaRdd = BeanConvertTools.covertBeanToPut(dtwdProductionMachineMaintenanceJavaRDD, DWDProductionMachineMaintenance.class);

        // 写入 HBase
        DPHbase.rddWrite("dpm_dwd_production_machine_maintenance", putJavaRdd);
        System.out.println("===============>>>END<<<======================");
    }



    /**
     * <H2>描述: 提取ODSProductionSfcTpmLineMapping的line_code和area_code </H2>
     * @date 2020/7/2
     */
    private DWDProductionMachineMaintenance buildDWDProductionMachineMaintenance(ODSProductionTpmMachineMaintenance machine, ODSProductionSfcTpmLineMapping sfc) {
        DWDProductionMachineMaintenance target = new DWDProductionMachineMaintenance();

        BeanConvertTools.copyByName(machine, target);
        target.setLineCode(SystemConst.NA);
        target.setAreaCode(SystemConst.NA);
        if(sfc != null) {
            target.setLineCode(sfc.getLineCode());
            target.setAreaCode(sfc.getAreaCode());
        }
        return target;
    }



    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
