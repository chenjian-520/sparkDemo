package com.foxconn.dpm.sprint5.dwd_dws;

import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.tools.HBaseTools;
import com.foxconn.dpm.common.tools.JsonTools;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionMachineMaintenance;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWSProductionMachineMaintenanceHH;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_machine_maintenance.DateRuleBean;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_machine_maintenance.GroupRuleBean;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author HY
 * @Date 2020/6/28 9:32
 * @Description  dpm_dws_production_line_maintenance_hh数据抽取
 *
 */
public class ProductionMachineMaintenance extends DPSparkBase {

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        String dwdTableName = "dpm_dwd_production_machine_maintenance";
        String dwsTableName = "dpm_dws_production_line_maintenance_hh";

        // 读取HBase获取数据
        JavaRDD<DWDProductionMachineMaintenance> dwdProductionMachineMaintenanceJavaRDD = HBaseTools.getRdd(dwdTableName, DWDProductionMachineMaintenance.class);
        System.out.println(dwdTableName + ":count = "+dwdProductionMachineMaintenanceJavaRDD.cache().count());

        // 读取配置文件规则
        List<GroupRuleBean> ruleList = JsonTools.readJsonArray("config/sprint5/dwd_dws/production_machine_maintenance/ProductionMachineMaintenance.json", GroupRuleBean.class);
        System.out.println("时间段配置文件：" + ruleList.size());

        // 按线对数据分组
        JavaPairRDD<String, Iterable<DWDProductionMachineMaintenance>> iterableJavaPairRDD = dwdProductionMachineMaintenanceJavaRDD.groupBy(DWDProductionMachineMaintenance::getGroupBy);

        // 按时间规则计算数据
        JavaRDD<List<DWSProductionMachineMaintenanceHH>> map1 = iterableJavaPairRDD.map(pairRdd -> {
            String key = pairRdd._1;

            List<DWSProductionMachineMaintenanceHH> list = new ArrayList<>();
            for(GroupRuleBean rule : ruleList) {
                if(checkDate(rule, key)){
                    list.addAll(buildDWSProductionMachineMaintenanceHH(rule, pairRdd._2));
                }
            }

            return list;
        });

        // list - obj
        JavaRDD<DWSProductionMachineMaintenanceHH> objectJavaRDD = map1.flatMap(List::iterator);
        System.out.println("存入" + dwsTableName +"的数据count为：" +objectJavaRDD.cache().count());


        // obj 转 put
        JavaRDD<Put> putJavaRdd = BeanConvertTools.covertBeanToPut(objectJavaRDD, DWSProductionMachineMaintenanceHH.class);

        // 写入 hbase
        DPHbase.rddWrite(dwsTableName, putJavaRdd);
        System.out.println("===============>>>END<<<======================");
    }

    /**
     * <H2>描述: 判断当前是否和规则匹配 </H2>
     * @date 2020/7/2
     */
    private boolean checkDate(GroupRuleBean rule, String key) {
        return key.contains(rule.getSiteCode()) && key.contains(rule.getLevelCode());
    }


    /**
     * <H2>描述: 核心逻辑：
     *  1、根据24个区间段去创建报警数据
     *  2、判断报警开始、结束时间是否处于某个时间区间段，如果处于该时间段，就构建一条该时间区间段的报警数据
     *  3、如果报警开始时间、结束时间跨越某个时间区间段，则查分成多个时间段的多条数据
     *  4、如果某个时间段不存在报警数据，  构建一条NA数据
     * </H2>
     * @date 2020/7/3
     */

    private List<DWSProductionMachineMaintenanceHH> buildDWSProductionMachineMaintenanceHH(GroupRuleBean rule, Iterable<DWDProductionMachineMaintenance> iterable) {

        List<DWSProductionMachineMaintenanceHH> listDws = new ArrayList<>();
        List<DateRuleBean> times = rule.getTimes();  // 时间段数组[24个]
        times.sort(Comparator.comparing(DateRuleBean::getStartTimeSeq)); // 按时间升序排序
        for(int i=0; i<times.size(); i++) {

            List<DWSProductionMachineMaintenanceHH> tempList = new ArrayList<>();
            Iterator<DWDProductionMachineMaintenance> iterator = iterable.iterator();
            DWDProductionMachineMaintenance next = null;

            while(iterator.hasNext()) {
                next = iterator.next();
                tempList.addAll(next.buildDWSProductionMachineMaintenanceHH(times, i));
            }

            // 如果一条都不存在， 插入一条NA数据
            if(tempList.isEmpty() && next != null) {
                tempList.add(next.buildDefaultDWSProductionMachineMaintenanceHH(times.get(i)));
            }

            listDws.addAll(tempList);
        }

        return clearNAData(listDws);

    }

    /**
     * <H2>描述: 清理NA数据  如果同一个时间段既有NA又有正常报警信息，清楚NA数据 </H2>
     * @date 2020/7/8
     */
    private List<DWSProductionMachineMaintenanceHH> clearNAData(List<DWSProductionMachineMaintenanceHH> listDws) {
        Map<String, List<DWSProductionMachineMaintenanceHH>> map = listDws.stream().collect(Collectors.groupingBy(item -> item.getWorkDt() + item.getStartTime() + item.getEndTime()));

        Set<Map.Entry<String, List<DWSProductionMachineMaintenanceHH>>> entries = map.entrySet();
        return  entries.stream().map(set -> {

            // 如果只有一条表示NA 直接返回
            List<DWSProductionMachineMaintenanceHH> value = set.getValue();
            if (value.size() == 1) {
                return value;
            }


            // 如果有多条 ，过滤掉NA
            return value.stream().filter(data ->
                    !(data.getBreakdownTime().equals(SystemConst.NA))
            ).collect(Collectors.toList());


        }).flatMap(Collection::stream).collect(Collectors.toList());

    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
