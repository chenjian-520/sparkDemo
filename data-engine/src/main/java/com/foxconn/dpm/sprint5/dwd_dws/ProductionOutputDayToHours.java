package com.foxconn.dpm.sprint5.dwd_dws;

import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.common.tools.*;
import com.foxconn.dpm.enums.TableNameEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionOutput;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionStandaryCt;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWSProductionOutputHour;
import com.foxconn.dpm.common.bean.DateBean;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DimProductionActLineMapping;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.DateRuleBean;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.FilterRuleBean;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.GroupRuleBean;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.ws.rs.POST;
import java.util.*;

/**
 * @Author HY
 * @Date 2020/6/28 9:32
 * @Description  将DWD天产量按小时进行统计存入DWS
 */
public class ProductionOutputDayToHours extends DPSparkBase {

    private boolean debug;

    private void init(Map<String, Object> map) {
        debug = "debug".equalsIgnoreCase(""+map.get("debug"));
    }


    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        init(map);
        String dwdTable = "dpm_dwd_production_output";
        String dwsTable = "dpm_dws_production_output_hh";
        long start = System.currentTimeMillis();
        // 获取传入的时间
        DateBean date = DateBeanTools.buildDateBean(map);
        System.out.println("startTime:" +date.getStartTime());
        System.out.println("endTime:" + date.getEndTime());
        // 读取hbase表数据
        JavaRDD<Result> rddResult = DPHbase.saltRddRead(dwdTable, date.getStartTime(), date.getEndTime(), new Scan(),false);
        if(debug) {
            System.out.println(dwdTable + ":count = "+rddResult.cache().count());
        }

        // Result转换为javaBean
        JavaRDD<DWDProductionOutput> dwdProductionOutputJavaRDD = BeanConvertTools.convertResultToBean(rddResult, DWDProductionOutput.class);

        // 构造dws数据
        JavaRDD<DWSProductionOutputHour> dtoJavaRDD = convertDwdToDwsData(dwdProductionOutputJavaRDD);

        // obj 转 put
        JavaRDD<Put> putJavaRdd = BeanConvertTools.covertBeanToPut(dtoJavaRDD, DWSProductionOutputHour.class);
        if(debug) {
            System.out.println("存入" + dwsTable + "的数据count为：" + putJavaRdd.cache().count());
        }


        // 写入 hbase
        DPHbase.rddWrite(dwsTable, putJavaRdd);
        System.out.println("===============>>>END<<<======================");

        System.out.println((System.currentTimeMillis() - start)/1000/60);

    }



    public JavaRDD<DWSProductionOutputHour> convertDwdToDwsData(JavaRDD<DWDProductionOutput> dwdProductionOutputJavaRDD) throws Exception {

//        WH  L5   ALL      PACKING --> PACKING
//        WH  L6   ALL      PACKING -->  PACKING
//        WH  L10  ALL      ASSEMBLY1 --> assy
//        WH  L10  Lenovo   Testing --> test
//        WH  L10  HP       PREPEST --> test
//        WH  L10  HP       POST RUNIN --> test
//        WH  L10  ALL      SCAN COUNTRY KIT  -->packing
//         L10规则： 如果stationCode = packing/assy -> 不分客户      如果是test， 只获取lenovo和HP，同时HP合并工站PREPEST、POST RUNIN
        List<FilterRuleBean> filterList = JsonTools.readJsonArray("config/sprint5/dwd_dws/production_output/ProductionOutputDayToHours_filter.json", FilterRuleBean.class);
        System.out.println("过滤规则：" + filterList.size());
        DWDProductionOutput output = new DWDProductionOutput(filterList);

        // 重置dwd的stationCode / groupBy 为过滤做准备
        JavaRDD<DWDProductionOutput> map = dwdProductionOutputJavaRDD.map(output.map);
        // 过滤数据
        JavaRDD<DWDProductionOutput> filter = map.filter(output.filter);

        if(debug) {
            System.out.println("按rule过滤后数据为" + filter.count());
        }


        //  根据分组规则分组
        JavaPairRDD<String, Iterable<DWDProductionOutput>> pairRDD = filter.groupBy(DWDProductionOutput::getGroupBy);
        // 读取按时间分组的规则
        List<GroupRuleBean> ruleList = JsonTools.readJsonArray("config/sprint5/dwd_dws/production_output/ProductionOutputDayToHours.json", GroupRuleBean.class);
        System.out.println("时间段配置文件：" + ruleList.size());

        // 将dwd对象转dws对象
        JavaRDD<List<DWSProductionOutputHour>> dwsProductionOutputHourJavaRDD = pairRDD.map(rdd -> {
            List<DWSProductionOutputHour> listDws = new ArrayList<>();
            for(GroupRuleBean rule : ruleList) {
                if(checkData(rule, rdd._1)){
                    listDws.addAll(createDWSProductionOutputHour(rule, rdd._2));
                }
            }
            return listDws;
        });

        // list 转 obj
        JavaRDD<DWSProductionOutputHour> dwsProductionOutputHourJavaRDD1 = dwsProductionOutputHourJavaRDD.flatMap(List::iterator);

        // 查询dpm_dwd_production_standary_ct数据，用于计算日产量
        String ctTableName = "dpm_dwd_production_standary_ct";
        JavaRDD<DWDProductionStandaryCt> ctRdd = HBaseTools.getRdd(ctTableName, DWDProductionStandaryCt.class);
        if(debug) {
            System.out.println(ctTableName + ":count = " + ctRdd.cache().count());
        }


        JavaRDD<DimProductionActLineMapping> actLineMappingJavaRDD = HBaseTools
                .getRdd(TableNameEnum.DIM.DIM_PRODUCTION_ACT_LINE_MAPPING.getTableName()
                        , DimProductionActLineMapping.class);
        if(debug) {
            System.out.println("对象 actLineMappingJavaRDD 数据条数为：{}" + actLineMappingJavaRDD.count());
        }

        // key 为 siteCode levelCode factoryCode processCode lineCode area
        JavaPairRDD<String, DWDProductionStandaryCt> ctPair = ctRdd.mapToPair(DWDProductionStandaryCt.mapToPair);
        JavaPairRDD<String, DimProductionActLineMapping> mappingPair = actLineMappingJavaRDD.mapToPair(DimProductionActLineMapping.mapToPair);

        JavaRDD<DWDProductionStandaryCt> map1 = ctPair.leftOuterJoin(mappingPair).map(data -> {
            Tuple2<DWDProductionStandaryCt, Optional<DimProductionActLineMapping>> dwdProductionStandaryCtOptionalTuple2 = data._2;
            DWDProductionStandaryCt ct = dwdProductionStandaryCtOptionalTuple2._1;
            if (dwdProductionStandaryCtOptionalTuple2._2.isPresent()) {
                DimProductionActLineMapping mapping = dwdProductionStandaryCtOptionalTuple2._2.get();
                ct.setLineCode(mapping.getLineCode());
            }
            return ct;
        });


        // key 为 siteCode levelCode factoryCode processCode lineCode part_no
        JavaPairRDD<String, DWSProductionOutputHour> productionOutputJavaPairRDD =
                dwsProductionOutputHourJavaRDD1.mapToPair(
                    (PairFunction<DWSProductionOutputHour, String, DWSProductionOutputHour>)
                        item -> new Tuple2<String, DWSProductionOutputHour>(
                                item.leftJoinCondition(), item));

        JavaPairRDD<String, DWDProductionStandaryCt> ctJavaPairRDD = map1.mapToPair(
                    (PairFunction<DWDProductionStandaryCt, String, DWDProductionStandaryCt>)
                        item -> new Tuple2<String, DWDProductionStandaryCt>(
                                item.leftJoinCondition(), item));



        // join ct 计算目标产量   3600 / ct
        JavaRDD<DWSProductionOutputHour> map2 = productionOutputJavaPairRDD.leftOuterJoin(ctJavaPairRDD).map(item -> {
            Tuple2<DWSProductionOutputHour, Optional<DWDProductionStandaryCt>> data = item._2;
            DWSProductionOutputHour hour = data._1;
            if (!data._2.isPresent()) {
                hour.setOutputQtyTarget(SystemConst.ZERO);
                return hour;
            }
            DWDProductionStandaryCt standaryCt = data._2.get();
            String cycleTime = standaryCt.getCycleTime();
            int qtyTarget = (int) Math.round(3600 / Double.parseDouble(cycleTime));
            hour.setOutputQtyTarget(String.valueOf(qtyTarget));
            return hour;
        });

        if(debug) {
            JavaRDD<DWSProductionOutputHour> filter1 = map2.filter(data -> {
                return data.getOutputQtyTarget().equals("0")
                        && data.getLevelCode().equals("L6")
                        && data.getStartTimeSeq().equals("0730")
                        && data.getSiteCode().equals("WH");
            });

            System.out.println("未匹配到ct表的数据为：");
            filter1.collect().forEach(System.out::println);
        }
        return map2;
    }


    /**
     * <H2>描述: 验证配置和数据是否匹配 </H2>
     * @date 2020/6/30
     */
    private boolean checkData(GroupRuleBean rule, String key) {
        return key.contains(rule.getSiteCode()) && key.contains(rule.getLevelCode());
    }


    /**
     * <H2>描述: 核心逻辑：根据时间段计算产量 </H2>
     * @date 2020/6/29
     */
    private List<DWSProductionOutputHour> createDWSProductionOutputHour(GroupRuleBean rule, Iterable<DWDProductionOutput> iterable) {

        String dayPattern = "yyyy-MM-dd";
        String hourMinutePattern = "yyyy-MM-dd HHmm";
        List<DWSProductionOutputHour> listDws = new ArrayList<>();

        List<DateRuleBean> times = rule.getTimes(); // 时间段数组[24个]
        for(DateRuleBean date : times) {
            Iterator<DWDProductionOutput> iterator = iterable.iterator();
            String startTime = date.getStartTime();
            String endTime = date.getEndTime();

            // 计算 startTime 到  endTime的产量和   此处规则是 如果产量是0 生成一条为0的数据
            int qty = 0;
            DWDProductionOutput next = null;
            while(iterator.hasNext()) {
                next = iterator.next();
                long scanDt = DateTools.timeMillisToLong(next.getScanDt(), hourMinutePattern);
                long start = DateTools.dateStrToLong(DateTools.timeMillisToStr(next.getScanDt(), dayPattern) + " "+ startTime, hourMinutePattern);
                long end = DateTools.dateStrToLong(DateTools.timeMillisToStr(next.getScanDt(), dayPattern) + " "+ endTime, hourMinutePattern);

                if( start < scanDt && scanDt < end) {
                    qty += Integer.parseInt(next.getOutputQty());
                }
            }
            listDws.add(new DWDProductionOutput().buildDWSProductionOutputHour(next, ""+qty, date));
        }
        return listDws;
    }




    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
