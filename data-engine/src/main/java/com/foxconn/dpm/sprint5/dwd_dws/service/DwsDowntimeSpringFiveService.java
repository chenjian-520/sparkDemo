package com.foxconn.dpm.sprint5.dwd_dws.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.common.tools.*;
import com.foxconn.dpm.common.util.JoinConditionUtil;
import com.foxconn.dpm.common.util.KeyUtil;
import com.foxconn.dpm.core.base.BaseService;
import com.foxconn.dpm.core.enums.BusinessTypeEnum;
import com.foxconn.dpm.core.enums.LevelEnum;
import com.foxconn.dpm.core.enums.SiteEnum;
import com.foxconn.dpm.core.enums.Strategy;
import com.foxconn.dpm.enums.SuccessFailEnum;
import com.foxconn.dpm.enums.TableNameEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.*;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.DateRuleBean;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.GroupRuleBean;
import com.foxconn.dpm.sprint5.dwd_dws.dto.DWDProductionOutputDTO;
import com.foxconn.dpm.sprint5.dwd_dws.dto.DWSProductionLineDowntimeHHDTO;
import com.google.common.collect.Lists;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * dws 线体停机小时统计 业务类 .
 *
 * @className: DwsDowntimeSpringFiveService
 * @author: ws
 * @date: 2020/7/3 9:28
 * @version: 1.0.0
 */
public class DwsDowntimeSpringFiveService extends BaseService {

    /**
     * 时间序列路径
     */
    private static final String SEQUENTIALLY_PATH =
            "config/sprint5/dwd_dws/production_output/ProductionOutputDayToHours.json";

    /**
     * 绑定业务类型 .
     *
     * @return com.foxconn.dpm.core.enums.BusinessTypeEnum
     * @author ws
     * @date 2020/7/2 14:08
     **/
    @Override
    protected BusinessTypeEnum bindBusinessType() {
        return BusinessTypeEnum.DWS_DOWNTIME_SPRING_FIVE;
    }

    /**
     * 绑定策略 .
     *
     * @return java.util.List<java.util.List < com.foxconn.dpm.core.enums.Strategy>>
     * @author ws
     * @date 2020/7/2 17:15
     **/
    @Override
    protected List<List<Strategy>> bindStrategy() {
        List<List<Strategy>> list = Lists.newArrayList();
        List<Strategy> strategies = Lists.newArrayList(SiteEnum.WH);
        list.add(strategies);
        return list;
    }

    /**
     * 执行任务 .
     *
     * @param parameterMap
     * @return void
     * @author ws
     * @date 2020/7/2 15:32
     **/
    @Override
    public void doTask(Map<String, Object> parameterMap) throws Exception {
        // 获取基础数据
        JavaRDD<Tuple2<String, List<DWDProductionOutputDTO>>> baseDataJavaRDD
                = getBaseDataByJoin(parameterMap);

        log.info("dpm_dwd_production_output join 后的基础数据条数为：{}"
                , baseDataJavaRDD.count());


        // 根据时间差，计算 D，注意：当前集合中的最后一个 endtime 为 N/A，downtime 为 0
        JavaRDD<DWSProductionLineDowntimeHHDTO> downtimeHHDTOJavaRDD
                = computeDowntime(baseDataJavaRDD);

        log.info("计算 downtime 数据条数为：{}", downtimeHHDTOJavaRDD.count());

        // 填充 dpm_dws_production_line_downtime_list
        fillDwsDowntimeList(downtimeHHDTOJavaRDD);

        // 填充 dpm_dws_production_line_downtime_hh
        fillDwsDowntimeHH(downtimeHHDTOJavaRDD);

    }

    /**
     * 计算 downtime .
     * @param baseDataJavaRDD
     * @author ws
     * @date 2020/7/6 11:34
     * @return org.apache.spark.api.java.JavaRDD<com.foxconn.dpm.sprint5.dwd_dws.dto.DWSProductionLineDowntimeHHDTO>
     **/
    private JavaRDD<DWSProductionLineDowntimeHHDTO> computeDowntime(
            JavaRDD<Tuple2<String, List<DWDProductionOutputDTO>>> baseDataJavaRDD) {
        return baseDataJavaRDD
                .map(r -> {
                    List<DWSProductionLineDowntimeHHDTO> dList = Lists.newArrayList();
                    List<DWDProductionOutputDTO> dtos = r._2;
                    if (CollUtil.isEmpty(dtos)) {
                        return dList;
                    }
                    // 该组的对比时间
                    DWDProductionOutputDTO firstDTO = dtos.get(0);
                    // 单位 毫秒
                    long standardTime = 534000L;
                    try {
                        standardTime = NumberUtil.add(firstDTO.getCycleTime()
                                , firstDTO.getInAndOutBuffer()
                                , firstDTO.getDowntimeBuffer()).longValue() * 1000;
                    } catch (Exception e) {
                        log.error("数字转换异常 {}", firstDTO, e);
                    }
                    for (int i = 0; i < dtos.size(); i++) {
                        DWDProductionOutputDTO current = dtos.get(i);
                        String nextScanDt = null;
                        String nextSn = null;
                        String nextPartNo = null;
                        if (i >= dtos.size() - 1) {
                            // 若记录最后一个板子
                            nextScanDt = Long.toString(System.currentTimeMillis());
                            nextSn = SystemConst.NA;
                            nextPartNo = SystemConst.NA;
                        } else {
                            DWDProductionOutputDTO next = dtos.get(i + 1);
                            nextScanDt = next.getScanDt();
                            nextSn = next.getSn();
                            nextPartNo = next.getPartNo();
                        }
                        long downtime = NumberUtil.sub(nextScanDt, current.getScanDt()).longValue();
                        if (downtime >= standardTime) {
                            DWSProductionLineDowntimeHHDTO downtimeHH = new DWSProductionLineDowntimeHHDTO();
                            BeanUtil.copyProperties(current, downtimeHH);
                            downtimeHH.setDownTime(Long.toString(downtime))
                                    .setLossStartTime(current.getScanDt())
                                    .setLossEndTime(nextScanDt)
                                    .setPreSn(current.getSn())
                                    .setNextSn(nextSn)
                                    .setPreSnPartNo(current.getPartNo())
                                    .setNextSnPartNo(nextPartNo);
                            dList.add(downtimeHH);
                        }
                    }
                    return dList;
                }).filter(CollUtil::isNotEmpty).flatMap(List::listIterator);
    }

    /**
     * 填充 dpm_dws_production_line_downtime_list .
     * @param downtimeHHDTOJavaRDD
     * @author ws
     * @date 2020/7/6 13:20
     * @return void
     **/
    private void fillDwsDowntimeList(JavaRDD<DWSProductionLineDowntimeHHDTO> downtimeHHDTOJavaRDD)
            throws Exception {
        JavaRDD<DWSProductionLineDowntimeList> downtimeListJavaRDD = downtimeHHDTOJavaRDD.map(dto -> {
            DWSProductionLineDowntimeList downtimeList = new DWSProductionLineDowntimeList();
            BeanUtil.copyProperties(dto, downtimeList);
            return downtimeList;
        });

        log.info("即将进入 DB dpm_dws_production_line_downtime_list 的数据条数为：{}"
                , downtimeListJavaRDD.count());


        // entity 转 put
        JavaRDD<Put> putJavaRdd = BeanConvertTools.covertBeanToPut(downtimeListJavaRDD
                , DWSProductionLineDowntimeList.class);

        // 写入 hbase
        DPHbase.rddWrite(TableNameEnum.DWS.PRODUCTION_LINE_DOWNTIME_LIST.getTableName(), putJavaRdd);
    }

    /**
     * 填充 dpm_dws_production_line_downtime_hh  .
     * @param downtimeHHDTOJavaRDD
     * @author ws
     * @date 2020/7/6 13:14
     * @return void
     **/
    private void fillDwsDowntimeHH(JavaRDD<DWSProductionLineDowntimeHHDTO> downtimeHHDTOJavaRDD) throws Exception {
        List<GroupRuleBean> ruleList = JsonTools.readJsonArray(SEQUENTIALLY_PATH, GroupRuleBean.class);
        log.info("时间段配置文件：", ruleList.size());

        // 按时间规则计算数据    一天拆分成 24 条
        JavaRDD<List<DWSProductionLineDowntimeHHDTO>> listJavaRDDHHJavaRDD = downtimeHHDTOJavaRDD.map(item -> {
            List<DWSProductionLineDowntimeHHDTO> list = new ArrayList<>();
            for(GroupRuleBean rule : ruleList) {
                // 根据 site_code 和 level_code 筛选出对应的规则
                if (rule.getSiteCode().equals(item.getSiteCode())
                        && rule.getLevelCode().equals(item.getLevelCode())) {
                    list.addAll(rebuildDWSProductionLineDowntimeHH(rule, item));
                }
            }

            return list;
        });

        // dto -> entity
        JavaRDD<DWSProductionLineDowntimeHH> javaRdd = listJavaRDDHHJavaRDD.flatMap(item -> item.iterator())
                .map(item -> {
                    DWSProductionLineDowntimeHH downtimeHH = new DWSProductionLineDowntimeHH();
                    BeanUtil.copyProperties(item, downtimeHH);
                    return downtimeHH;
                });
        log.info("即将进入 DB dpm_dws_production_line_downtime_hh 的数据条数为：{}", javaRdd.count());

        // entity 转 put
        JavaRDD<Put> putJavaRdd = BeanConvertTools.covertBeanToPut(javaRdd, DWSProductionLineDowntimeHH.class);

        // 写入 hbase
        DPHbase.rddWrite(TableNameEnum.DWS.PRODUCTION_LINE_DOWNTIME_HH.getTableName(), putJavaRdd);
    }

    /**
     * 通过 join 获取基础数据 .
     * @param parameterMap
     * @author ws
     * @date 2020/7/6 11:27
     * @return org.apache.spark.api.java.JavaRDD<scala.Tuple2<java.lang.String,java.util.List<com.foxconn.dpm.sprint5.dwd_dws.dto.DWDProductionOutputDTO>>>
     **/
    private JavaRDD<Tuple2<String, List<DWDProductionOutputDTO>>> getBaseDataByJoin(
            Map<String, Object> parameterMap) throws Exception {

        // 读取HBase获取数据
        JavaRDD<DWDProductionOutput> dwdProductionOutputJavaRDD = HBaseTools
                .getSaltRdd(TableNameEnum.DWD.PRODUCTION_OUTPUT.getTableName()
                        , DWDProductionOutput.class, parameterMap)
                .filter(r -> SiteEnum.WH.getCode().equals(r.getSiteCode()))
                .filter(r -> LevelEnum.L6.getCode().equals(r.getLevelCode()));
        log.info("对象 DWDProductionOutput 数据条数为：{}", dwdProductionOutputJavaRDD.count());

        JavaRDD<DWDProductionStandaryCt> ctJavaRDD = HBaseTools
                .getRdd(TableNameEnum.DWD.PRODUCTION_STANDARY_CT.getTableName()
                        , DWDProductionStandaryCt.class, parameterMap)
                .filter(r -> StrUtil.isNotBlank(r.getCycleTime()))
                .filter(r -> SiteEnum.WH.getCode().equals(r.getSiteCode()))
                .filter(r -> LevelEnum.L6.getCode().equals(r.getLevelCode()));
        log.info("对象 DWDProductionStandaryCt 数据条数为：{}", ctJavaRDD.count());

        JavaRDD<ODSProductionDowntimeBuffer> downtimeBufferJavaRDD = HBaseTools
                .getRdd(TableNameEnum.ODS.PRODUCTION_DOWNTIME_BUFFER.getTableName()
                        , ODSProductionDowntimeBuffer.class, parameterMap)
                .filter(r -> SiteEnum.WH.getCode().equals(r.getSiteCode()))
                .filter(r -> LevelEnum.L6.getCode().equals(r.getLevelCode()));
        log.info("对象 ODSProductionDowntimeBuffer 数据条数为：{}", downtimeBufferJavaRDD.count());

        JavaRDD<DimProductionActLineMapping> actLineMappingJavaRDD = HBaseTools
                .getRdd(TableNameEnum.DIM.DIM_PRODUCTION_ACT_LINE_MAPPING.getTableName()
                        , DimProductionActLineMapping.class, parameterMap);
        log.info("对象 actLineMappingJavaRDD 数据条数为：{}", actLineMappingJavaRDD.count());

        // 获取过站状态成功的数据
        JavaRDD<DWDProductionOutput> successProductionJavaRDD = dwdProductionOutputJavaRDD.filter(data -> {
            return SuccessFailEnum.SUCCESS.getCode().equals(data.getIsFail()); // 0 == 成功
        });
        log.info("对象DWDProductionOutput获取过站状态成功的数据数据条数为：{}", successProductionJavaRDD.count());

        // key 为 siteCode levelCode factoryCode processCode lineCode area
        JavaPairRDD<String, DWDProductionOutput> outputMappingJavaPairRDD =
                successProductionJavaRDD.mapToPair((PairFunction<DWDProductionOutput, String, DWDProductionOutput>)
                        item -> new Tuple2<String, DWDProductionOutput>(
                                JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLineArea(item), item));


        // join act_line_mapping 表  start
        JavaPairRDD<String, DimProductionActLineMapping> mappingKeyJavaPairRDD = actLineMappingJavaRDD.mapToPair(
                (PairFunction<DimProductionActLineMapping, String, DimProductionActLineMapping>)
                        item -> new Tuple2<String, DimProductionActLineMapping>(
                                JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLineArea(item), item));

        JavaRDD<DWDProductionOutputDTO> mappingJavaRDD = outputMappingJavaPairRDD
                .leftOuterJoin(mappingKeyJavaPairRDD)
                .map(item -> {
                    Tuple2<DWDProductionOutput, Optional<DimProductionActLineMapping>> data = item._2;
                    DWDProductionOutputDTO dto = new DWDProductionOutputDTO();
                    BeanUtil.copyProperties(data._1, dto);
                    if (!data._2.isPresent()) {
                        log.info("DimProductionActLineMapping 存在不匹配数据 字段为：{}，值为：{}"
                                , "lineCode", item._1);
                        return null;
                    }
                    DimProductionActLineMapping actLineMapping = data._2.get();
                    dto.setOldLineCode(dto.getLineCode())
                            .setLineCode(actLineMapping.getActLineCode());
                    return dto;
                }).filter(item -> item != null);

        log.info("join DimProductionActLineMapping 表后的数据条数为：{}", mappingJavaRDD.count());

        // join act_line_mapping 表  end

        // join CT start
        JavaPairRDD<String, DWDProductionOutputDTO> outputCtJavaPairRDD =
                mappingJavaRDD.mapToPair((PairFunction<DWDProductionOutputDTO, String, DWDProductionOutputDTO>)
                        item -> new Tuple2<String, DWDProductionOutputDTO>(
                                JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLinePartNoOrPlatform(item), item));

        JavaPairRDD<String, DWDProductionStandaryCt> ctJavaPairRDD = ctJavaRDD.mapToPair(
                (PairFunction<DWDProductionStandaryCt, String, DWDProductionStandaryCt>)
                        item -> new Tuple2<String, DWDProductionStandaryCt>(
                                JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLinePartNoOrPlatform(item), item));

        JavaRDD<DWDProductionOutputDTO> dtoJavaRDD = outputCtJavaPairRDD.leftOuterJoin(ctJavaPairRDD)
                .map(item -> {
                    Tuple2<DWDProductionOutputDTO, Optional<DWDProductionStandaryCt>> data = item._2;
                    DWDProductionOutputDTO dto = data._1;
                    if (!data._2.isPresent()) {
                        log.info("DWDProductionStandaryCt 存在不匹配数据 字段为：{}，值为：{}"
                                , "partNo", item._1);
                        return null;
                    }
                    // 还原回 line_code
                    dto.setLineCode(dto.getOldLineCode());
                    DWDProductionStandaryCt standaryCt = data._2.get();
                    dto.setCycleTime(standaryCt.getCycleTime());
                    return dto;
                }).filter(item -> item != null);

        log.info("join ct 表后的数据条数为：{}", dtoJavaRDD.count());

        // join CT end

        // join downtime_buffer 表   start
        // 条件： siteCode levelCode factoryCode processCode areaCode lineCode
        JavaPairRDD<String, DWDProductionOutputDTO> dtoJavaPairRDD = dtoJavaRDD.mapToPair(
                (PairFunction<DWDProductionOutputDTO, String, DWDProductionOutputDTO>)
                item -> new Tuple2<String, DWDProductionOutputDTO>(
                        JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLineArea(item)
                        , item));


        JavaPairRDD<String, ODSProductionDowntimeBuffer> downtimeBufferJavaPairRDD = downtimeBufferJavaRDD.mapToPair(
                (PairFunction<ODSProductionDowntimeBuffer, String, ODSProductionDowntimeBuffer>)
                item -> new Tuple2<String, ODSProductionDowntimeBuffer>(
                        JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLineArea(item)
                        , item));


        // 包含所有的数据
        JavaRDD<DWDProductionOutputDTO> dtoJavaRDDAll = dtoJavaPairRDD.leftOuterJoin(downtimeBufferJavaPairRDD)
                .map(item -> {
                    Tuple2<DWDProductionOutputDTO, Optional<ODSProductionDowntimeBuffer>> data = item._2;
                    DWDProductionOutputDTO dto = data._1;
                    dto.setUpdateDt(Long.toString(System.currentTimeMillis()))
                            .setDataFrom(TableNameEnum.DWD.PRODUCTION_OUTPUT.getTableName());
                    if (!data._2.isPresent()) {
                        return null;
                    }
                    // 按照 station_code 过滤
                    ODSProductionDowntimeBuffer downtimeBuffer = data._2.get();
                    String downtimeStation = downtimeBuffer.getDowntimeStation();
                    if (StrUtil.isBlank(downtimeStation)) {
                        return null;
                    }
                    boolean stationMatch = Arrays.stream(downtimeStation.split(SystemConst.COMMA))
                            .anyMatch(station -> station.equals(dto.getStationCode()));
                    if (!stationMatch) {
                        return null;
                    }
                    BeanUtil.copyProperties(downtimeBuffer, dto);
                    return dto;
                }).filter(item -> item != null);

        log.info("join buffer 表后的数据条数为：{}", dtoJavaRDDAll.count());
        // join downtime_buffer 表   end

        // 分组并排序
        return dtoJavaRDDAll.groupBy(r ->
                KeyUtil.buildKey(r.getSiteCode(), r.getLevelCode(), r.getFactoryCode()
                        , r.getProcessCode(), r.getAreaCode(), r.getLineCode(), r.getStationCode()))
                .map(r -> {
                    String key = r._1;
                    List<DWDProductionOutputDTO> adtos = Lists.newArrayList(r._2);
                    // 按修改时间升序
                    adtos.sort(Comparator.comparing(DWDProductionOutputDTO::getScanDt));
                    return new Tuple2<>(key, adtos);
                });
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
    private List<DWSProductionLineDowntimeHHDTO> rebuildDWSProductionLineDowntimeHH(GroupRuleBean rule
            , DWSProductionLineDowntimeHHDTO downtimeHHDTO) {

        List<DWSProductionLineDowntimeHHDTO> listDws = new ArrayList<>();
        // 时间段数组[24个]
        List<DateRuleBean> times = rule.getTimes();
        // 按时间升序排序
        times.sort(Comparator.comparing(DateRuleBean::getStartTimeSeq));
        for (int i = 0; i < times.size(); i++) {
            List<DWSProductionLineDowntimeHHDTO> tempList = downtimeHHDTO.splitTime(times, i);
            // 如果一条都不存在， 插入一条NA数据
            if (tempList.isEmpty()) {
                tempList.add(downtimeHHDTO.buildDefaultDWSProductionLineDowntimeHH(times.get(i)));
            }
            listDws.addAll(tempList);
        }
        return clearNAData(listDws);
    }

    /**
     * 清除 N/A 数据 .
     * @param listDws
     * @author ws
     * @date 2020/7/8 15:54
     * @return java.util.List<com.foxconn.dpm.sprint5.dwd_dws.dto.DWSProductionLineDowntimeHHDTO>
     **/
    private List<DWSProductionLineDowntimeHHDTO> clearNAData(List<DWSProductionLineDowntimeHHDTO> listDws) {
        Map<String, List<DWSProductionLineDowntimeHHDTO>> map = listDws.stream()
                .collect(Collectors.groupingBy(item ->
                    KeyUtil.buildKey(item.getWorkDt(), item.getStartTime(), item.getEndTime())
        ));

        return map.entrySet().stream()
                .map(entry -> {
                    List<DWSProductionLineDowntimeHHDTO> value = entry.getValue();
                    if (value.size() == 1) {
                        return value;
                    }
                    return value.stream()
                            .filter(data ->!(SystemConst.NA.equals(data.getDownTime())))
                            .collect(Collectors.toList());
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }


}
