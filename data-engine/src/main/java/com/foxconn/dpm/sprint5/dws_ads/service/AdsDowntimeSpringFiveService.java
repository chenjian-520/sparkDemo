package com.foxconn.dpm.sprint5.dws_ads.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Entity;
import com.foxconn.dpm.DpMysql;
import com.foxconn.dpm.common.tools.HBaseTools;
import com.foxconn.dpm.core.base.BaseService;
import com.foxconn.dpm.core.enums.BusinessTypeEnum;
import com.foxconn.dpm.core.enums.SiteEnum;
import com.foxconn.dpm.core.enums.Strategy;
import com.foxconn.dpm.core.exception.BusinessException;
import com.foxconn.dpm.enums.IssueCategoryEnum;
import com.foxconn.dpm.enums.TableNameEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionMachineMaintenance;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWSProductionLineDowntimeHH;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWSProductionLineDowntimeList;
import com.foxconn.dpm.sprint5.dwd_dws.bean.ODSProductionDowntimeBuffer;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDay;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDetail;
import com.foxconn.dpm.sprint5.dws_ads.bean.SysIssuetrackingCategory;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;
import com.foxconn.dpm.sprint5.dws_ads.service.issue.category.IssueCategoryManagemer;
import com.google.common.collect.Lists;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 线体停机小时统计 业务类 .
 *
 * @className: AdsDowntimeSpringFiveService
 * @author: ws
 * @date: 2020/7/2 15:55
 * @version: 1.0.0
 */
public class AdsDowntimeSpringFiveService extends BaseService {
    /**
     * 绑定业务类型 .
     *
     * @return com.foxconn.dpm.core.enums.BusinessTypeEnum
     * @author ws
     * @date 2020/7/2 14:08
     **/
    @Override
    protected BusinessTypeEnum bindBusinessType() {
        return BusinessTypeEnum.ADS_DOWNTIME_SPRING_FIVE;
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
        // 填充 dpm_ads_production_downtime_detail
        fillDowntimeDetail(parameterMap);

        // 填充 dpm_ads_production_downtime_day
        fillDowntimeDay(parameterMap);
    }

    /**
     * 填充 dpm_ads_production_downtime_day .
     * @param parameterMap
     * @author ws
     * @date 2020/7/6 15:50
     * @return void
     **/
    public void fillDowntimeDay(Map<String, Object> parameterMap) throws Exception {
        String dwsTableName = TableNameEnum.DWS.PRODUCTION_LINE_DOWNTIME_HH.getTableName();

        JavaRDD<DWSProductionLineDowntimeHH> downtimeHHJavaRDD = HBaseTools
                .getRdd(dwsTableName, DWSProductionLineDowntimeHH.class, parameterMap);
        log.info("对象 DWSProductionLineDowntimeHH 数据条数为：{}", downtimeHHJavaRDD.count());

        // 转换成ADS对象
        JavaRDD<AdsProductionDowntimeDay> list = downtimeHHJavaRDD
                .map(DWSProductionLineDowntimeHH::convert2AdsProductionDowntimeDay);
        log.info("插入 DB dpm_ads_production_downtime_day 数据条数为：{}", list.count());

        // 插入mysql
        DpMysql.insertOrUpdateData(list, AdsProductionDowntimeDay.class);
    }

    /**
     * 填充 dpm_ads_production_downtime_detail .
     * @param parameterMap
     * @author ws
     * @date 2020/7/6 15:50
     * @return void
     **/
    public void fillDowntimeDetail(Map<String, Object> parameterMap) throws Exception {
        String dwsTableName = TableNameEnum.DWS.PRODUCTION_LINE_DOWNTIME_LIST.getTableName();

        JavaRDD<DWSProductionLineDowntimeList> downtimeListJavaRDD = HBaseTools
                .getRdd(dwsTableName, DWSProductionLineDowntimeList.class, parameterMap);
        log.info("对象 DWSProductionLineDowntimeList 数据条数为：{}", downtimeListJavaRDD.count());

        // 转换成ADS对象
        JavaRDD<AdsProductionDowntimeDetail> listJavaRDD = downtimeListJavaRDD
                .map(item -> {
                    AdsProductionDowntimeDetail downtimeDetail = item.convert2AdsProductionDowntimeDetail();

                    return downtimeDetail;
                });
        log.info("对象 AdsProductionDowntimeDetail 数据条数为：{}", listJavaRDD.count());

        // 填充 issue_category_code
        List<AdsProductionDowntimeDetail> details = fillIssueCategoryCode(listJavaRDD, parameterMap);
        log.info("插入 DB dpm_ads_production_downtime_detail 数据条数为：{}", details.size());

        // 插入mysql
        DpMysql.insertOrUpdateData(DPSparkApp.getContext().parallelize(details), AdsProductionDowntimeDetail.class);
    }

    /**
     * issue_category_code 填充逻辑未做，逻辑如下：
     * dpm_ads_production_downtime_detail 表 issue_category_code（范围：1-5）填充逻辑
     *
     * 1 交接班
     * 	L6		7:30-7:45	19:30-19:45
     * 	L5/L10 	8:00-8:15   20:00-20:15
     * 	e.g. 以 L6 举例 loss_start_time: 7:20 loss_end_time: 7:50
     * 		判断条件：(7:30 <= loss_start_time && loss_end_time <= 7:45)
     * 					|| (19:30 <= loss_start_time && loss_end_time <= 19:45)
     * 2 机故
     * 	dpm_dwd_production_machine_maintenance 关联条件 line_code，work_dt，doc_type = R 有数据则是机故
     * 3 机种切换
     * 	dpm_ods_production_downtime_buffer
     * 	downtime > switch_models_buffer 并且前后两个 sn 是不同的 part_no（part_no 在 downtime_list表中没有，需要去过站表查，条件就是 sn）
     * 4 保养
     * 	dpm_dwd_production_machine_maintenance 关联条件 line_code，work_dt，doc_type = M	有数据则是保养
     * 5 未排产
     * 	downtime 大于 4小时
     * 注意：若1 - 5都未命中，则不填充 issue_category_code
     * @param listJavaRDD
     * @author ws
     * @date 2020/7/9 17:39
     * @return
     **/
    private List<AdsProductionDowntimeDetail> fillIssueCategoryCode(JavaRDD<AdsProductionDowntimeDetail> listJavaRDD
            , Map<String, Object> parameterMap) {
        List<AdsProductionDowntimeDetail> details = listJavaRDD.collect();
        try {
            // 获取 DWDProductionMachineMaintenance 数据
            JavaRDD<DWDProductionMachineMaintenance> maintenanceJavaRDD = HBaseTools
                    .getRdd(TableNameEnum.DWD.PRODUCTION_MACHINE_MAINTENANCE.getTableName()
                            , DWDProductionMachineMaintenance.class, parameterMap).cache();

            // 获取 ODSProductionDowntimeBuffer 数据
            JavaRDD<ODSProductionDowntimeBuffer> downtimeBufferJavaRDD = HBaseTools
                    .getRdd(TableNameEnum.ODS.PRODUCTION_DOWNTIME_BUFFER.getTableName()
                            , ODSProductionDowntimeBuffer.class, parameterMap).cache();

            // 获取 issue 配置信息
            Map<IssueCategoryEnum, SysIssuetrackingCategory> categoryMap
                    = getSysIssuetrackingCategory();

            details.forEach(item -> {
                AdsProductionDowntimeDetailDTO downtimeDetailDTO
                        = new AdsProductionDowntimeDetailDTO();
                BeanUtil.copyProperties(item, downtimeDetailDTO);
                downtimeDetailDTO.setDowntimeBufferJavaRDD(downtimeBufferJavaRDD)
                        .setMaintenanceJavaRDD(maintenanceJavaRDD)
                        .setSysIssuetrackingCategoryMap(categoryMap);

                // 填充 issue_category_code
                IssueCategoryManagemer.getInstance().fillIssueCategory(downtimeDetailDTO);

                item.setIssueCategoryCode(downtimeDetailDTO.getIssueCategoryCode())
                        .setIssueCategoryCodeDesc(downtimeDetailDTO.getIssueCategoryCodeDesc())
                        .setDtNo(System.currentTimeMillis() + RandomUtil.randomInt(100, 1000) + "")
                        .setCurrentStatus(StrUtil.isBlank(item.getIssueCategoryCode())
                                ? Boolean.FALSE.toString() : Boolean.TRUE.toString());

            });
        } catch (Exception e) {
            log.error("填充 issue_category_code 异常", e);
            throw new BusinessException(e);
        }
        return details;
    }

    /**
     * 获取 IssueCategory 配置信息.
     * @author ws
     * @date 2020/7/16 17:15
     * @return java.util.Map<com.foxconn.dpm.enums.IssueCategoryEnum,com.foxconn.dpm.sprint5.dws_ads.bean.SysIssuetrackingCategory>
     **/
    private Map<IssueCategoryEnum, SysIssuetrackingCategory> getSysIssuetrackingCategory() {
        Map<IssueCategoryEnum, SysIssuetrackingCategory> map = new HashMap<>();
        Arrays.stream(IssueCategoryEnum.values())
                .forEach(issueCategoryEnum -> {
                    // 机故单独处理
                    if (IssueCategoryEnum.MACHINE_BREAKDOWN == issueCategoryEnum) {
                        return;
                    }
                    StringBuilder sql = new StringBuilder("select * from ");
                    sql.append(TableNameEnum.ADS.SYS_ISSUETRACKING_CATEGORY.getTableName())
                            .append(" where id = ? ");

                    SysIssuetrackingCategory issuetrackingCategory =
                            getSysIssuetrackingCategoryBySql(sql.toString(), issueCategoryEnum.getId());
                    if (issuetrackingCategory == null) {
                        return;
                    }
                    map.put(issueCategoryEnum, issuetrackingCategory);
                });
        return map;
    }

    /**
     * 根据 sql 查配置表 .
     * @param sql
     * @param params
     * @author ws
     * @date 2020/7/17 10:08
     * @return com.foxconn.dpm.sprint5.dws_ads.bean.SysIssuetrackingCategory
     **/
    public SysIssuetrackingCategory getSysIssuetrackingCategoryBySql(String sql, Object... params) {
        List<Entity> entities = DpMysql.queryBySql(sql, params);
        if (CollUtil.isEmpty(entities)) {
            return null;
        }
        String id = entities.get(0).getStr("id");
        String name = entities.get(0).getStr("name");
        SysIssuetrackingCategory sysIssuetrackingCategory = new SysIssuetrackingCategory();
        return sysIssuetrackingCategory.setId(id)
                .setName(name);
    }

}
