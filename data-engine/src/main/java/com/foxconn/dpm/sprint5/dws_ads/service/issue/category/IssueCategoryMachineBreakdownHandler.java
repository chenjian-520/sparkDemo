package com.foxconn.dpm.sprint5.dws_ads.service.issue.category;

import cn.hutool.core.lang.Singleton;
import cn.hutool.core.util.StrUtil;
import com.foxconn.dpm.enums.IssueCategoryEnum;
import com.foxconn.dpm.enums.TableNameEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionMachineMaintenance;
import com.foxconn.dpm.sprint5.dws_ads.bean.SysIssuetrackingCategory;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;
import com.foxconn.dpm.sprint5.dws_ads.service.AdsDowntimeSpringFiveService;
import com.foxconn.dpm.sprint5.enums.MaintenanceDocTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

/**
 * 机故处理 .
 * todo rdd 需要缓存起来
 *
 * @className: IssueCategoryMachineBreakdownHandler
 * @author: ws
 * @date: 2020/7/14 15:30
 * @version: 1.0.0
 */
@Slf4j
public class IssueCategoryMachineBreakdownHandler implements IssueCategoryHandler {
    /**
     * 处理 .
     *
     * @param downtimeDetailDTO
     * @return boolean
     * @author ws
     * @date 2020/7/14 15:29
     **/
    @Override
    public boolean handle(AdsProductionDowntimeDetailDTO downtimeDetailDTO) {
        DWDProductionMachineMaintenance machineMaintenance =
                getFirstByDocType(downtimeDetailDTO, MaintenanceDocTypeEnum.ALARM);
        if (machineMaintenance != null) {
            SysIssuetrackingCategory issuetrackingCategory = getFirstByName(
                    machineMaintenance.getAlarmRemark());
            if (issuetrackingCategory != null) {
                downtimeDetailDTO.setIssueCategoryCode(issuetrackingCategory.getId())
                        .setIssueCategoryCodeDesc(issuetrackingCategory.getName());
                return true;
            }
        }
        return false;
    }

    /**
     * 根据 docType 取第一个 .
     * @param downtimeDetailDTO
     * @param docTypeEnum
     * @author ws
     * @date 2020/7/14 17:23
     * @return long
     **/
    public DWDProductionMachineMaintenance getFirstByDocType(AdsProductionDowntimeDetailDTO downtimeDetailDTO
            , MaintenanceDocTypeEnum docTypeEnum) {
        JavaRDD<DWDProductionMachineMaintenance> maintenanceJavaRDD = downtimeDetailDTO.getMaintenanceJavaRDD();

        // 规则  siteCode levelCode factoryCode processCode lineCode work_dt doc_type 有数据则是机故
        JavaRDD<DWDProductionMachineMaintenance> filterJavaRDD = maintenanceJavaRDD.filter(item ->
                item.getSiteCode().equals(downtimeDetailDTO.getSiteCode())
                        && item.getLevelCode().equals(downtimeDetailDTO.getLevelCode())
                        && item.getFactoryCode().equals(downtimeDetailDTO.getFactoryCode())
                        && item.getProcessCode().equals(downtimeDetailDTO.getProcessCode())
                        && item.getLineCode().equals(downtimeDetailDTO.getLineCode())
                        && item.getWorkDt().equals(downtimeDetailDTO.getWorkDate())
                        && docTypeEnum.getCode().equals(item.getDocType())
        );
        long count = filterJavaRDD.count();
        if (count == 0) {
            return null;
        }
        return filterJavaRDD.first();
    }

    /**
     * 根据 name 获取第一个 .
     * @param name
     * @author ws
     * @date 2020/7/17 9:35
     * @return com.foxconn.dpm.sprint5.dws_ads.bean.SysIssuetrackingCategory
     **/
    private SysIssuetrackingCategory getFirstByName(String name) {
        if (StrUtil.isBlank(name)) {
            return null;
        }
        StringBuilder sql = new StringBuilder("select * from ");
        sql.append(TableNameEnum.ADS.SYS_ISSUETRACKING_CATEGORY.getTableName())
                .append(" where name = ? limit 1 ");
        AdsDowntimeSpringFiveService adsDowntimeSpringFiveService
                = Singleton.get(AdsDowntimeSpringFiveService.class);
        return adsDowntimeSpringFiveService.getSysIssuetrackingCategoryBySql(
                sql.toString(), name);
    }

}
