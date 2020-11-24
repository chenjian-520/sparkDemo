package com.foxconn.dpm.sprint5.dws_ads.service.issue.category;

import cn.hutool.core.lang.Singleton;
import com.foxconn.dpm.enums.IssueCategoryEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionMachineMaintenance;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;
import com.foxconn.dpm.sprint5.enums.MaintenanceDocTypeEnum;

/**
 * 保养处理 .
 *
 * @className: IssueCategoryMaintenanceHandler
 * @author: ws
 * @date: 2020/7/14 15:30
 * @version: 1.0.0
 */
public class IssueCategoryMaintenanceHandler implements IssueCategoryHandler {
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
        IssueCategoryMachineBreakdownHandler breakdownHandler
                = Singleton.get(IssueCategoryMachineBreakdownHandler.class);
        DWDProductionMachineMaintenance machineMaintenance = breakdownHandler
                .getFirstByDocType(downtimeDetailDTO, MaintenanceDocTypeEnum.MAINTENANCE);
        if (machineMaintenance != null) {
            downtimeDetailDTO.fillIssueCategory(IssueCategoryEnum.MAINTENANCE);
            return true;
        }
        return false;
    }
}
