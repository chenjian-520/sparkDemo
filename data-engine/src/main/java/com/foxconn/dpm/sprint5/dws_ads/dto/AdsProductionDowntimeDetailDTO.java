package com.foxconn.dpm.sprint5.dws_ads.dto;

import com.foxconn.dpm.common.annotation.MySqlColumn;
import com.foxconn.dpm.common.enums.MySqlDataTypes;
import com.foxconn.dpm.enums.IssueCategoryEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.DWDProductionMachineMaintenance;
import com.foxconn.dpm.sprint5.dwd_dws.bean.ODSProductionDowntimeBuffer;
import com.foxconn.dpm.sprint5.dws_ads.bean.SysIssuetrackingCategory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Map;

/**
 * downtime 详情表 dto .
 *
 * @className: AdsProductionDowntimeDetailDTO
 * @author: ws
 * @date: 2020/7/16 16:24
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
public class AdsProductionDowntimeDetailDTO implements Serializable {

    private String id;

    private String regionCode;

    private String regionCodeDesc;

    private String siteCode;

    private String siteCodeDesc;

    private String buildingCode;

    private String buildingCodeDesc;

    private String blockCode;

    private String blockCodeDesc;

    private String lineCode;

    private String lineCodeDesc;

    private String machineId;

    private String levelCode;

    private String levelCodeDesc;

    private String factoryCode;

    private String factoryCodeDesc;

    private String processCode;

    private String processCodeDesc;

    private String customerCode;

    private String customerCodeDesc;

    private String productTypeGroupCode;

    private String productTypeGroupDesc;

    private String productTypeCode;

    private String productTypeDesc;

    private String plantCode;

    private String plantCodeDesc;

    private String platform;

    private String sku;

    private String partNo;

    private String workDate;

    private String workshiftCode;

    private String workshiftCodeDesc;

    private String sectionId;

    private String sectionDesc;

    private String custSectionId;

    private String custSectionDesc;

    private String errorCode;

    private String errorDesc;

    private String dtNo;

    private String issueCategoryCode;

    private String issueCategoryCodeDesc;

    private String errorStartTime;

    private String errorEndTime;

    private String primaryPerson;

    private String currentStatus;

    private String etlTime;

    private String preSn;

    private String nextSn;

    private String preSnPartNo;

    private String nextSnPartNo;

    private JavaRDD<DWDProductionMachineMaintenance> maintenanceJavaRDD;

    private JavaRDD<ODSProductionDowntimeBuffer> downtimeBufferJavaRDD;

    private Map<IssueCategoryEnum, SysIssuetrackingCategory> sysIssuetrackingCategoryMap;

    /**
     * 填充 IssueCategory .
     * @param issueCategoryEnum
     * @author ws
     * @date 2020/7/16 18:26
     * @return void
     **/
    public void fillIssueCategory(IssueCategoryEnum issueCategoryEnum) {
        Map<IssueCategoryEnum, SysIssuetrackingCategory> map = getSysIssuetrackingCategoryMap();
        SysIssuetrackingCategory category = map.get(issueCategoryEnum);
        if (category == null) {
            return;
        }
        setIssueCategoryCode(category.getId());
        setIssueCategoryCodeDesc(category.getName());
    }

}
