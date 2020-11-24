package com.foxconn.dpm.sprint5.dwd_dws.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 日产出SN基础资料 dto .
 *
 * @className: DWDProductionOutputDTO
 * @author: ws
 * @date: 2020/7/3 16:49
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
public class DWDProductionOutputDTO implements Serializable {

    private String siteCode;

    private String levelCode;

    private String factoryCode;

    private String processCode;

    private String areaCode;

    private String lineCode;

    private String machineId;

    private String partNo;

    private String sku;

    private String platform;

    private String customer;

    private String wo;

    private String workorderType;

    private String workDt;

    private String workShift;

    private String sn;

    private String stationCode;

    private String stationName;

    private String isFail;

    private String scanBy;

    private String scanDt;

    private String outputQty;

    private String updateDt;

    private String updateBy;

    private String dataFrom;

    private String cycleTime;

    private String downtimeStation;

    private String downtimeBuffer;

    private String switchModelsStation;

    private String switchModelsBuffer;

    private String inAndOutBuffer;

    private String oldLineCode;

}
