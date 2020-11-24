package com.foxconn.dpm.sprint5.dwd_dws.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 线体异常缓冲时间配置表 .
 *
 * @className: DWDProductionStandaryCt
 * @author: ws
 * @date: 2020/7/3 15:56
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "dwd", table = "dpm_ods_production_downtime_buffer", family = "DPM_ODS_PRODUCTION_DOWNTIME_BUFFER")
public class ODSProductionDowntimeBuffer extends HBaseBean {

    @HBaseColumn( column = "site_code")
    private String siteCode;

    @HBaseColumn( column = "level_code")
    private String levelCode;

    @HBaseColumn( column = "factory_code")
    private String factoryCode;

    @HBaseColumn( column = "process_code")
    private String processCode;

    @HBaseColumn( column = "area_code")
    private String areaCode;

    @HBaseColumn( column = "line_code")
    private String lineCode;

    @HBaseColumn( column = "downtime_station")
    private String downtimeStation;

    @HBaseColumn( column = "downtime_buffer")
    private String downtimeBuffer;

    @HBaseColumn( column = "switch_models_station")
    private String switchModelsStation;

    @HBaseColumn( column = "switch_models_buffer")
    private String switchModelsBuffer;

    @HBaseColumn( column = "In_and out_buffer")
    private String inAndOutBuffer;

    @HBaseColumn( column = "not_scheduled_buffer")
    private String notScheduledBuffer;


    @Override
    public String getBaseRowKey() {
        return null;
    }
}
