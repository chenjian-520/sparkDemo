package com.foxconn.dpm.sprint5.ods_dwd.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @Author HY
 * @Date 2020/6/28 17:30
 * @Description
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "ods", table = "dpm_ods_production_sfc_tpm_line_mapping", family = "DPM_ODS_PRODUCTION_SFC_TPM_LINE_MAPPING")
public class ODSProductionSfcTpmLineMapping extends HBaseBean {

    @HBaseColumn( column = "site_code")
    private String siteCode;

    @HBaseColumn( column = "level_code")
    private String levelCode;

    @HBaseColumn( column = "factory_code")
    private String factoryCode;

    @HBaseColumn( column = "process_code")
    private String processCode;

    @HBaseColumn( column = "tpm_line_code")
    private String  tpmLineCode;

    @HBaseColumn( column = "area_code")
    private String  areaCode;

    @HBaseColumn( column = "line_code")
    private String  lineCode;

    @HBaseColumn( column = "update_dt")
    private String updateDt;

    @HBaseColumn( column = "update_by")
    private String updateBy;

    @HBaseColumn( column = "data_from")
    private String dataFrom;

    @HBaseColumn( column = "rowkey")
    private String rowKey;
    public String groupBy(){
        return this.getSiteCode() + this.getLevelCode() + this.getFactoryCode() + this.getProcessCode() + this.getTpmLineCode();
    }

    @Override
    public String getBaseRowKey() {
        return null;
    }
}
