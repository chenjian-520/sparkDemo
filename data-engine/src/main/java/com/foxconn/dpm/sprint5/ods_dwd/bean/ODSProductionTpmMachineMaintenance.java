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
@HBaseTable(level = "ods", table = "dpm_ods_production_tpm_machine_maintenance", family = "DPM_ODS_PRODUCTION_TPM_MACHINE_MAINTENANCE")
public class ODSProductionTpmMachineMaintenance extends HBaseBean {

    @HBaseColumn( column = "site_code")
    private String siteCode;

    @HBaseColumn( column = "level_code")
    private String levelCode;

    @HBaseColumn( column = "factory_code")
    private String factoryCode;

    @HBaseColumn( column = "process_code")
    private String processCode;

    @HBaseColumn( column = "tpm_line_code")
    private String tpmLineCode;

    @HBaseColumn( column = "work_dt")
    private String workDt;

    @HBaseColumn( column = "request_no")
    private String requestNo;

    @HBaseColumn( column = "doc_type")
    private String docType;

    @HBaseColumn( column = "alarm_code")
    private String alarmCode;

    @HBaseColumn( column = "alarm_remark")
    private String alarmRemark;

    @HBaseColumn( column = "eqp_code")
    private String eqpCode;

    @HBaseColumn( column = "eqp_name")
    private String eqpName;

    @HBaseColumn( column = "start_time")
    private String startTime;

    @HBaseColumn( column = "end_time")
    private String endTime;

    @HBaseColumn( column = "created_by")
    private String createdBy;

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
