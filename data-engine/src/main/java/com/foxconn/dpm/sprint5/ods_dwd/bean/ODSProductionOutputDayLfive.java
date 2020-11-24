package com.foxconn.dpm.sprint5.ods_dwd.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 数据处理业务类 
 *
 * @author cj
 * @version 1.0.0
 * @Description
 * @data 2020-07-06 
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "ods", table = "dpm_ods_production_output_day_lfive", family = "DPM_ODS_PRODUCTION_OUTPUT_DAY_LFIVE_BAK")
public class ODSProductionOutputDayLfive implements Serializable {

    @HBaseColumn( column = "site_code")
    private String siteCode;

    @HBaseColumn( column = "level_code")
    private String levelCode;

    @HBaseColumn( column = "factory_code")
    private String factorycode;

    @HBaseColumn( column = "process_code")
    private String processCode;

    @HBaseColumn( column = "area_code")
    private String areaCode;

    @HBaseColumn( column = "line_code")
    private String lineCode;

    @HBaseColumn( column = "work_shift")
    private String workShift;

    @HBaseColumn( column = "work_dt")
    private String workDt;

    @HBaseColumn( column = "wo")
    private String wo;

    @HBaseColumn( column = "workorder_type")
    private String workorderType;

    @HBaseColumn( column = "part_no")
    private String partNo;

    @HBaseColumn( column = "customer")
    private String customer;

    @HBaseColumn( column = "model_no")
    private String modelNo;

    @HBaseColumn( column = "sn")
    private String sn;

    @HBaseColumn( column = "station_code")
    private String stationCode;

    @HBaseColumn( column = "station_name")
    private String stationName;

    @HBaseColumn( column = "is_pass")
    private String isPass;

    @HBaseColumn( column = "is_fail")
    private String isFail;

    @HBaseColumn( column = "scan_by")
    private String scanBy;

    @HBaseColumn( column = "scan_dt")
    private String scanDt;

    @HBaseColumn( column = "update_dt")
    private String updateDt;

    @HBaseColumn( column = "update_by")
    private String updateBy;

    @HBaseColumn( column = "data_from")
    private String dataFrom;



}
