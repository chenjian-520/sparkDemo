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
public class ODSProductionlfiveStreaming implements Serializable {

    @HBaseColumn( column = "site_code")
    private String site_code;

    @HBaseColumn( column = "level_code")
    private String level_code;

    @HBaseColumn( column = "factory_code")
    private String factory_code;

    @HBaseColumn( column = "process_code")
    private String process_code;

    @HBaseColumn( column = "area_code")
    private String area_code;

    @HBaseColumn( column = "line_code")
    private String line_code;

    @HBaseColumn( column = "work_shift")
    private String work_shift;

    @HBaseColumn( column = "work_dt")
    private String work_dt;

    @HBaseColumn( column = "wo")
    private String wo;

    @HBaseColumn( column = "workorder_type")
    private String workorder_type;

    @HBaseColumn( column = "part_no")
    private String part_no;

    @HBaseColumn( column = "customer")
    private String customer;

    @HBaseColumn( column = "model_no")
    private String model_no;

    @HBaseColumn( column = "sn")
    private String sn;

    @HBaseColumn( column = "station_code")
    private String station_code;

    @HBaseColumn( column = "station_name")
    private String station_name;

    @HBaseColumn( column = "is_pass")
    private String is_pass;

    @HBaseColumn( column = "is_fail")
    private String is_fail;

    @HBaseColumn( column = "scan_by")
    private String scan_by;

    @HBaseColumn( column = "scan_dt")
    private String scan_dt;

    @HBaseColumn( column = "update_dt")
    private String update_dt;

    @HBaseColumn( column = "update_by")
    private String update_by;

    @HBaseColumn( column = "data_from")
    private String data_from;

    @Override
    public String toString() {
        return "ODSProductionlfiveStreaming{" +
                "site_code='" + site_code + '\'' +
                ", level_code='" + level_code + '\'' +
                ", factory_code='" + factory_code + '\'' +
                ", process_code='" + process_code + '\'' +
                ", area_code='" + area_code + '\'' +
                ", line_code='" + line_code + '\'' +
                ", work_shift='" + work_shift + '\'' +
                ", work_dt='" + work_dt + '\'' +
                ", wo='" + wo + '\'' +
                ", workorder_type='" + workorder_type + '\'' +
                ", part_no='" + part_no + '\'' +
                ", customer='" + customer + '\'' +
                ", model_no='" + model_no + '\'' +
                ", sn='" + sn + '\'' +
                ", station_code='" + station_code + '\'' +
                ", station_name='" + station_name + '\'' +
                ", is_pass='" + is_pass + '\'' +
                ", is_fail='" + is_fail + '\'' +
                ", scan_by='" + scan_by + '\'' +
                ", scan_dt='" + scan_dt + '\'' +
                ", update_dt='" + update_dt + '\'' +
                ", update_by='" + update_by + '\'' +
                ", data_from='" + data_from + '\'' +
                '}';
    }

}
