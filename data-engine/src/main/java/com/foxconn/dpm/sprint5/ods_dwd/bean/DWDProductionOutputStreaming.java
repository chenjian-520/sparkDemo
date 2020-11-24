package com.foxconn.dpm.sprint5.ods_dwd.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;

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
@HBaseTable(level = "dwd", table = "dpm_dwd_production_output_bak", family = "DPM_DWD_PRODUCTION_OUTPUT_BAK")
public class DWDProductionOutputStreaming  implements Serializable {
    /**addsalt+WorkDT+Site+LevelCode+Line+IsFail+UUID*/
    @HBaseColumn( column = "site_code")
    private String site_code;

    @HBaseColumn( column = "level_code")
    private String level_code;

    @HBaseColumn( column = "factory_code")
    private String factory_code;

    @HBaseColumn( column = "process_code")
    private String process_code;

    @HBaseColumn( column = "area_code")
    private String  area_code;

    @HBaseColumn( column = "line_code")
    private String  line_code;

    @HBaseColumn( column = "machine_id")
    private String machine_id;

    @HBaseColumn( column = "part_no")
    private String part_no;

    @HBaseColumn( column = "sku")
    private String sku;

    @HBaseColumn( column = "platform")
    private String platform;

    @HBaseColumn( column = "customer")
    private String customer;

    @HBaseColumn( column = "wo")
    private String wo;

    @HBaseColumn( column = "workorder_type")
    private String workorder_type;

    @HBaseColumn( column = "work_dt")
    private String work_dt;

    @HBaseColumn( column = "work_shift")
    private String work_shift;

    @HBaseColumn( column = "sn")
    private String sn;

    @HBaseColumn( column = "station_code")
    private String station_code;

    @HBaseColumn( column = "station_name")
    private String station_name;

    @HBaseColumn( column = "is_fail")
    private String is_fail;

    @HBaseColumn( column = "scan_by")
    private String scan_by;

    @HBaseColumn( column = "scan_dt")
    private String scan_dt;

    @HBaseColumn( column = "output_qty")
    private String output_qty;

    @HBaseColumn( column = "update_dt")
    private String update_dt;

    @HBaseColumn( column = "data_from")
    private String data_from;

    @HBaseColumn( column = "update_by")
    private String update_by;

    private static SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");


}
