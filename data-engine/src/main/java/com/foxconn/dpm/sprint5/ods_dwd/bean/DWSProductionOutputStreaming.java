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
@HBaseTable(level = "dws", table = "dpm_dws_production_output_hh", family = "DPM_DWS_PRODUCTION_OUTPUT_HH")
public class DWSProductionOutputStreaming  implements Serializable {

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

    @HBaseColumn( column = "customer")
    private String customer;

    @HBaseColumn( column = "part_no")
    private String part_no;

    @HBaseColumn( column = "platform")
    private String platform;

    @HBaseColumn( column = "work_dt")
    private String work_dt;

    @HBaseColumn( column = "work_shift")
    private String work_shift;

    @HBaseColumn( column = "start_time_seq")
    private String start_time_seq;

    @HBaseColumn( column = "start_time")
    private String start_time;

    @HBaseColumn( column = "end_time")
    private String end_time;

    @HBaseColumn( column = "station_code")
    private String station_code;

    @HBaseColumn( column = "output_qty")
    private String output_qty;

    @HBaseColumn( column = "output_qty_target")
    private String output_qty_target;

    @HBaseColumn( column = "update_dt")
    private String update_dt;

    @HBaseColumn( column = "data_from")
    private String data_from;

    @HBaseColumn( column = "update_by")
    private String update_by;

    @Override
    public String toString() {
        return "DWSProductionOutputStreaming{" +
                "site_code='" + site_code + '\'' +
                ", level_code='" + level_code + '\'' +
                ", factory_code='" + factory_code + '\'' +
                ", process_code='" + process_code + '\'' +
                ", area_code='" + area_code + '\'' +
                ", line_code='" + line_code + '\'' +
                ", customer='" + customer + '\'' +
                ", part_no='" + part_no + '\'' +
                ", platform='" + platform + '\'' +
                ", work_dt='" + work_dt + '\'' +
                ", work_shift='" + work_shift + '\'' +
                ", start_time_seq='" + start_time_seq + '\'' +
                ", start_time='" + start_time + '\'' +
                ", end_time='" + end_time + '\'' +
                ", station_code='" + station_code + '\'' +
                ", output_qty='" + output_qty + '\'' +
                ", output_qty_target='" + output_qty_target + '\'' +
                ", update_dt='" + update_dt + '\'' +
                ", data_from='" + data_from + '\'' +
                ", update_by='" + update_by + '\'' +
                '}';
    }
}
