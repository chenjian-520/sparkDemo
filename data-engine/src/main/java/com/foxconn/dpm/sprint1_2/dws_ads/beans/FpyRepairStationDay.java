package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @version 1.0
 * @program: ehr->FpyRepairStationDay
 * @description: repair
 * @author: Axin
 * @create: 2020-01-17 17:10
 **/
@Data
public class FpyRepairStationDay {

    String site_code;
    String level_code;
    String factory_code;
    String process_code;
    String area_code;
    String line_code;
    String work_dt;
    String work_shift;
    String sku;
    String part_no;
    String platform;
    String fail_station;
    String total_count;
    String customer;
    String data_granularity;
    String update_dt;
    String update_by;
    String data_from;

    public FpyRepairStationDay() {
        this.site_code = "";
        this.level_code = "";
        this.line_code = "";
        this.platform = "";
        this.work_dt = "";
        this.work_shift = "";
        this.fail_station = "";
        this.total_count = "";
        this.customer = "";
    }

    public FpyRepairStationDay(String site_code, String level_code, String line_code, String platform, String work_dt, String work_shift, String fail_station, String total_count, String customer) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.platform = platform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.fail_station = fail_station;
        this.total_count = total_count;
        this.customer = customer;
    }

}
