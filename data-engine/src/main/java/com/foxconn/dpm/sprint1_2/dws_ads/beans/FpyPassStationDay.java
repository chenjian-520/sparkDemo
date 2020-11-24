package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @version 1.0
 * @program: ehr->FpyPassStationDay
 * @description: Fpy
 * @author: Axin
 * @create: 2020-01-17 17:03
 **/
@Data
public class FpyPassStationDay {
    private String site_code;
    private String level_code;
    private String work_dt;

    private String line_code;
    private String platform;
    private String work_shift;
    private String station_code;
    private String total_count;
    private String update_dt;
    private String update_by;
    private String data_from;
    private String customer;
    private String factory_code;

    public FpyPassStationDay() {
        this.site_code = "";
        this.level_code = "";
        this.line_code = "";
        this.platform = "";
        this.work_dt = "";
        this.work_shift = "";
        this.station_code = "";
        this.total_count = "";
        this.customer = "";
    }


    public FpyPassStationDay(String site_code, String level_code, String line_code, String platform, String work_dt, String work_shift,String station_code,String total_count,String customer) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.platform = platform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.station_code = station_code;
        this.total_count = total_count;
        this.customer = customer;
    }

    public FpyPassStationDay(String site_code, String level_code, String line_code, String platform, String work_dt, String work_shift, String station_code, String total_count, String customer, String factory_code) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.line_code = line_code;
        this.platform = platform;
        this.station_code = station_code;
        this.total_count = total_count;
        this.customer = customer;
        this.factory_code = factory_code;
    }
}
