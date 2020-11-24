package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * Description:  com.dl.spark.ehr.dws.dto
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/16
 */
@Data
public class UphPartnoOutput {
    private String site_code;
    private String level_code;
    private String line_code;
    private String platform;
    private String work_dt;
    private String work_shift;
    private String output_qty;
    private String ct;
    private String update_dt;
    private String update_by;
    private String data_from;
    private String data_granularity;
    private String process_code;
    private String customer;
    private String area_code;

    public UphPartnoOutput(String site_code, String level_code, String line_code, String platform, String work_dt, String work_shift, String output_qty, String ct, String update_dt, String update_by, String data_from, String area_code) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.platform = platform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.output_qty = output_qty;
        this.ct = ct;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
        this.area_code = area_code;
    }
    public UphPartnoOutput(String site_code, String level_code, String line_code, String platform, String work_dt, String work_shift, String output_qty, String ct, String update_dt, String update_by, String data_from, String data_granularity, String process_code, String customer, String area_code) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.platform = platform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.output_qty = output_qty;
        this.ct = ct;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
        this.data_granularity = data_granularity;
        this.process_code = process_code;
        this.customer = customer;
        this.area_code = area_code;
    }
}
