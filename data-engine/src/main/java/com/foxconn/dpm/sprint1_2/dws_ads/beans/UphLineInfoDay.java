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
public class UphLineInfoDay {
    private String site_code;
    private String level_code;
    private String line_code;
    private String work_dt;
    private String work_shift;
    private String work_time;
    private String update_dt;
    private String update_by;
    private String data_from;
    private String factory_code;
    private String area_code;
    private String customer;


    public UphLineInfoDay(String site_code, String level_code, String line_code, String work_dt, String work_shift, String work_time, String update_dt, String update_by, String data_from, String area_code) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.work_time = work_time;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
        this.area_code = area_code;
    }
    public UphLineInfoDay(String site_code, String level_code, String line_code, String work_dt, String work_shift, String work_time, String update_dt, String update_by, String data_from, String area_code, String customer) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.work_time = work_time;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
        this.area_code = area_code;
        this.customer = customer;
    }
}
