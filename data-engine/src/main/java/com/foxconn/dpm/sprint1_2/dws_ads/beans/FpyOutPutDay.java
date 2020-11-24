package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @version 1.0
 * @program: ehr->FpyOutPutDay
 * @description: output
 * @author: Axin
 * @create: 2020-01-17 17:28
 **/
@Data
public class FpyOutPutDay {
    private String site_code;
    private String level_code;
    private String work_dt;

    private String line_code;
    private String platform;
    private String work_shift;
    private String output_qty;
    private String update_dt;
    private String update_by;
    private String data_from;
    private String customer;
    private String station_code;

    public FpyOutPutDay() {
        this.site_code = "";
        this.level_code = "";
        this.line_code = "";
        this.platform = "";
        this.work_dt = "";
        this.work_shift = "";
        this.output_qty = "";
        this.customer = "";
    }


    public FpyOutPutDay(String site_code, String level_code, String line_code, String platform, String work_dt, String work_shift, String output_qty,String customer) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.platform = platform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.output_qty = output_qty;
        this.customer = customer;
    }

}
