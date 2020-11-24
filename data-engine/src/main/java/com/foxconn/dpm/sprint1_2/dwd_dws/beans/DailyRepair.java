package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;

/**
 * @author Axin
 * @version V1.0
 * @program: ehr
 * @Package com.dl.spark.ehr.dws.bean
 * @Description: TODO
 * @date 2020/4/27 11:00
 */
@Data
public class DailyRepair {
    private String site_code;
    private String level_code;
    private String area_code;
    private String line_code;
    private String work_shift;
    private String work_dt;
    private String part_no;
    private String customer;
    private String platform;
//    private String wo;
//    private String sn;
//    private Long repair_in_dt;
//    private String fail_code;
//    private String fail_desc;
//    private Long repair_out_dt;
    private String fail_station;
//    private String repair_code;
//    private String repair_code_desc;


    public DailyRepair(String site_code, String level_code, String area_code, String line_code, String work_shift, String work_dt, String part_no, String customer, String platform, String fail_station) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.work_shift = work_shift;
        this.work_dt = work_dt;
        this.part_no = part_no;
        this.customer = customer;
        this.platform = platform;
        this.fail_station = fail_station;
    }


    public DailyRepair() {
        this.site_code = "";
        this.level_code = "";
        this.area_code = "";
        this.line_code = "";
        this.work_shift = "";
        this.work_dt = "";
        this.part_no = "";
        this.customer = "";
        this.platform = "";
        this.fail_station = "";
    }
}
