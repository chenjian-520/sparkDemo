package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

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
    private String factory_code;
    private String process_code;
    private String area_code;
    private String line_code;
    private String part_no;
    private String platform;
    private String work_dt;
    private String work_shift;
    private String smt_ttl_pass;
    private String output_qty;
    private String ct;
    private String update_dt;
    private String update_by;
    private String data_from;
    private String customer;

    public UphPartnoOutput() {
        this.site_code = "";
        this.level_code = "";
        this.factory_code = "";
        this.process_code = "";
        this.area_code = "";
        this.line_code = "";
        this.part_no = "";
        this.platform = "";
        this.work_dt = "";
        this.work_shift = "";
        this.smt_ttl_pass = "";
        this.output_qty = "";
        this.ct = "";
    }

    public UphPartnoOutput(String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String part_no, String platform, String work_dt, String work_shift, String smt_ttl_pass, String output_qty, String ct) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.part_no = part_no;
        this.platform = platform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.smt_ttl_pass = smt_ttl_pass;
        this.output_qty = output_qty;
        this.ct = ct;
    }
    public UphPartnoOutput(String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String part_no, String platform, String work_dt, String work_shift, String smt_ttl_pass, String output_qty, String ct, String customer) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.part_no = part_no;
        this.platform = platform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.smt_ttl_pass = smt_ttl_pass;
        this.output_qty = output_qty;
        this.ct = ct;
        this.customer = customer;
    }

    public UphPartnoOutput(String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String part_no, String plantform, String work_dt, String work_shift, String smt_ttl_pass, String output_qty, String ct, String update_dt, String update_by, String data_from) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.part_no = part_no;
        this.platform = plantform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.smt_ttl_pass = smt_ttl_pass;
        this.output_qty = output_qty;
        this.ct = ct;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
    }
}
