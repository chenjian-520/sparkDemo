package com.foxconn.dpm.sprint1_2.ods_dwd.beans;

import lombok.Data;

/**
 * Description:  com.dl.spark.ehr.dwd.bean
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/15
 */
@Data
public class DwdUphCtOutput {
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String line_code;
    private String plantform;
    private String part_no;
    private String cycle_time;
    private String update_dt;
    private String update_by;
    private String data_from;

    public DwdUphCtOutput() {
        this.site_code = "WH";
        this.level_code = "L10";
        this.factory_code = "L10";
        this.process_code = "L10";
        this.line_code = "";
        this.plantform = "";
        this.part_no = "";
        this.cycle_time = "";
    }

    public DwdUphCtOutput(String site_code, String level_code, String factory_code, String process_code, String line_code, String plantform, String part_no, String cycle_time) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.line_code = line_code;
        this.plantform = plantform;
        this.part_no = part_no;
        this.cycle_time = cycle_time;
    }
}
