package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

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
public class UphDwdCt {
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String line_code;
    private String platform;
    private String part_no;
    private String cycle_time;
    private Long update_dt;
    private String update_by;
    private String data_from;

    public UphDwdCt(String site_code, String level_code, String factory_code, String process_code, String line_code, String plantform, String part_no, String cycle_time) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.line_code = line_code;
        this.platform = plantform;
        this.part_no = part_no;
        this.cycle_time = cycle_time;
    }

    public UphDwdCt(String site_code, String level_code, String factory_code, String process_code, String line_code, String plantform, String part_no, String cycle_time, Long update_dt, String update_by, String data_from) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.line_code = line_code;
        this.platform = plantform;
        this.part_no = part_no;
        this.cycle_time = cycle_time;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
    }
}
