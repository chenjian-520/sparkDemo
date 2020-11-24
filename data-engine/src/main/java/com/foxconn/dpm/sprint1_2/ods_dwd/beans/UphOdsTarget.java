package com.foxconn.dpm.sprint1_2.ods_dwd.beans;

import lombok.Data;

/**
 * Description:  com.dl.spark.ehr.dwd.dto
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/15
 */
@Data
public class UphOdsTarget {
    private String day_id;
    private String platfrom;
    private String line_code;
    private String uph_target;
    private Long update_dt;
    private String update_by;
    private String data_from;
    private String site_code;
    public UphOdsTarget(String day_id, String platfrom, String line_code, String uph_target, Long update_dt, String update_by, String data_from) {
        this.day_id = day_id;
        this.platfrom = platfrom;
        this.line_code = line_code;
        this.uph_target = uph_target;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
    }
    public UphOdsTarget(String day_id, String platfrom, String line_code, String uph_target, Long update_dt, String update_by, String data_from, String site_code) {
        this.day_id = day_id;
        this.platfrom = platfrom;
        this.line_code = line_code;
        this.uph_target = uph_target;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
        this.site_code = site_code;
    }
}
