package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

import java.util.UUID;

/**
 * Description:  com.dl.spark.ehr.ads.beans
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/4/28
 */
@Data
public class L10UphActualOutput {
    private String id;
    private String site_code;
    private String level_code;
    private String work_date;
    private String uph_actual;
    private String uph_target;
    private String output_qty_actual;
    private String cycle_time_actual;
    private String output_hours;
    private String uph_adherence_actual;
    private String uph_adherence_target;
    private String etl_time;

    public L10UphActualOutput() {
        this.id = UUID.randomUUID().toString().replaceAll("-", "");
        this.site_code = "";
        this.level_code = "";
        this.work_date = "";
        this.uph_actual = "";
        this.uph_target = "";
        this.output_qty_actual = "";
        this.cycle_time_actual = "";
        this.output_hours = "";
        this.uph_adherence_actual = "";
        this.uph_adherence_target = "";
        this.etl_time = "";
    }

    public L10UphActualOutput(String site_code, String level_code, String work_date, String uph_actual, String uph_target, String output_qty_actual, String cycle_time_actual, String output_hours, String uph_adherence_actual, String uph_adherence_target, String etl_time) {
        this.id = UUID.randomUUID().toString().replaceAll("-", "");
        this.site_code = site_code;
        this.level_code = level_code;
        this.work_date = work_date;
        this.uph_actual = uph_actual;
        this.uph_target = uph_target;
        this.output_qty_actual = output_qty_actual;
        this.cycle_time_actual = cycle_time_actual;
        this.output_hours = output_hours;
        this.uph_adherence_actual = uph_adherence_actual;
        this.uph_adherence_target = uph_adherence_target;
        this.etl_time = etl_time;
    }
}
