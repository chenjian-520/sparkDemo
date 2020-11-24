package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;

import java.io.Serializable;

@Data
public class DpmAdsProductionSiteTotalKpiDay implements Serializable {
    /**
     * id
     * site_code
     * site_code_desc
     * work_date
     * online_dl_headcount_actual
     * offline_var_dl_headcount_actual
     * offline_fix_dl_headcount_actual
     * idl_headcount_actual
     * turnover_rate_actual
     * turnover_rate_target
     * attendance_rate_actual
     * attendance_rate_target
     * safety_actual
     * safety_target
     * etl_time
     */
    private String id;
    private String site_code;
    private String site_code_desc;
    private String work_date;
    private int online_dl_headcount_actual;
    private int offline_var_dl_headcount_actual;
    private int offline_fix_dl_headcount_actual;
    private int idl_headcount_actual;
    private int turnover_rate_actual;
    private int turnover_rate_target;
    private int attendance_rate_actual;
    private int attendance_rate_target;
    private int safety_actual;
    private int safety_target;
    private String etl_time;

    public DpmAdsProductionSiteTotalKpiDay() {
        super();
    }


}
