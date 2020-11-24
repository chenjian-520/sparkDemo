package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @author HS
 * @className DpmDwsPersonnelEmpWorkhoursDD
 * @description TODO
 * @date 2020/4/21 15:10
 */
@Data
public class DpmDwsPersonnelEmpWorkhoursDD {
    String Rowkey;
    String site_code;
    String level_code;
    String factory_code;
    String process_code;
    String line_code;
    String work_dt;
    String work_shift;
    String humresource_type;
    Integer attendance_qty;
    Float act_attendance_workhours;
    String data_granularity;
    Long update_dt;
    String update_by;
    String data_from;
    String area_code;
}



