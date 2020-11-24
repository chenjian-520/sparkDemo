package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.UUID;

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
@Accessors(chain = true)
public class HrAttendanceOutput {
    private String id;
    private String site_code;
    private String level_code;
    private String work_date;
    private String attendance_headcount; //出勤人力
    private String onjob_headcount; //在职人力
    private String attendance_rate_actual; //实际出勤率
    private String attendance_rate_target;
    private String plan_attendance_qty; //应出勤人力
    private String etl_time; //etl时间

    public HrAttendanceOutput() {
        this.id = UUID.randomUUID().toString().replaceAll("-", "");
        this.site_code = "";
        this.level_code = "";
        this.work_date = "";
        this.attendance_headcount = "";
        this.onjob_headcount = "";
        this.attendance_rate_actual = "";
        this.etl_time = "";
    }

    public HrAttendanceOutput(String site_code, String level_code, String work_date, String attendance_headcount, String onjob_headcount, String attendance_rate_actual, String etl_time) {
        this.id = UUID.randomUUID().toString().replaceAll("-", "");
        this.site_code = site_code;
        this.level_code = level_code;
        this.work_date = work_date;
        this.attendance_headcount = attendance_headcount;
        this.onjob_headcount = onjob_headcount;
        this.attendance_rate_actual = attendance_rate_actual;
        this.etl_time = etl_time;
    }

    public HrAttendanceOutput(String id, String site_code, String level_code, String work_date, String attendance_headcount, String onjob_headcount, String attendance_rate_actual, String attendance_rate_target, String plan_attendance_qty, String etl_time) {
        this.id = id;
        this.site_code = site_code;
        this.level_code = level_code;
        this.work_date = work_date;
        this.attendance_headcount = attendance_headcount;
        this.onjob_headcount = onjob_headcount;
        this.attendance_rate_actual = attendance_rate_actual;
        this.attendance_rate_target = attendance_rate_target;
        this.plan_attendance_qty = plan_attendance_qty;
        this.etl_time = etl_time;
    }
}
