package com.foxconn.dpm.sprint4.dws_ads.bean;

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
    private String factory_code;
    private String work_date;
    private String attendance_headcount; //出勤人力
    private String onjob_headcount; //在职人力
    private String attendance_rate_actual; //实际出勤率
    private String attendance_rate_target;
    private String plan_attendance_qty; //应出勤人力
    private String etl_time; //etl时间
}
