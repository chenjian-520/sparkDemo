package com.foxconn.dpm.sprint1_2.ods_dwd.beans;

import lombok.Data;

/**
 * Description:  com.dl.spark.ehr.dwd
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/2
 */
@Data
public class EhrOdsKaoqin {
    private String emp_id;
    private String kq_date;
    private String late_times;
    private String leave_early_times;
    private String absent_times;
    private String ot_late_times;
    private String ot_leave_early_times ;
    private String ot_absent_times;
    private String uss_start_time;
    private String ur_start_time;
    private String us_end_time;
    private String ur_end_time;
    private String ds_start_time;
    private String dr_start_time;
    private String ds_end_time;
    private String dr_end_time;
    private String PersonPost; //emp_info表的字段，用于合并
    private String update_dt; //emp_info表的字段，用于合并

    public EhrOdsKaoqin(String emp_id, String kq_date, String late_times, String leave_early_times, String absent_times, String ot_late_times, String ot_leave_early_times, String ot_absent_times, String uss_start_time, String ur_start_time, String us_end_time, String ur_end_time, String ds_start_time, String dr_start_time, String ds_end_time, String dr_end_time, String personPost) {
        this.emp_id = emp_id;
        this.kq_date = kq_date;
        this.late_times = late_times;
        this.leave_early_times = leave_early_times;
        this.absent_times = absent_times;
        this.ot_late_times = ot_late_times;
        this.ot_leave_early_times = ot_leave_early_times;
        this.ot_absent_times = ot_absent_times;
        this.uss_start_time = uss_start_time;
        this.ur_start_time = ur_start_time;
        this.us_end_time = us_end_time;
        this.ur_end_time = ur_end_time;
        this.ds_start_time = ds_start_time;
        this.dr_start_time = dr_start_time;
        this.ds_end_time = ds_end_time;
        this.dr_end_time = dr_end_time;
        PersonPost = personPost;
    }
    public EhrOdsKaoqin(String emp_id, String kq_date, String late_times, String leave_early_times, String absent_times, String ot_late_times, String ot_leave_early_times, String ot_absent_times, String uss_start_time, String ur_start_time, String us_end_time, String ur_end_time, String ds_start_time, String dr_start_time, String ds_end_time, String dr_end_time, String personPost,String update_dt) {
        this.emp_id = emp_id;
        this.kq_date = kq_date;
        this.late_times = late_times;
        this.leave_early_times = leave_early_times;
        this.absent_times = absent_times;
        this.ot_late_times = ot_late_times;
        this.ot_leave_early_times = ot_leave_early_times;
        this.ot_absent_times = ot_absent_times;
        this.uss_start_time = uss_start_time;
        this.ur_start_time = ur_start_time;
        this.us_end_time = us_end_time;
        this.ur_end_time = ur_end_time;
        this.ds_start_time = ds_start_time;
        this.dr_start_time = dr_start_time;
        this.ds_end_time = ds_end_time;
        this.dr_end_time = dr_end_time;
        this.PersonPost = personPost;
        this.update_dt = update_dt;
    }
}
