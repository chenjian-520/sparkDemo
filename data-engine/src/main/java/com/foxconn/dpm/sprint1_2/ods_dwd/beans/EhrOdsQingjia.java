package com.foxconn.dpm.sprint1_2.ods_dwd.beans;

import lombok.Data;

/**
 * Description:  com.dl.spark.ehr.dwd.dto
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/3
 */
@Data
public class EhrOdsQingjia {
    private String emp_id;
    private String leave_type;
    private String start_date;
    private String start_time;
    private String end_date;
    private String end_time;
    private String total_hours;
    private String sign_flag;
    private String is_modify;
    private String is_agree;

    public EhrOdsQingjia() {
        this.emp_id = "";
        this.leave_type = "";
        this.start_date = "";
        this.start_time = "";
        this.end_date = "";
        this.end_time = "";
        this.total_hours = "";
        this.sign_flag = "";
        this.is_modify = "";
        this.is_agree = "";
    }

    public EhrOdsQingjia(String emp_id, String leave_type, String start_date, String start_time, String end_date, String end_time, String total_hours, String sign_flag, String is_modify, String is_agree) {
        this.emp_id = emp_id;
        this.leave_type = leave_type;
        this.start_date = start_date;
        this.start_time = start_time;
        this.end_date = end_date;
        this.end_time = end_time;
        this.total_hours = total_hours;
        this.sign_flag = sign_flag;
        this.is_modify = is_modify;
        this.is_agree = is_agree;
    }
}
