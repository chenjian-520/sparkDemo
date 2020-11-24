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
public class EhrOdsJiaban {
    private String emp_id;
    private String ot_date;
    private String ot_hours;

    public EhrOdsJiaban() {
        emp_id = "";
        this.ot_date = "";
        this.ot_hours = "";
    }

    public EhrOdsJiaban(String emp_id, String ot_date, String ot_hours) {
        this.emp_id = emp_id;
        this.ot_date = ot_date;
        this.ot_hours = ot_hours;
    }
}
