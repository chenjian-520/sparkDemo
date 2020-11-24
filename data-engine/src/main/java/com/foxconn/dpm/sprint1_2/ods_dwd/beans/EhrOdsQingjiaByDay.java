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
public class EhrOdsQingjiaByDay {
    private String empId;
    private String workDate;
    private String leaveHours;

    public EhrOdsQingjiaByDay(String empId, String workDate, String leaveHours) {
        this.empId = empId;
        this.workDate = workDate;
        this.leaveHours = leaveHours;
    }

    public EhrOdsQingjiaByDay() {
        this.empId = "";
        this.workDate = "";
        this.leaveHours = "";
    }
}
