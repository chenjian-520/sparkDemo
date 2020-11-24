package com.foxconn.dpm.sprint1_2.ods_dwd.beans;

import lombok.Data;

/**
 * Description:  com.dl.spark.ehr.dwd.bean
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/10
 */
@Data
public class DwdTurnoverOutput {
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String emp_id;
    private String time_of_separation;
    private String org_code; //不用持久化，只是用于计算匹配
    private String department;
    private String update_dt;
    private String update_by;
    private String data_from;

    public DwdTurnoverOutput(String site_code, String level_code, String factory_code, String process_code, String emp_id, String time_of_separation) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.emp_id = emp_id;
        this.time_of_separation = time_of_separation;
    }

    public DwdTurnoverOutput() {
        this.site_code = "";
        this.level_code = "";
        this.factory_code = "";
        this.process_code = "";
        this.emp_id = "";
        this.time_of_separation = "";
    }
}
