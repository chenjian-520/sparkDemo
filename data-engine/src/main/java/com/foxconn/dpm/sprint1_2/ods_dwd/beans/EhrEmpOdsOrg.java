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
public class EhrEmpOdsOrg {
    private String emp_id;
    private String sbg_code;
    private String l1;
    private String l2;
    private String l3;
    private String l4;
    private String l5;
    private String group_code;
    private String department_code;
    private String level_code; //非持久化字段，仅用于计算
    private Long update_dt;
    private String factory_code;

    public EhrEmpOdsOrg(String emp_id, String sbg_code, String l1, String l2, String l3, String l4, String l5, String group_code, String department_code) {
        this.emp_id = emp_id;
        this.sbg_code = sbg_code;
        this.l1 = l1;
        this.l2 = l2;
        this.l3 = l3;
        this.l4 = l4;
        this.l5 = l5;
        this.group_code = group_code;
        this.department_code = department_code;
        this.level_code = "";
    }

    public EhrEmpOdsOrg(String emp_id, String sbg_code, String l1, String l2, String l3, String l4, String l5, String group_code, String department_code, Long update_dt) {
        this.emp_id = emp_id;
        this.sbg_code = sbg_code;
        this.l1 = l1;
        this.l2 = l2;
        this.l3 = l3;
        this.l4 = l4;
        this.l5 = l5;
        this.group_code = group_code;
        this.department_code = department_code;
        this.level_code = "";
        this.update_dt = update_dt;
    }
}
