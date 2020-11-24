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
public class EhrOdsOrg {
    private String bg;
    private String plant;
    private String dept;
    private String bu;
    private String org_fee_code;
    private String org_name;
    private String org_code;

    public EhrOdsOrg(String bg, String plant, String dept, String bu, String org_fee_code, String org_name, String org_code) {
        this.bg = bg;
        this.plant = plant;
        this.dept = dept;
        this.bu = bu;
        this.org_fee_code = org_fee_code;
        this.org_name = org_name;
        this.org_code = org_code;
    }
}
