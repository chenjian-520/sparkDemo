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
public class EhrOdsEmpinfo {
    private String emp_id;
    private String human_resource_category_text;
    private String person_post;
    private String org_code;
    private String resign_date;
    private Long update_dt;
    private String positiontype;

    public EhrOdsEmpinfo(String emp_id, String human_resource_category_text, String person_post, String org_code) {
        this.emp_id = emp_id;
        this.human_resource_category_text = human_resource_category_text;
        this.person_post = person_post;
        this.org_code = org_code;
        this.resign_date="";
    }

    //重载构造和之前的程序兼容
    public EhrOdsEmpinfo(String emp_id, String human_resource_category_text, String person_post, String org_code, Long update_dt,String positiontype) {
        this.emp_id = emp_id;
        this.human_resource_category_text = human_resource_category_text;
        this.person_post = person_post;
        this.org_code = org_code;
        this.update_dt = update_dt;
        this.resign_date="";
        this.positiontype = positiontype;
    }

    public EhrOdsEmpinfo(String emp_id, String human_resource_category_text, String person_post, String org_code, String resign_date, Long update_dt ,String positiontype) {
        this.emp_id = emp_id;
        this.human_resource_category_text = human_resource_category_text;
        this.person_post = person_post;
        this.org_code = org_code;
        this.resign_date = resign_date;
        this.update_dt = update_dt;
        this.positiontype = positiontype;
    }
    public EhrOdsEmpinfo(String emp_id, String human_resource_category_text, String person_post, String org_code, String resign_date, Long update_dt) {
        this.emp_id = emp_id;
        this.human_resource_category_text = human_resource_category_text;
        this.person_post = person_post;
        this.org_code = org_code;
        this.resign_date = resign_date;
        this.update_dt = update_dt;
    }
}
