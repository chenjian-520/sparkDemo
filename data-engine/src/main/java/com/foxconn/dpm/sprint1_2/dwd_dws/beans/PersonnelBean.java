package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data
 *
 */
@Data
@Accessors(chain = true)
public class PersonnelBean {

    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String emp_id;
    private String humresource_type;
    private String time_of_separation;
    private String update_dt;

    public PersonnelBean(String site_code, String level_code, String factory_code, String process_code, String emp_id, String humresource_type, String time_of_separation, String update_dt) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.emp_id = emp_id;
        this.humresource_type = humresource_type;
        this.time_of_separation = time_of_separation;
        this.update_dt = update_dt;
    }
}
