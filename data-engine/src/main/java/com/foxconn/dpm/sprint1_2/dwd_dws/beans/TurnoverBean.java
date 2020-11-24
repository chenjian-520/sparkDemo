package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data
 */
@Data
public class TurnoverBean {
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String humresource_type;
    private String work_dt;
    private String emp_id;
    private String onduty_states;
    private String work_shift;

    public TurnoverBean(String site_code, String level_code, String factory_code, String process_code, String humresource_type, String work_dt, String emp_id, String onduty_states, String work_shift) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.humresource_type = humresource_type;
        this.work_dt = work_dt;
        this.emp_id = emp_id;
        this.onduty_states = onduty_states;
        this.work_shift = work_shift;
    }

}
