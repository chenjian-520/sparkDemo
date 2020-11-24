package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.UUID;

/**
 * Description:  com.dl.spark.ehr.ads.beans
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/4/29
 */
@Data
@Accessors(chain = true)
public class HrHumanResourceBean {
    private String id;
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String work_date;
    private String emp_humman_resource_code;
    private String headcount_target;
    private String onjob_headcount;
    private String etl_time;
    private String update_dt;

}
