package com.foxconn.dpm.sprint4.dws_ads.bean;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.UUID;

/**
 * Description:  com.dl.spark.ehr.dws.dto
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/16
 */
@Data
@Accessors(chain = true)
public class HrTurnoverOutput {
    private String id;
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String humresource_type;

    private String work_date;
    private String turnover_headcount; //离职人力
    private String onjob_headcount; //在职人力
    private String turnover_rate_actual; //实际离职率
    private String etl_time; //etl时间
    private String update_dt; //实际离职率

}
