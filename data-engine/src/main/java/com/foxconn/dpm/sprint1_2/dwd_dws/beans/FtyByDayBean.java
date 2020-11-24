package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;

import javax.validation.groups.Default;
import java.io.Serializable;

/**
 * @author wxj
 * @date 2020/1/15 10:06
 */
@Data
public class FtyByDayBean implements Serializable {
    private String id ;
    private String etl_time;
    private float fpy_target;
    private float fpy_actual;
    private String site_code;
    private String level_code;
    private String work_date;
    private String week_id;
    private String month_id;
    private String quarter_id;
    private String year_id;
    private Integer ng_qty;
    private Integer pass_qty;
    private String customer_code;
    private String station_code;
}
