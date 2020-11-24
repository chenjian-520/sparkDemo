package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

import java.util.UUID;

/**
 * @author Axin
 * @version V1.0
 * @program: ehr
 * @Package com.dl.spark.ehr.ads.beans
 * @Description: TODO
 * @date 2020/4/30 14:48
 */
@Data
public class L10FpyActualOutput {

    private String id;
    private String site_code;
    private String level_code;
    private String work_date;
    private Float fpy_actual;
    private Float fpy_target;
    private String etl_time;
}
