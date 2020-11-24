package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @author HS
 * @className DpmAdsQualityFpyDayTemp
 * @description TODO
 * @date 2020/1/16 19:03
 */
@Data
public class DpmAdsQualityFpyDayTemp {
    String id;

    String work_date;
    String site_code;
    String level_code;

    Float fpy_actual;
    Float fpy_target;

    String etl_time;


    public DpmAdsQualityFpyDayTemp() {
    }

    public DpmAdsQualityFpyDayTemp(String id, String work_date, String site_code, String level_code, Float fpy_actual, Float fpy_target, String etl_time) {
        this.id = id;
        this.work_date = work_date;
        this.site_code = site_code;
        this.level_code = level_code;
        this.fpy_actual = fpy_actual;
        this.fpy_target = fpy_target;
        this.etl_time = etl_time;
    }
}
