package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @author HS
 * @className DpmAdsUtilisationTemp
 * @description TODO
 * @date 2020/4/22 11:16
 */
@Data
public class DpmAdsUtilisationTemp {
    String id;
    String site_code;
    String level_code;
    String factory_code;
    String process_code;
    String building_code;
    String line_code;
    String work_date;
    String workshift_code;
    Integer output_hours;
    Integer line_qty_actual;
    Integer can_produce_time;
    String line_utilisation_actual;
    String line_utilisation_target;
    String etl_time;
}
