package com.foxconn.dpm.sprint1_2.dws_ads.beans;


import lombok.Data;

import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * @author HS
 * @className DpmDwsProductionOutputDD
 * @description TODO
 * @date 2020/4/21 13:43
 */
@Data
public class DpmDwsProductionOutputDD {
    String Rowkey;
    String site_code;
    String level_code;
    String factory_code;
    String process_code;
    String area_code;
    String line_code;
    String part_no;
    String sku;
    String platform;
    String workorder_type;
    String work_dt	;
    Float output_qty;
    Float normalized_output_qty;
    String data_granularity;
    String customer;
    Long update_dt;
    String update_by;
    String data_from;
    String work_shift;
}
