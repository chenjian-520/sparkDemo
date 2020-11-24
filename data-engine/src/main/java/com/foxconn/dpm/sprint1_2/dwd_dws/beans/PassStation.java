package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;

/**
 * @author Axin
 * @version V1.0
 * @program: ehr
 * @Package com.dl.spark.ehr.dws.bean
 * @Description: TODO
 * @date 2020/4/27 8:54
 */
@Data
public class PassStation {
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String area_code;
    private String line_code;
    private String work_dt;
    private String work_shift;
    private String sku;
    private String part_no;
    private String plantform;
    private String station_code;
    private String station_name;
    private String customer;
    private String is_fail;


    public PassStation(String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String work_dt, String work_shift, String sku, String part_no, String plantform, String station_code, String station_name, String customer,String is_fail) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.sku = sku;
        this.part_no = part_no;
        this.plantform = plantform;
        this.station_code = station_code;
        this.station_name = station_name;
        this.customer = customer;
        this.is_fail = is_fail;
    }


    public PassStation() {
        this.site_code = "";
        this.level_code = "";
        this.factory_code = "";
        this.process_code = "";
        this.area_code = "";
        this.line_code = "";
        this.part_no = "";
        this.sku = "";
        this.plantform = "";
        this.customer = "";
        this.work_dt = "";
        this.work_shift = "";
        this.station_code = "";
        this.station_name = "";
        this.is_fail = "";
    }
}
