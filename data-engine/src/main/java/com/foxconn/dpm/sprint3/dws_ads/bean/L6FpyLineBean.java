package com.foxconn.dpm.sprint3.dws_ads.bean;

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
public class L6FpyLineBean {

    private String site_code;
    private String level_code;
    private String line_code;
    private String station_code;
    private String work_dt;
    private String customer;
    private String fail_count;
    private String total_count;

    public L6FpyLineBean(String site_code, String level_code, String line_code, String station_code, String work_dt, String customer, String fail_count, String total_count) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.line_code = line_code;
        this.station_code = station_code;
        this.work_dt = work_dt;
        this.customer = customer;
        this.fail_count = fail_count;
        this.total_count = total_count;
    }


}
