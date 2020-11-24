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
public class L6SASiteLineBean {

    private String site_code;           //	厂区
    private String level_code;          //	BU或者Level
    private String factory_code;        //	工厂
    private String process_code;        //	制程
    private String area_code;           //	区域
    private String line_code;           //	线
    private String part_no;             //	料号
    private String sku;                 //	SKU
    private String platform	;           //  产品系列
    private String workorder_type;      //	工单模式
    private String customer;            //	客户
    private String work_dt;             //	工作日
    private String work_shift;          //	班别
    private String output_qty;          //	真实产量
    private String normalized_output_qty;  //	约当后的产量
    private String data_granularity;        //	数据统计粒度
    private String update_dt;

    public L6SASiteLineBean(String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String part_no, String sku, String platform, String workorder_type, String customer, String work_dt, String work_shift, String output_qty, String normalized_output_qty, String data_granularity ,String update_dt) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.part_no = part_no;
        this.sku = sku;
        this.platform = platform;
        this.workorder_type = workorder_type;
        this.customer = customer;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.output_qty = output_qty;
        this.normalized_output_qty = normalized_output_qty;
        this.data_granularity = data_granularity;
        this.update_dt = update_dt;
    }

}
