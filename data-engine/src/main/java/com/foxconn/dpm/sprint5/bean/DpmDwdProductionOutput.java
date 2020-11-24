package com.foxconn.dpm.sprint5.bean;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-06-22
 */
@Data
@Accessors(chain = true)
public class DpmDwdProductionOutput {
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String area_code;
    private String line_code;
    private String machine_id;
    private String part_no;
    private String sku;
    private String platform;
    private String customer;
    private String wo;
    private String workorder_type;
    private String work_dt;
    private String work_shift;
    private String sn;
    private String station_code;
    private String station_name;
    private String is_fail;
    private String scan_by;
    private String scan_dt;
    private String output_qty;

    public DpmDwdProductionOutput(String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String machine_id, String part_no, String sku, String platform, String customer, String wo, String workorder_type, String work_dt, String work_shift, String sn, String station_code, String station_name, String is_fail, String scan_by, String scan_dt, String output_qty) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.machine_id = machine_id;
        this.part_no = part_no;
        this.sku = sku;
        this.platform = platform;
        this.customer = customer;
        this.wo = wo;
        this.workorder_type = workorder_type;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.sn = sn;
        this.station_code = station_code;
        this.station_name = station_name;
        this.is_fail = is_fail;
        this.scan_by = scan_by;
        this.scan_dt = scan_dt;
        this.output_qty = output_qty;
    }

    @Override
    public String toString() {
        return "DpmDwdProductionOutput{" +
                "site_code='" + site_code + '\'' +
                ", level_code='" + level_code + '\'' +
                ", factory_code='" + factory_code + '\'' +
                ", process_code='" + process_code + '\'' +
                ", area_code='" + area_code + '\'' +
                ", line_code='" + line_code + '\'' +
                ", machine_id='" + machine_id + '\'' +
                ", part_no='" + part_no + '\'' +
                ", sku='" + sku + '\'' +
                ", platform='" + platform + '\'' +
                ", customer='" + customer + '\'' +
                ", wo='" + wo + '\'' +
                ", workorder_type='" + workorder_type + '\'' +
                ", work_dt='" + work_dt + '\'' +
                ", work_shift='" + work_shift + '\'' +
                ", sn='" + sn + '\'' +
                ", station_code='" + station_code + '\'' +
                ", station_name='" + station_name + '\'' +
                ", is_fail='" + is_fail + '\'' +
                ", scan_by='" + scan_by + '\'' +
                ", scan_dt='" + scan_dt + '\'' +
                ", output_qty='" + output_qty + '\'' +
                '}';
    }
}
