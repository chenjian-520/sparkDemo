package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

import java.io.Serializable;

@Data
public class ProductionPartnoDay implements Serializable {
    /**
     * dpm_dws_production_partno_day  料号平台生产状态统计  dws
     * 找到  WH L6
     * Rowkey
     * site_code	    厂区
     * level_code	    BU或者Level
     * factory_code	    工厂
     * process_code	    制程
     * area_code	    区域
     * line_code	    线
     * part_no	        料号
     * plantform        产品系列
     * work_dt	        出勤日期
     * work_shift	    班别
     * ttl_pass_station	过站总数
     * output_qty       真實產量
     * smt_ct	        partno标准CT
     * update_dt
     * update_by
     * data_from
     */
    private String site_code;
    private String level_code;
    private String factory_code;
    private String process_code;
    private String area_code;
    private String line_code;
    private String part_no;
    private String plantform;
    private String work_dt;
    private String work_shift;
    private String ttl_pass_station;
    private String output_qty;
    private String smt_ct;
    private String update_dt;
    private String update_by;
    private String data_from;

    public ProductionPartnoDay() {
    }

    public ProductionPartnoDay(String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String part_no, String plantform, String work_dt, String work_shift, String ttl_pass_station, String output_qty, String smt_ct, String update_dt, String update_by, String data_from) {
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.part_no = part_no;
        this.plantform = plantform;
        this.work_dt = work_dt;
        this.work_shift = work_shift;
        this.ttl_pass_station = ttl_pass_station;
        this.output_qty = output_qty;
        this.smt_ct = smt_ct;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
    }


    public String getSite_code() {
        return site_code;
    }

    public void setSite_code(String site_code) {
        this.site_code = site_code;
    }

    public String getLevel_code() {
        return level_code;
    }

    public void setLevel_code(String level_code) {
        this.level_code = level_code;
    }

    public String getFactory_code() {
        return factory_code;
    }

    public void setFactory_code(String factory_code) {
        this.factory_code = factory_code;
    }

    public String getProcess_code() {
        return process_code;
    }

    public void setProcess_code(String process_code) {
        this.process_code = process_code;
    }

    public String getArea_code() {
        return area_code;
    }

    public void setArea_code(String area_code) {
        this.area_code = area_code;
    }

    public String getLine_code() {
        return line_code;
    }

    public void setLine_code(String line_code) {
        this.line_code = line_code;
    }

    public String getPart_no() {
        return part_no;
    }

    public void setPart_no(String part_no) {
        this.part_no = part_no;
    }

    public String getPlantform() {
        return plantform;
    }

    public void setPlantform(String plantform) {
        this.plantform = plantform;
    }

    public String getWork_dt() {
        return work_dt;
    }

    public void setWork_dt(String work_dt) {
        this.work_dt = work_dt;
    }

    public String getWork_shift() {
        return work_shift;
    }

    public void setWork_shift(String work_shift) {
        this.work_shift = work_shift;
    }

    public String getTtl_pass_station() {
        return ttl_pass_station;
    }

    public void setTtl_pass_station(String ttl_pass_station) {
        this.ttl_pass_station = ttl_pass_station;
    }

    public String getOutput_qty() {
        return output_qty;
    }

    public void setOutput_qty(String output_qty) {
        this.output_qty = output_qty;
    }

    public String getSmt_ct() {
        return smt_ct;
    }

    public void setSmt_ct(String smt_ct) {
        this.smt_ct = smt_ct;
    }

    public String getUpdate_dt() {
        return update_dt;
    }

    public void setUpdate_dt(String update_dt) {
        this.update_dt = update_dt;
    }

    public String getUpdate_by() {
        return update_by;
    }

    public void setUpdate_by(String update_by) {
        this.update_by = update_by;
    }

    public String getData_from() {
        return data_from;
    }

    public void setData_from(String data_from) {
        this.data_from = data_from;
    }

    @Override
    public String toString() {
        return "ProductionPartnoDay{" +
                "site_code='" + site_code + '\'' +
                ", level_code='" + level_code + '\'' +
                ", factory_code='" + factory_code + '\'' +
                ", process_code='" + process_code + '\'' +
                ", area_code='" + area_code + '\'' +
                ", line_code='" + line_code + '\'' +
                ", part_no='" + part_no + '\'' +
                ", plantform='" + plantform + '\'' +
                ", work_dt='" + work_dt + '\'' +
                ", work_shift='" + work_shift + '\'' +
                ", ttl_pass_station='" + ttl_pass_station + '\'' +
                ", output_qty='" + output_qty + '\'' +
                ", smt_ct='" + smt_ct + '\'' +
                ", update_dt='" + update_dt + '\'' +
                ", update_by='" + update_by + '\'' +
                ", data_from='" + data_from + '\'' +
                '}';
    }
}
