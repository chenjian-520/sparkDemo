package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;

/**
 * @author HS
 * @className DsnDayOutPut
 * @description TODO
 * @date 2019/12/31 10:52
 */
@Data
public class DsnDayOutPut {
    String SiteCode;//
    String LevelCode;//

    String PlantCode;
    String ProcessCode;
    String area_code;
    String line_code;
    String part_no;
    String sku;
    String plantform;
    String WOType;

    String WorkDate;//

    String customer;

    String Key;//
    Long QTY;//
    String work_shift;

    public DsnDayOutPut() {
    }

    public DsnDayOutPut(String siteCode, String levelCode, String plantCode, String processCode, String area_code, String line_code, String part_no, String sku, String plantform, String WOType, String workDate, String customer, String key, Long QTY, String work_shift) {
        SiteCode = siteCode;
        LevelCode = levelCode;
        PlantCode = plantCode;
        ProcessCode = processCode;
        this.area_code = area_code;
        this.line_code = line_code;
        this.part_no = part_no;
        this.sku = sku;
        this.plantform = plantform;
        this.WOType = WOType;
        WorkDate = workDate;
        this.customer = customer;
        Key = key;
        this.QTY = QTY;
        this.work_shift = work_shift;
    }

    public String getSiteCode() {
        return SiteCode;
    }

    public void setSiteCode(String siteCode) {
        SiteCode = siteCode;
    }

    public String getLevelCode() {
        return LevelCode;
    }

    public void setLevelCode(String levelCode) {
        LevelCode = levelCode;
    }

    public String getPlantCode() {
        return PlantCode;
    }

    public void setPlantCode(String plantCode) {
        PlantCode = plantCode;
    }

    public String getProcessCode() {
        return ProcessCode;
    }

    public void setProcessCode(String processCode) {
        ProcessCode = processCode;
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

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getPlantform() {
        return plantform;
    }

    public void setPlantform(String plantform) {
        this.plantform = plantform;
    }

    public String getWOType() {
        return WOType;
    }

    public void setWOType(String WOType) {
        this.WOType = WOType;
    }

    public String getWorkDate() {
        return WorkDate;
    }

    public void setWorkDate(String workDate) {
        WorkDate = workDate;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public String getKey() {
        return Key;
    }

    public void setKey(String key) {
        Key = key;
    }

    public Long getQTY() {
        return QTY;
    }

    public void setQTY(Long QTY) {
        this.QTY = QTY;
    }

    public String getWork_shift() {
        return work_shift;
    }

    public void setWork_shift(String work_shift) {
        this.work_shift = work_shift;
    }
}
