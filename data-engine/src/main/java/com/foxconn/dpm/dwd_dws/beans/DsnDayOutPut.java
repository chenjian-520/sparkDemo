package com.foxconn.dpm.dwd_dws.beans;

/**
 * @author HS
 * @className DsnDayOutPut
 * @description TODO
 * @date 2019/12/31 10:52
 */
public class DsnDayOutPut {
    String SiteCode;
    String LevelCode;
    String PlantCode;
    String ProcessCode;
    String WOType;
    String Key;
    String WorkDate;
    Long QTY;

    public DsnDayOutPut() {
    }

    public DsnDayOutPut(String siteCode, String levelCode, String plantCode, String processCode, String WOType, String key, String workDate, Long QTY) {
        SiteCode = siteCode;
        LevelCode = levelCode;
        PlantCode = plantCode;
        ProcessCode = processCode;
        this.WOType = WOType;
        Key = key;
        WorkDate = workDate;
        this.QTY = QTY;
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

    public String getWOType() {
        return WOType;
    }

    public void setWOType(String WOType) {
        this.WOType = WOType;
    }

    public String getKey() {
        return Key;
    }

    public void setKey(String key) {
        Key = key;
    }

    public String getWorkDate() {
        return WorkDate;
    }

    public void setWorkDate(String workDate) {
        WorkDate = workDate;
    }

    public Long getQTY() {
        return QTY;
    }

    public void setQTY(Long QTY) {
        this.QTY = QTY;
    }
}
