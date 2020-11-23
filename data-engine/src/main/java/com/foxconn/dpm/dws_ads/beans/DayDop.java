package com.foxconn.dpm.dws_ads.beans;

/**
 * @author HS
 * @className DayDop
 * @description TODO
 * @date 2019/12/25 15:23
 */
public class DayDop {
    String WorkDate;
    String FactoryCode;
    String SBGCode;
    String BGCode;
    String BUCode;
    String PartNo;
    String ModelNo;
    Double QTY;

    public DayDop() {
    }

    public DayDop(String workDate, String factoryCode, String SBGCode, String BGCode, String BUCode, String partNo, String modelNo, Double QTY) {
        WorkDate = workDate;
        FactoryCode = factoryCode;
        this.SBGCode = SBGCode;
        this.BGCode = BGCode;
        this.BUCode = BUCode;
        PartNo = partNo;
        ModelNo = modelNo;
        this.QTY = QTY;
    }

    public String getWorkDate() {
        return WorkDate;
    }

    public void setWorkDate(String workDate) {
        WorkDate = workDate;
    }

    public String getFactoryCode() {
        return FactoryCode;
    }

    public void setFactoryCode(String factoryCode) {
        FactoryCode = factoryCode;
    }

    public String getSBGCode() {
        return SBGCode;
    }

    public void setSBGCode(String SBGCode) {
        this.SBGCode = SBGCode;
    }

    public String getBGCode() {
        return BGCode;
    }

    public void setBGCode(String BGCode) {
        this.BGCode = BGCode;
    }

    public String getBUCode() {
        return BUCode;
    }

    public void setBUCode(String BUCode) {
        this.BUCode = BUCode;
    }

    public String getPartNo() {
        return PartNo;
    }

    public void setPartNo(String partNo) {
        PartNo = partNo;
    }

    public String getModelNo() {
        return ModelNo;
    }

    public void setModelNo(String modelNo) {
        ModelNo = modelNo;
    }

    public Double getQTY() {
        return QTY;
    }

    public void setQTY(Double QTY) {
        this.QTY = QTY;
    }

    @Override
    public String toString() {
        return "DayDop{" +
                "WorkDate='" + WorkDate + '\'' +
                ", FactoryCode='" + FactoryCode + '\'' +
                ", SBGCode='" + SBGCode + '\'' +
                ", BGCode='" + BGCode + '\'' +
                ", BUCode='" + BUCode + '\'' +
                ", PartNo='" + PartNo + '\'' +
                ", ModelNo='" + ModelNo + '\'' +
                ", QTY=" + QTY +
                '}';
    }
}