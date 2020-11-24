package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

/**
 * @author HS
 * @className EHR_MAN_HOUR
 * @description TODO
 * @date 2020/1/3 15:35
 */
public class EHR_MAN_HOUR {
    String WorkDT;
    String SiteCodeID;
    String LevelCodeID;
    Double DL1;
    Double DL2V;
    Integer DL2F;
    Integer IDL;

    public EHR_MAN_HOUR() {
    }

    public EHR_MAN_HOUR(String workDT, String siteCodeID, String levelCodeID, Double DL1, Double DL2V, Integer DL2F, Integer IDL) {
        WorkDT = workDT;
        SiteCodeID = siteCodeID;
        LevelCodeID = levelCodeID;
        this.DL1 = DL1;
        this.DL2V = DL2V;
        this.DL2F = DL2F;
        this.IDL = IDL;
    }

    public String getWorkDT() {
        return WorkDT;
    }

    public void setWorkDT(String workDT) {
        WorkDT = workDT;
    }

    public String getSiteCodeID() {
        return SiteCodeID;
    }

    public void setSiteCodeID(String siteCodeID) {
        SiteCodeID = siteCodeID;
    }

    public String getLevelCodeID() {
        return LevelCodeID;
    }

    public void setLevelCodeID(String levelCodeID) {
        LevelCodeID = levelCodeID;
    }

    public Double getDL1() {
        return DL1;
    }

    public void setDL1(Double DL1) {
        this.DL1 = DL1;
    }

    public Double getDL2V() {
        return DL2V;
    }

    public void setDL2V(Double DL2V) {
        this.DL2V = DL2V;
    }

    public Integer getDL2F() {
        return DL2F;
    }

    public void setDL2F(Integer DL2F) {
        this.DL2F = DL2F;
    }

    public Integer getIDL() {
        return IDL;
    }

    public void setIDL(Integer IDL) {
        this.IDL = IDL;
    }


    @Override
    public String toString() {
        return "EHR_MAN_HOUR{" +
                "WorkDT='" + WorkDT + '\'' +
                ", SiteCodeID='" + SiteCodeID + '\'' +
                ", LevelCodeID='" + LevelCodeID + '\'' +
                ", DL1=" + DL1 +
                ", DL2V=" + DL2V +
                ", DL2F=" + DL2F +
                ", IDL=" + IDL +
                '}';
    }
}
