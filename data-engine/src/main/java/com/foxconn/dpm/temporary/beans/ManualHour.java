package com.foxconn.dpm.temporary.beans;

/**
 * @author HS
 * @className ManualHour
 * @description TODO
 * @WorkDate 2019/12/25 15:23
 */
public class ManualHour {
    String Date;
    String Group;
    String Site;
    String Level;
    String BU;
    String Factory;
    String Line;
    Double DL1_TTL_Manhour;
    Double Output;
    Double DL2_Variable_Manhour;
    Double Offline_DL_fixed_headcount;

    public ManualHour() {
    }

    public ManualHour(String date, String group, String site, String level, String BU, String factory, String line, Double DL1_TTL_Manhour, Double output, Double DL2_Variable_Manhour, Double offline_DL_fixed_headcount) {
        Date = date;
        Group = group;
        Site = site;
        Level = level;
        this.BU = BU;
        Factory = factory;
        Line = line;
        this.DL1_TTL_Manhour = DL1_TTL_Manhour;
        Output = output;
        this.DL2_Variable_Manhour = DL2_Variable_Manhour;
        Offline_DL_fixed_headcount = offline_DL_fixed_headcount;
    }

    public String getDate() {
        return Date;
    }

    public void setDate(String date) {
        Date = date;
    }

    public String getGroup() {
        return Group;
    }

    public void setGroup(String group) {
        Group = group;
    }

    public String getSite() {
        return Site;
    }

    public void setSite(String site) {
        Site = site;
    }

    public String getLevel() {
        return Level;
    }

    public void setLevel(String level) {
        Level = level;
    }

    public String getBU() {
        return BU;
    }

    public void setBU(String BU) {
        this.BU = BU;
    }

    public String getFactory() {
        return Factory;
    }

    public void setFactory(String factory) {
        Factory = factory;
    }

    public String getLine() {
        return Line;
    }

    public void setLine(String line) {
        Line = line;
    }

    public Double getDL1_TTL_Manhour() {
        return DL1_TTL_Manhour;
    }

    public void setDL1_TTL_Manhour(Double DL1_TTL_Manhour) {
        this.DL1_TTL_Manhour = DL1_TTL_Manhour;
    }

    public Double getOutput() {
        return Output;
    }

    public void setOutput(Double output) {
        Output = output;
    }

    public Double getDL2_Variable_Manhour() {
        return DL2_Variable_Manhour;
    }

    public void setDL2_Variable_Manhour(Double DL2_Variable_Manhour) {
        this.DL2_Variable_Manhour = DL2_Variable_Manhour;
    }

    public Double getOffline_DL_fixed_headcount() {
        return Offline_DL_fixed_headcount;
    }

    public void setOffline_DL_fixed_headcount(Double offline_DL_fixed_headcount) {
        Offline_DL_fixed_headcount = offline_DL_fixed_headcount;
    }

    @Override
    public String toString() {
        return "ManualHour{" +
                "Date='" + Date + '\'' +
                ", Group='" + Group + '\'' +
                ", Site='" + Site + '\'' +
                ", Level='" + Level + '\'' +
                ", BU='" + BU + '\'' +
                ", Factory='" + Factory + '\'' +
                ", Line='" + Line + '\'' +
                ", DL1_TTL_Manhour=" + DL1_TTL_Manhour +
                ", Output=" + Output +
                ", DL2_Variable_Manhour=" + DL2_Variable_Manhour +
                ", Offline_DL_fixed_headcount=" + Offline_DL_fixed_headcount +
                '}';
    }
}