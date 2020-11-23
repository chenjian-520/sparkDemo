package com.foxconn.dpm.dws_ads.beans;

/**
 * Description:  com.foxconn.dpm.dws_ads.beans
 * Copyright: © 2019 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2019/12/29
 */
public class ProductionOutput {
    private String SiteCodeID;
    private String LevelCodeID;
    private String PlantCodeID;
    private String ProcessCodeID;
    private String AreaCodeID;
    private String LineCodeID;
    private String MachineID;
    private String PartNo;
    private String Sku;
    private String Plantform;
    private String WorkorderType;
    private String WorkDT;
    private Integer WorkShifitClass;
    private Integer normalized_output_qty;

    public ProductionOutput() {
    }

    public ProductionOutput(String siteCodeID, String levelCodeID, String plantCodeID, String processCodeID, String areaCodeID, String lineCodeID, String machineID, String partNo, String sku, String plantform, String workorderType, String workDT, Integer workShifitClass, Integer normalized_output_qty) {
        SiteCodeID = siteCodeID;
        LevelCodeID = levelCodeID;
        PlantCodeID = plantCodeID;
        ProcessCodeID = processCodeID;
        AreaCodeID = areaCodeID;
        LineCodeID = lineCodeID;
        MachineID = machineID;
        PartNo = partNo;
        Sku = sku;
        Plantform = plantform;
        WorkorderType = workorderType;
        WorkDT = workDT;
        WorkShifitClass = workShifitClass;
        this.normalized_output_qty = normalized_output_qty;
    }

    @Override
    public String toString() {
        return "ProductionOutput{" +
                "SiteCodeID='" + SiteCodeID + '\'' +
                ", LevelCodeID='" + LevelCodeID + '\'' +
                ", PlantCodeID='" + PlantCodeID + '\'' +
                ", ProcessCodeID='" + ProcessCodeID + '\'' +
                ", AreaCodeID='" + AreaCodeID + '\'' +
                ", LineCodeID='" + LineCodeID + '\'' +
                ", MachineID='" + MachineID + '\'' +
                ", PartNo='" + PartNo + '\'' +
                ", Sku='" + Sku + '\'' +
                ", Plantform='" + Plantform + '\'' +
                ", WorkorderType='" + WorkorderType + '\'' +
                ", WorkDT='" + WorkDT + '\'' +
                ", WorkShifitClass=" + WorkShifitClass +
                ", normalized_output_qty=" + normalized_output_qty +
                '}';
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

    public String getPlantCodeID() {
        return PlantCodeID;
    }

    public void setPlantCodeID(String plantCodeID) {
        PlantCodeID = plantCodeID;
    }

    public String getProcessCodeID() {
        return ProcessCodeID;
    }

    public void setProcessCodeID(String processCodeID) {
        ProcessCodeID = processCodeID;
    }

    public String getAreaCodeID() {
        return AreaCodeID;
    }

    public void setAreaCodeID(String areaCodeID) {
        AreaCodeID = areaCodeID;
    }

    public String getLineCodeID() {
        return LineCodeID;
    }

    public void setLineCodeID(String lineCodeID) {
        LineCodeID = lineCodeID;
    }

    public String getMachineID() {
        return MachineID;
    }

    public void setMachineID(String machineID) {
        MachineID = machineID;
    }

    public String getPartNo() {
        return PartNo;
    }

    public void setPartNo(String partNo) {
        PartNo = partNo;
    }

    public String getSku() {
        return Sku;
    }

    public void setSku(String sku) {
        Sku = sku;
    }

    public String getPlantform() {
        return Plantform;
    }

    public void setPlantform(String plantform) {
        Plantform = plantform;
    }

    public String getWorkorderType() {
        return WorkorderType;
    }

    public void setWorkorderType(String workorderType) {
        WorkorderType = workorderType;
    }

    public String getWorkDT() {
        return WorkDT;
    }

    public void setWorkDT(String workDT) {
        WorkDT = workDT;
    }

    public Integer getWorkShifitClass() {
        return WorkShifitClass;
    }

    public void setWorkShifitClass(Integer workShifitClass) {
        WorkShifitClass = workShifitClass;
    }

    public Integer getNormalized_output_qty() {
        return normalized_output_qty;
    }

    public void setNormalized_output_qty(Integer normalized_output_qty) {
        this.normalized_output_qty = normalized_output_qty;
    }
}
