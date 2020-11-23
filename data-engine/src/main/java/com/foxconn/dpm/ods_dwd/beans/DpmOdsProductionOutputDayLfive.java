package com.foxconn.dpm.ods_dwd.beans;

/**
 * @author HS
 * @className DpmOdsProductionOutputDayLfive
 * @description TODO
 * @date 2019/12/27 13:34
 */
public class DpmOdsProductionOutputDayLfive {

    String BUCode;
    String AreaCode;
    String ShiftCode;
    String LineCode;
    String WorkDate;
    String WO;
    String WOType;
    String PartNo;
    String Customer;
    String ModelNo;
    String SN;
    String StationCode;
    String StationName;
    Integer IsPass;
    Integer IsFail;
    String ScanBy;
    String ScanDT;
    Long InsertDT;
    String InsertBy;
    Long UpdateDT;
    String UpdateBy;
    String DataFrom;

    public DpmOdsProductionOutputDayLfive() {
    }

    public DpmOdsProductionOutputDayLfive(String BUCode, String areaCode, String shiftCode, String lineCode, String workDate, String WO, String WOType, String partNo, String customer, String modelNo, String SN, String stationCode, String stationName, Integer isPass, Integer isFail, String scanBy, String scanDT, Long insertDT, String insertBy, Long updateDT, String updateBy, String dataFrom) {
        this.BUCode = BUCode;
        AreaCode = areaCode;
        ShiftCode = shiftCode;
        LineCode = lineCode;
        WorkDate = workDate;
        this.WO = WO;
        this.WOType = WOType;
        PartNo = partNo;
        Customer = customer;
        ModelNo = modelNo;
        this.SN = SN;
        StationCode = stationCode;
        StationName = stationName;
        IsPass = isPass;
        IsFail = isFail;
        ScanBy = scanBy;
        ScanDT = scanDT;
        InsertDT = insertDT;
        InsertBy = insertBy;
        UpdateDT = updateDT;
        UpdateBy = updateBy;
        DataFrom = dataFrom;
    }

    public String getBUCode() {
        return BUCode;
    }

    public void setBUCode(String BUCode) {
        this.BUCode = BUCode;
    }

    public String getAreaCode() {
        return AreaCode;
    }

    public void setAreaCode(String areaCode) {
        AreaCode = areaCode;
    }

    public String getShiftCode() {
        return ShiftCode;
    }

    public void setShiftCode(String shiftCode) {
        ShiftCode = shiftCode;
    }

    public String getLineCode() {
        return LineCode;
    }

    public void setLineCode(String lineCode) {
        LineCode = lineCode;
    }

    public String getWorkDate() {
        return WorkDate;
    }

    public void setWorkDate(String workDate) {
        WorkDate = workDate;
    }

    public String getWO() {
        return WO;
    }

    public void setWO(String WO) {
        this.WO = WO;
    }

    public String getWOType() {
        return WOType;
    }

    public void setWOType(String WOType) {
        this.WOType = WOType;
    }

    public String getPartNo() {
        return PartNo;
    }

    public void setPartNo(String partNo) {
        PartNo = partNo;
    }

    public String getCustomer() {
        return Customer;
    }

    public void setCustomer(String customer) {
        Customer = customer;
    }

    public String getModelNo() {
        return ModelNo;
    }

    public void setModelNo(String modelNo) {
        ModelNo = modelNo;
    }

    public String getSN() {
        return SN;
    }

    public void setSN(String SN) {
        this.SN = SN;
    }

    public String getStationCode() {
        return StationCode;
    }

    public void setStationCode(String stationCode) {
        StationCode = stationCode;
    }

    public String getStationName() {
        return StationName;
    }

    public void setStationName(String stationName) {
        StationName = stationName;
    }

    public Integer getIsPass() {
        return IsPass;
    }

    public void setIsPass(Integer isPass) {
        IsPass = isPass;
    }

    public Integer getIsFail() {
        return IsFail;
    }

    public void setIsFail(Integer isFail) {
        IsFail = isFail;
    }

    public String getScanBy() {
        return ScanBy;
    }

    public void setScanBy(String scanBy) {
        ScanBy = scanBy;
    }

    public String getScanDT() {
        return ScanDT;
    }

    public void setScanDT(String scanDT) {
        ScanDT = scanDT;
    }

    public Long getInsertDT() {
        return InsertDT;
    }

    public void setInsertDT(Long insertDT) {
        InsertDT = insertDT;
    }

    public String getInsertBy() {
        return InsertBy;
    }

    public void setInsertBy(String insertBy) {
        InsertBy = insertBy;
    }

    public Long getUpdateDT() {
        return UpdateDT;
    }

    public void setUpdateDT(Long updateDT) {
        UpdateDT = updateDT;
    }

    public String getUpdateBy() {
        return UpdateBy;
    }

    public void setUpdateBy(String updateBy) {
        UpdateBy = updateBy;
    }

    public String getDataFrom() {
        return DataFrom;
    }

    public void setDataFrom(String dataFrom) {
        DataFrom = dataFrom;
    }
}
