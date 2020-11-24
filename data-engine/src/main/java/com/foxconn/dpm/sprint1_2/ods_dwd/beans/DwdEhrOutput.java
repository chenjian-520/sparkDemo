package com.foxconn.dpm.sprint1_2.ods_dwd.beans;

import lombok.Data;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Description:  com.dl.spark.ehr.dwd
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/2
 */
@Data
public class DwdEhrOutput implements Serializable {
    private String rowkey;
    private String SiteCode;
    private String BG;
    private String SBG;
    private String BU;
    private String MBU;
    private String Department;
    private String SubDepartment;
    private String DepartmentCode;
    private String GroupCode;
    private String LevelCode;
    private String WorkDT;
    private String WorkShifitClass;
    private String HumresourceCode;
    private String onduty_states;
    private String EmpID;
    private String AttendanceWorkhours;
    private String WorkoverTimeHours;
    private String LeaveHours;
    private String processCode;
    private String factoryCode;
    private String InsertDT;
    private String InsertBy;
    private String UpdateDT;
    private String UpdateBy;
    private String DataFrom;

    public DwdEhrOutput() {
        this.rowkey = "";
        SiteCode = "";
        this.BG = "";
        this.SBG = "";
        this.BU = "";
        this.MBU = "";
        Department = "";
        SubDepartment = "";
        DepartmentCode = "";
        GroupCode = "";
        WorkDT = "";
        WorkShifitClass = "";
        HumresourceCode = "";
        EmpID = "";
        InsertDT = "";
        InsertBy = "";
        UpdateDT = "";
        UpdateBy = "";
        DataFrom = "";
        onduty_states = "";
        LevelCode = "";
        processCode = "";
        factoryCode = "";
    }

    @Override
    public String toString() {
        return
                 rowkey + ',' + SiteCode + ',' +
               BG + ',' +
               SBG + ',' +
               BU + ',' +
               MBU + ',' +
               Department + ',' +
              SubDepartment + ',' +
               DepartmentCode + ',' +
                GroupCode + ',' +
                LevelCode + ',' +
                WorkDT + ',' +
                WorkShifitClass + ',' +
                HumresourceCode + ',' +
              onduty_states + ',' +
               EmpID + ',' +
                 AttendanceWorkhours + ',' +
                WorkoverTimeHours + ',' +
                LeaveHours + ',' +
                processCode + ',' +
               factoryCode + ',' +
               InsertDT + ',' +
                InsertBy + ',' +
               UpdateDT + ',' +
                UpdateBy + ',' +
               DataFrom  ;
    }

}
