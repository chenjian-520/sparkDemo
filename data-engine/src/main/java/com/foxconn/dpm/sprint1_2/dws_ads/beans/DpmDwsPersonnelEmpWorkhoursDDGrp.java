package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @author HS
 * @className DpmDwsPersonnelEmpWorkhoursDDGrp
 * @description TODO
 * @date 2020/4/21 17:38
 */
@Data
public class DpmDwsPersonnelEmpWorkhoursDDGrp {

    String work_dt;
    String site_code;
    String level_code;
    String work_shift;
    Float DL1;
    Float DL2V;
    Integer DL2F;
    Integer IDL;

    public DpmDwsPersonnelEmpWorkhoursDDGrp() {
    }

    public DpmDwsPersonnelEmpWorkhoursDDGrp(String work_dt, String site_code, String level_code, String work_shift, Float DL1, Float DL2V, Integer DL2F, Integer IDL) {
        this.work_dt = work_dt;
        this.site_code = site_code;
        this.level_code = level_code;
        this.work_shift = work_shift;
        this.DL1 = DL1;
        this.DL2V = DL2V;
        this.DL2F = DL2F;
        this.IDL = IDL;
    }
}
