package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

import java.io.Serializable;

@Data
public class ProductionEquipmentDay implements Serializable {
    private String line_code;
    private String work_dt;
    private String ttl_time;
    private String planned_downtime_loss_hours;
    private String status;
    private String sfc_line_code;
    private String update_dt;
    private String update_by;
    private String data_from;

    @Override
    public String toString() {
        return "productionEquipmentDay{" +
                "line_code='" + line_code + '\'' +
                ", work_dt='" + work_dt + '\'' +
                ", ttl_time='" + ttl_time + '\'' +
                ", planned_downtime_loss_hours='" + planned_downtime_loss_hours + '\'' +
                ", status='" + status + '\'' +
                ", sfc_line_code='" + sfc_line_code + '\'' +
                ", update_dt='" + update_dt + '\'' +
                ", update_by='" + update_by + '\'' +
                ", data_from='" + data_from + '\'' +
                '}';
    }

    public ProductionEquipmentDay() {
    }

    public ProductionEquipmentDay(String line_code, String work_dt, String ttl_time, String planned_downtime_loss_hours, String status, String sfc_line_code, String update_dt, String update_by, String data_from) {
        this.line_code = line_code;
        this.work_dt = work_dt;
        this.ttl_time = ttl_time;
        this.planned_downtime_loss_hours = planned_downtime_loss_hours;
        this.status = status;
        this.sfc_line_code = sfc_line_code;
        this.update_dt = update_dt;
        this.update_by = update_by;
        this.data_from = data_from;
    }
}
