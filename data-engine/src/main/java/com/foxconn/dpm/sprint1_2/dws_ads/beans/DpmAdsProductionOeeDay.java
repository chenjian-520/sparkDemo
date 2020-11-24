package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @author HS
 * @className DpmAdsProductionOeeDay
 * @description TODO
 * @date 2020/4/22 14:13
 */
@Data
public class DpmAdsProductionOeeDay {
    String id;
    String work_date;
    String workshift_code;
    String site_code;
    String level_code;
    String factory_code;
    String process_code;
    String area_code;
    String line_code;
    String machine_id;

    Float oee1_actual;
    Float oee2_actual;
    Float oee2_target;
    String etltime;

    Float loading_time;
    Float npi_time;
    Float breakdown_time;
    Float die_failure_time;
    Float short_halt_time;
    Float test_equipment_time;
    Float co_time;
    Float stamping_change_material;
    Float molding_clean;
    Float incoming_material_adverse;
    Float wait_material;
    Float energy_error;
    Float equipment_error;
    Float labour_error;
    Float other_error;
    Float output_hours;
    Float operation_time;
    Float good_product_qty;
    Float actual_output_qty;
    Float total_work_time;
    Float total_worktime_oee1;
    Float total_worktime_oee2;

    public DpmAdsProductionOeeDay() {
    }

    public DpmAdsProductionOeeDay(String id, String work_date, String workshift_code, String site_code, String level_code, String factory_code, String process_code, String area_code, String line_code, String machine_id, Float oee1_actual, Float oee2_actual, Float oee2_target, String etltime,Float loading_time,Float npi_time,Float breakdown_time,Float die_failure_time,Float short_halt_time,Float test_equipment_time,Float co_time,Float stamping_change_material,Float molding_clean,Float incoming_material_adverse,Float wait_material,Float energy_error,Float equipment_error,Float labour_error,Float other_error,Float output_hours,Float operation_time,Float good_product_qty,Float actual_output_qty,Float total_worktime_oee1, Float total_worktime_oee2) {
        this.id = id;
        this.work_date = work_date;
        this.workshift_code = workshift_code;
        this.site_code = site_code;
        this.level_code = level_code;
        this.factory_code = factory_code;
        this.process_code = process_code;
        this.area_code = area_code;
        this.line_code = line_code;
        this.machine_id = machine_id;
        this.oee1_actual = oee1_actual;
        this.oee2_actual = oee2_actual;
        this.oee2_target = oee2_target;
        this.etltime = etltime;
        this.loading_time = loading_time;
        this.npi_time = npi_time;
        this.breakdown_time = breakdown_time;
        this.die_failure_time = die_failure_time;
        this.short_halt_time = short_halt_time;
        this.test_equipment_time = test_equipment_time;
        this.co_time = co_time;
        this.stamping_change_material = stamping_change_material;
        this.molding_clean = molding_clean;
        this.incoming_material_adverse = incoming_material_adverse;
        this.wait_material = wait_material;
        this.energy_error = energy_error;
        this.equipment_error = equipment_error;
        this.labour_error = labour_error;
        this.other_error = other_error;
        this.output_hours = output_hours;
        this.operation_time = operation_time;
        this.good_product_qty = good_product_qty;
        this.actual_output_qty = actual_output_qty;
        this.total_worktime_oee1 = total_worktime_oee1;
        this.total_worktime_oee2 = total_worktime_oee2;
    }

}
