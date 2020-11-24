package com.foxconn.dpm.sprint1_2.dws_ads.beans;

import lombok.Data;

/**
 * @author HS
 * @className DpmDwsProductionOeeEquipmentLineDD
 * @description TODO
 * @date 2020/4/22 13:41
 */
@Data
public class DpmDwsProductionOeeEquipmentLineDD {

    String work_dt;
    String work_shift;
    String site_code;
    String level_code;
    String factory_code;
    String process_code;
    String area_code;
    String line_code;
    String machine_id;
    String equipment_total_qty;
    String total_work_time;
    String equipment_work_qty;
    String plan_noproduction_time;
    String equipment_maintenance;
    String official_holiday;
    String plan_downtime_total;
    String npi_time;
    String breakdown_time;
    String die_failure_time;
    String short_halt_time;
    String test_equipment_time;
    String co_time;
    String stamping_change_material;
    String molding_clean;
    String incoming_material_adverse;
    String wait_material;
    String energy_error;
    String equipment_error;
    String labour_error;
    String other_error;
    String noplan_down_time;
    String loss_time;
    String operation_time;
    String plan_output_qty;
    Float actual_output_qty;
    String normalized_output_qty;
    Float good_product_qty;
    String output_hours;
    Float time_efficiency_rate;
    Float performance_efficiency_rate;
    Float yield_rate;
    String oee1_actual;
    String loading_time;
    String rate;
    String npi_time_per;
    String breakdown_time_per;
    String die_failure_time_per;
    String short_halt_time_per;
    String test_equipment_time_per;
    String co_time_per;
    String stamping_change_material_per;
    String molding_clean_per;
    String incoming_material_adverse_per;
    String wait_material_per;
    String energy_error_per;
    String equipment_error_per;
    String labour_error_per;
    String other_error_per;
    String time_efficiency_rate2;
    Float performance_efficiency_rate2;
    Float bad_losses;
    String oee2;
    String data_granularity;
    Long update_dt;
    String update_by;
    String data_from;

    public Float getYield_rate() {
        return yield_rate == null ? 0.0f : yield_rate;
    }

    public Float getTime_efficiency_rate() {
        return time_efficiency_rate == null ? 0.0f : time_efficiency_rate;
    }

    public Float getPerformance_efficiency_rate() {
        return performance_efficiency_rate == null ? 0.0f : performance_efficiency_rate;
    }

    public Float getActual_output_qty() {
        return actual_output_qty == null ? 0.0f : actual_output_qty;
    }

    public Float getGood_product_qty() {
        return good_product_qty == null ? 0.0f : good_product_qty;
    }


    public DpmDwsProductionOeeEquipmentLineDD add(DpmDwsProductionOeeEquipmentLineDD o2) {

        this.loading_time = (addValueOf(this.loading_time, o2.loading_time));
        this.npi_time = (addValueOf(this.npi_time, o2.npi_time));
        this.breakdown_time = (addValueOf(this.breakdown_time, o2.breakdown_time));
        this.die_failure_time = (addValueOf(this.die_failure_time, o2.die_failure_time));
        this.short_halt_time = (addValueOf(this.short_halt_time, o2.short_halt_time));
        this.test_equipment_time = (addValueOf(this.test_equipment_time, o2.test_equipment_time));
        this.co_time = (addValueOf(this.co_time, o2.co_time));
        this.stamping_change_material = (addValueOf(this.stamping_change_material, o2.stamping_change_material));
        this.molding_clean = (addValueOf(this.molding_clean, o2.molding_clean));
        this.incoming_material_adverse = (addValueOf(this.incoming_material_adverse, o2.incoming_material_adverse));
        this.wait_material = (addValueOf(this.wait_material, o2.wait_material));
        this.energy_error = (addValueOf(this.energy_error, o2.energy_error));
        this.equipment_error = (addValueOf(this.equipment_error, o2.equipment_error));
        this.labour_error = (addValueOf(this.labour_error, o2.labour_error));
        this.other_error = (addValueOf(this.other_error, o2.other_error));
        this.output_hours = (addValueOf(this.output_hours, o2.output_hours));
        this.operation_time = (addValueOf(this.operation_time, o2.operation_time));
        this.good_product_qty = addValueOf(this.good_product_qty, o2.good_product_qty);
        this.actual_output_qty = addValueOf(this.actual_output_qty, o2.actual_output_qty);
        this.total_work_time = (addValueOf(this.total_work_time, o2.total_work_time));
        return this;
    }

    public String addValueOf(String v1, String v2) {
        try {
            if (v1.contains("%")) {
                v1 = v1.replace("%", "");
                v1 = String.valueOf(Float.valueOf(v1) / 100);

            }
            if (v2.contains("%")) {
                v2 = v2.replace("%", "");
                v2 = String.valueOf(Float.valueOf(v2) / 100);
            }

            return String.valueOf(Float.valueOf(v1.replace(",", "")) + Float.valueOf(v2.replace(",", "")));

        } catch (Exception e) {
            return "0";
        }
    }

    public Float addValueOf(Float v1, Float v2) {
        Float filtedV1 = v1 == null ? 0 : v1;
        Float filtedV2 = v2 == null ? 0 : v2;

        return filtedV1 + filtedV2;

    }

}
