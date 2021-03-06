package com.foxconn.dpm.sprint5.dws_ads.bean;

import com.foxconn.dpm.common.annotation.MySqlColumn;
import com.foxconn.dpm.common.annotation.MySqlTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.enums.MySqlDataTypes;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 生产downtime .
 *
 * @className: AdsProductionDowntimeDay
 * @author: ws
 * @date: 2020/7/6 14:51
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@MySqlTable(table = "dpm_ads_production_downtime_day")
public class AdsProductionDowntimeDay implements Serializable {

    @MySqlColumn(column = "id", dataType = MySqlDataTypes.STRING, primaryKey = true)
    private String id;

    @MySqlColumn(column = "region_code", dataType = MySqlDataTypes.STRING, insert = false)
    private String regionCode;

    @MySqlColumn(column = "region_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String regionCodeDesc;

    @MySqlColumn(column = "site_code", dataType = MySqlDataTypes.STRING)
    private String siteCode;

    @MySqlColumn(column = "site_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String siteCodeDesc;

    @MySqlColumn(column = "buildingCode", dataType = MySqlDataTypes.STRING, insert = false)
    private String buildingCode;

    @MySqlColumn(column = "building_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String buildingCodeDesc;

    @MySqlColumn(column = "block_code", dataType = MySqlDataTypes.STRING, insert = false)
    private String blockCode;

    @MySqlColumn(column = "block_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String blockCodeDesc;

    @MySqlColumn(column = "line_code", dataType = MySqlDataTypes.STRING)
    private String lineCode;

    @MySqlColumn(column = "line_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String lineCodeDesc;

    @MySqlColumn(column = "machine_id", dataType = MySqlDataTypes.STRING, insert = false)
    private String machineId;

    @MySqlColumn(column = "level_code", dataType = MySqlDataTypes.STRING)
    private String levelCode;

    @MySqlColumn(column = "level_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String levelCodeDesc;

    @MySqlColumn(column = "factory_code", dataType = MySqlDataTypes.STRING)
    private String factoryCode;

    @MySqlColumn(column = "factory_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String factoryCodeDesc;

    @MySqlColumn(column = "process_code", dataType = MySqlDataTypes.STRING)
    private String processCode;

    @MySqlColumn(column = "process_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String processCodeDesc;

    @MySqlColumn(column = "customer_code", dataType = MySqlDataTypes.STRING)
    private String customerCode;

    @MySqlColumn(column = "customer_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String customerCodeDesc;

    @MySqlColumn(column = "product_type_group_code", dataType = MySqlDataTypes.STRING, insert = false)
    private String productTypeGroupCode;

    @MySqlColumn(column = "product_type_group_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String productTypeGroupDesc;

    @MySqlColumn(column = "product_type_code", dataType = MySqlDataTypes.STRING, insert = false)
    private String productTypeCode;

    @MySqlColumn(column = "product_type_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String productTypeDesc;

    @MySqlColumn(column = "plant_code", dataType = MySqlDataTypes.STRING, insert = false)
    private String plantCode;

    @MySqlColumn(column = "plant_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String plantCodeDesc;

    @MySqlColumn(column = "platform", dataType = MySqlDataTypes.STRING, insert = false)
    private String platform;

    @MySqlColumn(column = "sku", dataType = MySqlDataTypes.STRING, insert = false)
    private String sku;

    @MySqlColumn(column = "part_no", dataType = MySqlDataTypes.STRING, insert = false)
    private String partNo;

    @MySqlColumn(column = "work_date", dataType = MySqlDataTypes.STRING)
    private String workDate;

    @MySqlColumn(column = "workshift_code", dataType = MySqlDataTypes.STRING, insert = false)
    private String workshiftCode;

    @MySqlColumn(column = "workshift_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String workshiftCodeDesc;

    @MySqlColumn(column = "section_id", dataType = MySqlDataTypes.STRING)
    private String sectionId;

    @MySqlColumn(column = "section_desc", dataType = MySqlDataTypes.STRING)
    private String sectionDesc;

    @MySqlColumn(column = "cust_section_id", dataType = MySqlDataTypes.STRING, insert = false)
    private String custSectionId;

    @MySqlColumn(column = "cust_section_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String custSectionDesc;

    @MySqlColumn(column = "downtime_actual", dataType = MySqlDataTypes.INTEGER)
    private Integer downtimeActual;

    @MySqlColumn(column = "downtime_target", dataType = MySqlDataTypes.INTEGER, insert = false)
    private Integer downtimeTarget;

    @MySqlColumn(column = "etl_time", dataType = MySqlDataTypes.STRING)
    private String etlTime;

}
