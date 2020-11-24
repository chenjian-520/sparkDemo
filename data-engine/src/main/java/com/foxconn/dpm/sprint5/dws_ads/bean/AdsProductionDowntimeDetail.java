package com.foxconn.dpm.sprint5.dws_ads.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
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
 * downtime明细资料 .
 *
 * @className: AdsProductionDowntimeDetail
 * @author: ws
 * @date: 2020/7/6 15:02
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@MySqlTable(table = "dpm_ads_production_downtime_detail")
public class AdsProductionDowntimeDetail implements Serializable {

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

    @MySqlColumn(column = "section_id", dataType = MySqlDataTypes.STRING, insert = false)
    private String sectionId;

    @MySqlColumn(column = "section_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String sectionDesc;

    @MySqlColumn(column = "cust_section_id", dataType = MySqlDataTypes.STRING, insert = false)
    private String custSectionId;

    @MySqlColumn(column = "cust_section_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String custSectionDesc;

    @MySqlColumn(column = "error_code", dataType = MySqlDataTypes.STRING, insert = false)
    private String errorCode;

    @MySqlColumn(column = "error_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String errorDesc;

    @MySqlColumn(column = "dt_no", dataType = MySqlDataTypes.STRING)
    private String dtNo;

    @MySqlColumn(column = "issue_category_code", dataType = MySqlDataTypes.STRING)
    private String issueCategoryCode;

    @MySqlColumn(column = "issue_category_code_desc", dataType = MySqlDataTypes.STRING)
    private String issueCategoryCodeDesc;

    @MySqlColumn(column = "error_start_time", dataType = MySqlDataTypes.STRING)
    private String errorStartTime;

    @MySqlColumn(column = "error_end_time", dataType = MySqlDataTypes.STRING)
    private String errorEndTime;

    @MySqlColumn(column = "primary_person", dataType = MySqlDataTypes.STRING, insert = false)
    private String primaryPerson;

    @MySqlColumn(column = "current_status", dataType = MySqlDataTypes.STRING)
    private String currentStatus;

    @MySqlColumn(column = "etl_time", dataType = MySqlDataTypes.STRING)
    private String etlTime;

    @MySqlColumn(column = "pre_sn", dataType = MySqlDataTypes.STRING)
    private String preSn;

    @MySqlColumn(column = "next_sn", dataType = MySqlDataTypes.STRING)
    private String nextSn;

    @MySqlColumn(column = "pre_sn_partno", dataType = MySqlDataTypes.STRING)
    private String preSnPartNo;

    @MySqlColumn(column = "next_sn_partno", dataType = MySqlDataTypes.STRING)
    private String nextSnPartNo;

}
