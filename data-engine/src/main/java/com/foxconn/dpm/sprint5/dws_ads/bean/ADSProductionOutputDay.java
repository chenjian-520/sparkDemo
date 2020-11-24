package com.foxconn.dpm.sprint5.dws_ads.bean;

import com.foxconn.dpm.common.annotation.MySqlColumn;
import com.foxconn.dpm.common.annotation.MySqlTable;
import com.foxconn.dpm.common.enums.MySqlDataTypes;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * @Author HY
 * @Date 2020/7/1 11:32
 * @Description TODO
 */
@Data
@Accessors(chain = true)
@MySqlTable(table = "dpm_ads_production_output_day")
public class ADSProductionOutputDay implements Serializable {

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

    @MySqlColumn(column = "building_code", dataType = MySqlDataTypes.STRING, insert = false)
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

    @MySqlColumn(column = "part_no", dataType = MySqlDataTypes.STRING)
    private String partNo;

    @MySqlColumn(column = "work_date", dataType = MySqlDataTypes.STRING)
    private String workDate;

    @MySqlColumn(column = "workshift_code", dataType = MySqlDataTypes.STRING)
    private String workshiftCode;

    @MySqlColumn(column = "workshift_code_desc", dataType = MySqlDataTypes.STRING, insert = false)
    private String workshiftCodeDesc;

    @MySqlColumn(column = "section_id", dataType = MySqlDataTypes.STRING)
    private String sectionId;

    @MySqlColumn(column = "section_desc", dataType = MySqlDataTypes.STRING)
    private String sectionDesc;

    @MySqlColumn(column = "station_code", dataType = MySqlDataTypes.STRING)
    private String stationCode;

    @MySqlColumn(column = "output_qty_actual", dataType = MySqlDataTypes.INTEGER)
    private Integer outputQtyActual;

    @MySqlColumn(column = "output_qty_target", dataType = MySqlDataTypes.INTEGER)
    private Integer outputQtyTarget;

    @MySqlColumn(column = "gap_qty", dataType = MySqlDataTypes.INTEGER)
    private Integer gapQty;

    @MySqlColumn(column = "achievement_rate", dataType = MySqlDataTypes.DOUBLE)
    private Double achievementRate;

    @MySqlColumn(column = "etl_time", dataType = MySqlDataTypes.STRING)
    private String etlTime;


}
