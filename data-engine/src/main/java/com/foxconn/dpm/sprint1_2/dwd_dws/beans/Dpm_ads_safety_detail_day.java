package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class Dpm_ads_safety_detail_day implements Serializable {

    /**
     *id                            唯一id
     *region_code                   区域代碼
     *region_code_desc              区域描述
     *site_code                     廠區代碼
     *site_code_desc                廠區描述
     *building_code                 楼栋代碼
     *building_code_desc            楼栋描述
     *line_code                     线体代碼
     *line_code_desc                线体描述
     *accident_classification       事故類型
     *accident_date                 發生時間
     *accident_location             發生地點
     *accident_owner                所屬單位
     *accident_desc                 事故描述
     *accident_status               目前狀態
     *etltime                       ETLTIME
     */
    private String id;
    private String region_code;
    private String region_code_desc;
    private String site_code;
    private String site_code_desc;
    private String building_code;
    private String building_code_desc;
    private String line_code;
    private String line_code_desc;
    private String accident_classification;
    private String accident_date;
    private String accident_location;
    private String accident_owner;
    private String accident_desc;
    private String accident_status;
    private String etl_time;

}
