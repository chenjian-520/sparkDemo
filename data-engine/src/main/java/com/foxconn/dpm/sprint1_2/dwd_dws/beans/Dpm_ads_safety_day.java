package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class Dpm_ads_safety_day implements Serializable {
    /**
     * id	            		唯一id              String
     * region_code			    区域代碼
     * region_code_desc			区域描述
     * site_code			    廠區代碼
     * site_code_desc			廠區描述
     * building_code			楼栋代碼
     * building_code_desc		楼栋描述
     * line_code			    线体代碼
     * line_code_desc			线体描述
     * work_date			    工作日
     * safety_actual			实际廠區安全事件件数   int
     * safety_target			目标廠區安全事件件数   int
     * etl_time			        資料抄寫時間
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
    private String work_date;
    private int safety_actual;
    private int safety_target;
    private String etl_time;

}
