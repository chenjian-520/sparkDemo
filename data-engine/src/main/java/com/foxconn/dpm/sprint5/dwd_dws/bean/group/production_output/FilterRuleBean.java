package com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author HY
 * @Date 2020/7/20 10:42
 * @Description TODO
 */
@Data
public class FilterRuleBean    implements Serializable {
    private String siteCode;
    private String levelCode;
    private String customer;
    private String newStationCode;
    private String oldStationCode;
    private String fail;
}
