package com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_machine_maintenance;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @Author HY
 * @Date 2020/6/29 4:50
 * @Description TODO
 */
@Data
public class GroupRuleBean  implements Serializable {

    private String siteCode;
    private String levelCode;
    private List<DateRuleBean> times;

}
