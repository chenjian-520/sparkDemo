package com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author HY
 * @Date 2020/6/29 5:16
 * @Description TODO
 */
@Data
public class DateRuleBean  implements Serializable {
    private String startTime;
    private String endTime;
    private int offsetMinutes;
    private String shift;
    private String startTimeSeq;
}
