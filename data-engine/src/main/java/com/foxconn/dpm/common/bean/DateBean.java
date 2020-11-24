package com.foxconn.dpm.common.bean;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * @Author HY
 * @Date 2020/6/28 17:53
 * @Description TODO
 */
@Data
@Accessors(chain = true)
public class DateBean  implements Serializable {
    private String startDate;
    private String endDate;
    private String startTime;
    private String endTime;


}
