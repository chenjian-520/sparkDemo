package com.foxconn.dpm.sprint5.dwd_dws.controller;

import com.foxconn.dpm.core.base.BaseController;
import com.foxconn.dpm.core.enums.BusinessTypeEnum;

/**
 * dws 线体停机小时统计 .
 *
 * @className: DwsDowntimeSpringFiveController
 * @author: ws
 * @date: 2020/7/3 9:23
 * @version: 1.0.0
 */
public class DwsDowntimeSpringFiveController extends BaseController {

    /**
     * 绑定业务类型 .
     *
     * @return com.foxconn.dpm.core.enums.BusinessTypeEnum
     * @author ws
     * @date 2020/7/2 14:08
     **/
    @Override
    protected BusinessTypeEnum bindBusinessType() {
        return BusinessTypeEnum.DWS_DOWNTIME_SPRING_FIVE;
    }
}
