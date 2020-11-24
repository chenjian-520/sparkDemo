package com.foxconn.dpm.sprint5.dws_ads.controller;

import com.foxconn.dpm.core.base.BaseController;
import com.foxconn.dpm.core.enums.BusinessTypeEnum;

/**
 * 线体停机小时统计 .
 *
 * @className: AdsDowntimeSpringFiveController
 * @author: ws
 * @date: 2020/7/2 11:24
 * @version: 1.0.0
 */
public class AdsDowntimeSpringFiveController extends BaseController {

    /**
     * 绑定业务类型 .
     *
     * @return com.foxconn.dpm.core.enums.BusinessTypeEnum
     * @author ws
     * @date 2020/7/2 14:08
     **/
    @Override
    protected BusinessTypeEnum bindBusinessType() {
        return BusinessTypeEnum.ADS_DOWNTIME_SPRING_FIVE;
    }
}
