package com.foxconn.dpm.sprint5.dws_ads.service.issue.category;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDetail;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * IssueCategory 处理器 .
 *
 * @className: IssueCategoryHandler
 * @author: ws
 * @date: 2020/7/14 15:20
 * @version: 1.0.0
 */
public interface IssueCategoryHandler extends Serializable {

    /**
     * 处理 .
     * @param downtimeDetailDTO
     * @author ws
     * @date 2020/7/14 15:29
     * @return boolean
     **/
    boolean handle(AdsProductionDowntimeDetailDTO downtimeDetailDTO);


    /**
     * 构建 workDate .
     * @param downtimeDetail
     * @author ws
     * @date 2020/7/14 17:19
     * @return java.util.Map
     **/
    /*default Map<String, Object> buildWorkdateMap(AdsProductionDowntimeDetail downtimeDetail) {
        Map<String, Object> parameterMap = new HashMap<>();
//        String workDateStr = downtimeDetail.getWorkDate();
        String workDateStr = "2020-06-09";
        parameterMap.put(SystemConst.KEY_WORKDATE, workDateStr);
        return parameterMap;
    }*/

}
