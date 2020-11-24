package com.foxconn.dpm.sprint5.dws_ads.service.issue.category;

import cn.hutool.core.lang.Singleton;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.foxconn.dpm.enums.IssueCategoryEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.ODSProductionDowntimeBuffer;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDetail;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;
import lombok.extern.slf4j.Slf4j;

/**
 * 未排产处理 .
 *
 * @className: IssueCategoryHandoverHandler
 * @author: ws
 * @date: 2020/7/14 15:30
 * @version: 1.0.0
 */
@Slf4j
public class IssueCategoryUnscheduledHandler implements IssueCategoryHandler {

    /**
     * 最大 downtime 时间
     */
    private static long MAX_DOWNTIME_MILLS = 60L * 60L * 4L * 1000L;

    /**
     * 处理 .
     *
     * @param downtimeDetailDTO
     * @return boolean
     * @author ws
     * @date 2020/7/14 15:29
     **/
    @Override
    public boolean handle(AdsProductionDowntimeDetailDTO downtimeDetailDTO) {
        long downtime = NumberUtil.sub(downtimeDetailDTO.getErrorEndTime()
                , downtimeDetailDTO.getErrorStartTime()).longValue();
        IssueCategorySwitchModelsHandler switchModelsHandler
                = Singleton.get(IssueCategorySwitchModelsHandler.class);
        // 获取 buffer 信息
        ODSProductionDowntimeBuffer downtimeBuffer = switchModelsHandler
                .getFirstODSProductionDowntimeBuffer(downtimeDetailDTO);
        if (downtimeBuffer == null || StrUtil.isBlank(downtimeBuffer.getNotScheduledBuffer())) {
            log.info("未排产处理 未找到 downtimeBuffer 信息：{}", downtimeBuffer);
            return false;
        }
        if (NumberUtil.sub(Long.toString(downtime / 1000L), downtimeBuffer.getNotScheduledBuffer())
                .intValue() >= 0) {
            downtimeDetailDTO.fillIssueCategory(IssueCategoryEnum.UNSCHEDULED);
            return true;
        }
        return false;
    }
}
