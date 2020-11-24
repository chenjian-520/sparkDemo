package com.foxconn.dpm.sprint5.dws_ads.service.issue.category;

import cn.hutool.core.util.NumberUtil;
import com.foxconn.dpm.common.tools.HBaseTools;
import com.foxconn.dpm.core.exception.BusinessException;
import com.foxconn.dpm.enums.IssueCategoryEnum;
import com.foxconn.dpm.enums.TableNameEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.ODSProductionDowntimeBuffer;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDetail;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 * 机种切换处理 .
 *
 * @className: IssueCategorySwitchModelsHandler
 * @author: ws
 * @date: 2020/7/14 15:30
 * @version: 1.0.0
 */
@Slf4j
public class IssueCategorySwitchModelsHandler implements IssueCategoryHandler {
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
        ODSProductionDowntimeBuffer downtimeBuffer = getFirstODSProductionDowntimeBuffer(downtimeDetailDTO);
        if (downtimeBuffer == null) {
            log.warn("ODSProductionDowntimeBuffer 不存在配置, {}", downtimeDetailDTO);
            return false;
        }
        // 单位：秒
        String switchModelsBuffer = downtimeBuffer.getSwitchModelsBuffer();
        if (NumberUtil.sub(Long.toString(downtime / 1000L), switchModelsBuffer).intValue() < 0) {
            return false;
        }
        // 还需要判断两个 sn 的 part_no 不同
        if (downtimeDetailDTO.getPreSnPartNo().equals(downtimeDetailDTO.getNextSnPartNo())) {
            return false;
        }
        downtimeDetailDTO.fillIssueCategory(IssueCategoryEnum.SWITCH_MODELS);

        return true;
    }

    /**
     * 获取第一个 DowntimeBuffer .
     * @param downtimeDetailDTO
     * @author ws
     * @date 2020/7/22 11:44
     * @return com.foxconn.dpm.sprint5.dwd_dws.bean.ODSProductionDowntimeBuffer
     **/
    public ODSProductionDowntimeBuffer getFirstODSProductionDowntimeBuffer(
            AdsProductionDowntimeDetailDTO downtimeDetailDTO) {
        JavaRDD<ODSProductionDowntimeBuffer> downtimeBufferJavaRDD =
                downtimeDetailDTO.getDowntimeBufferJavaRDD();
        // 已经确认 areaCode 不存在的事
        // 条件： siteCode levelCode factoryCode processCode areaCode lineCode
        JavaRDD<ODSProductionDowntimeBuffer> filterJavaRDD = downtimeBufferJavaRDD.filter(item ->
                item.getSiteCode().equals(downtimeDetailDTO.getSiteCode())
                        && item.getLevelCode().equals(downtimeDetailDTO.getLevelCode())
                        && item.getFactoryCode().equals(downtimeDetailDTO.getFactoryCode())
                        && item.getProcessCode().equals(downtimeDetailDTO.getProcessCode())
                        && item.getLineCode().equals(downtimeDetailDTO.getLineCode())
        );
        if (filterJavaRDD.count() == 0) {
            log.warn("ODSProductionDowntimeBuffer 不存在配置, {}", downtimeDetailDTO);
            return null;
        }
        return filterJavaRDD.first();
    }
}
