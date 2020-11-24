package com.foxconn.dpm.sprint5.dws_ads.service.issue.category;

import cn.hutool.core.lang.Singleton;
import cn.hutool.core.util.StrUtil;
import com.foxconn.dpm.core.exception.BusinessException;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDetail;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * IssueCategory 管理器 .
 *
 * @className: IssueCategoryManagemer
 * @author: ws
 * @date: 2020/7/14 15:19
 * @version: 1.0.0
 */
@Slf4j
public class IssueCategoryManagemer {

    /**
     * 处理器
     */
    private static final List<IssueCategoryHandler> HANDLERS = Lists.newArrayList();

    /**
     * 构造方法 .
     * @author ws
     * @date 2020/7/14 15:37
     * @return
     **/
    private IssueCategoryManagemer() {
        HANDLERS.addAll(Lists.newArrayList(Singleton.get(IssueCategoryHandoverHandler.class)
                , Singleton.get(IssueCategoryMachineBreakdownHandler.class)
                , Singleton.get(IssueCategorySwitchModelsHandler.class)
                , Singleton.get(IssueCategoryMaintenanceHandler.class)
                , Singleton.get(IssueCategoryUnscheduledHandler.class)));
    }

    /**
     * 填充 IssueCategory .
     * @param downtimeDetailDTO
     * @author ws
     * @date 2020/7/14 15:40
     * @return boolean
     **/
    public boolean fillIssueCategory(AdsProductionDowntimeDetailDTO downtimeDetailDTO) {
        boolean result = false;
        for (int i = 0; i < HANDLERS.size(); i++) {
            IssueCategoryHandler handler = HANDLERS.get(i);
            boolean handleResult = handler.handle(downtimeDetailDTO);
            if (handleResult) {
                result = Boolean.TRUE;
                break;
            }
        }
        return result;
    }

    /**
     * 获取实例 .
     * @author ws
     * @date 2020/7/14 15:38
     * @return com.foxconn.dpm.sprint5.dws_ads.service.issue.category.IssueCategoryManagemer
     **/
    public static IssueCategoryManagemer getInstance() {
        return Singleton.get(IssueCategoryManagemer.class);
    }

}
