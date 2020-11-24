package com.foxconn.dpm.sprint5.dws_ads.service.issue.category;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.core.enums.LevelEnum;
import com.foxconn.dpm.enums.IssueCategoryEnum;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDetail;
import com.foxconn.dpm.sprint5.dws_ads.dto.AdsProductionDowntimeDetailDTO;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 交接班处理 .
 *
 * @className: IssueCategoryHandoverHandler
 * @author: ws
 * @date: 2020/7/14 15:30
 * @version: 1.0.0
 */
@Slf4j
public class IssueCategoryHandoverHandler implements IssueCategoryHandler {
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
        String levelCode = downtimeDetailDTO.getLevelCode();
        LevelEnum levelEnum = LevelEnum.getByCode(levelCode);
        if (levelEnum == null) {
            log.warn("根据 levelCode 未找到对应枚举，值为：{}", levelCode);
            return false;
        }
        AtomicBoolean result = new AtomicBoolean();
        LossTimeEnum.listByLevelEnum(levelEnum)
                .forEach(lossTimeEnum -> {
                    boolean compareResult = compareHourMinuteIn(downtimeDetailDTO.getErrorStartTime()
                            , downtimeDetailDTO.getErrorEndTime(), lossTimeEnum.getStartHourMinute()
                            , lossTimeEnum.getEndHourMinute());
                    if (compareResult) {
                        downtimeDetailDTO.fillIssueCategory(IssueCategoryEnum.HANDOVER);
                        result.set(Boolean.TRUE);
                        return;
                    }
                });
        return result.get();
    }

    /**
     * 比较时分是否在某个区间内 .
     * @param startMills
     * @param endMills
     * @param startHourMinute
     * @param endHourMinute
     * @author ws
     * @date 2020/7/14 10:48
     * @return boolean
     **/
    private static boolean compareHourMinuteIn(String startMills, String endMills, String startHourMinute
            , String endHourMinute) {
        DateTime startMillsDateTime = DateUtil.date(Long.parseLong(startMills));
        DateTime endMillsDateTime = DateUtil.date(Long.parseLong(endMills));
        String startMillsYmd = startMillsDateTime.toDateStr();
        String startHourMinuteDateTimeStr = startMillsYmd + SystemConst.SPACE + startHourMinute;
        String endHourMinuteDateTimeStr = startMillsYmd + SystemConst.SPACE + endHourMinute;
        DateTime startHourMinuteDateTime = DateUtil.parseDateTime(startHourMinuteDateTimeStr);
        DateTime endHourMinuteDateTime = DateUtil.parseDateTime(endHourMinuteDateTimeStr);
        return DateUtil.compare(startHourMinuteDateTime, startMillsDateTime) <= 0
                && DateUtil.compare(endHourMinuteDateTime, endMillsDateTime) >= 0;
    }


    /**
     * 停机时间枚举 .
     * @author ws
     * @date 2020/7/14 11:28
     **/
    @Getter
    private enum LossTimeEnum {

        /**
         * L6 区间1
         */
        L6_INTERVAL1(LevelEnum.L6.getCode(), "07:30:00", "07:45:00"),

        /**
         * L6 区间2
         */
        L6_INTERVAL2(LevelEnum.L6.getCode(), "19:30:00", "19:45:00"),

        /**
         * L5 区间1
         */
        L5_INTERVAL1(LevelEnum.L5.getCode(), "08:00:00", "08:15:00"),

        /**
         * L5 区间2
         */
        L5_INTERVAL2(LevelEnum.L5.getCode(), "20:00:00", "20:15:00"),

        /**
         * L10 区间1
         */
        L10_INTERVAL1(LevelEnum.L10.getCode(), "08:00:00", "08:15:00"),

        /**
         * L10 区间2
         */
        L10_INTERVAL2(LevelEnum.L10.getCode(), "20:00:00", "20:15:00"),

        ;

        /**
         * 层级
         */
        private String levelCode;

        /**
         * 开始时分
         */
        private String startHourMinute;

        /**
         * 结束时分
         */
        private String endHourMinute;

        /**
         * 构造方法 .
         * @param levelCode
         * @param startHourMinute
         * @param endHourMinute
         * @author ws
         * @date 2020/7/14 11:28
         * @return
         **/
        LossTimeEnum(String levelCode, String startHourMinute, String endHourMinute) {
            this.levelCode = levelCode;
            this.startHourMinute = startHourMinute;
            this.endHourMinute = endHourMinute;
        }

        /**
         * 根据 levelEnum 获取集合 .
         * @param levelEnum
         * @author ws
         * @date 2020/7/14 11:37
         * @return java.util.List<com.foxconn.dpm.sprint5.dws_ads.service.AdsDowntimeSpringFiveService.LossTimeEnum>
         **/
        public static List<LossTimeEnum> listByLevelEnum(LevelEnum levelEnum) {
            return Arrays.stream(values())
                    .filter(item -> item.getLevelCode().equals(levelEnum.getCode()))
                    .collect(Collectors.toList());
        }
    }
}
