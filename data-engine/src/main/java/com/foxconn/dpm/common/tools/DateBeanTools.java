package com.foxconn.dpm.common.tools;

import com.foxconn.dpm.common.bean.DateBean;
import com.foxconn.dpm.util.batchData.BatchGetter;

import java.util.Map;

/**
 * @Author HY
 * @Date 2020/6/28 17:54
 * @Description TODO
 */
public class DateBeanTools {

    private DateBeanTools(){}
    private static final String WORK_DATE = "workDate";
    private static final String START_DATE = "startDate";
    private static final String END_DATE = "endDate";
    private static final String TODAY = "today";


    public static DateBean buildDateBean(Map<String, Object> map) {

        BatchGetter batchGetter = BatchGetter.getInstance();
        //获取传入的时间
        String startDate;
        String endDate;
        String startTime;
        String endTime;

        //初始化时间
        if (map.get(WORK_DATE) != null) {
            startDate = map.get(WORK_DATE).toString();
            endDate = batchGetter.getStDateDayStrAdd(startDate, 1, "-");
            startTime = String.valueOf(batchGetter.formatTimestampMilis(startDate, "yyyy-MM-dd"));
            endTime = String.valueOf(batchGetter.formatTimestampMilis(endDate, "yyyy-MM-dd"));
        }else if(map.get(TODAY) != null) {
            startDate = batchGetter.getStDateDayAdd(0);
            endDate = batchGetter.getStDateDayAdd(1);
            startTime = batchGetter.getStDateDayStampAdd(0);
            endTime = batchGetter.getStDateDayStampAdd(1);
        }else if(map.get(START_DATE) != null && map.get(END_DATE) != null){
            startDate = map.get(START_DATE).toString();
            endDate = map.get(END_DATE).toString();
            startTime = String.valueOf(batchGetter.formatTimestampMilis(startDate, "yyyy-MM-dd"));
            endTime = String.valueOf(batchGetter.formatTimestampMilis(endDate, "yyyy-MM-dd"));
        }else {
            startDate = batchGetter.getStDateDayAdd(-1);
            endDate = batchGetter.getStDateDayAdd(0);
            startTime = batchGetter.getStDateDayStampAdd(-1);
            endTime = batchGetter.getStDateDayStampAdd(0);
        }

        return new DateBean()
                .setStartDate(startDate)
                .setEndDate(endDate)
                .setStartTime(startTime)
                .setEndTime(endTime);
    }


}
