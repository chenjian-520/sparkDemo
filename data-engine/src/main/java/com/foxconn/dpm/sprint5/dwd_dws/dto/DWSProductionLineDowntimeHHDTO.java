package com.foxconn.dpm.sprint5.dwd_dws.dto;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.consts.DateConst;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.tools.DateTools;
import com.foxconn.dpm.common.util.KeyUtil;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.DateRuleBean;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

/**
 * 线体停机小时统计 .
 *
 * @className: DWSProductionLineDowntimeHHDTO
 * @author: ws
 * @date: 2020/7/3 17:34
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "dws", table = "dpm_dws_production_line_downtime_hh", family = "DPM_DWS_PRODUCTION_LINE_DOWNTIME_HH")
public class DWSProductionLineDowntimeHHDTO extends HBaseBean {

    @HBaseColumn(column = "site_code")
    private String siteCode;

    @HBaseColumn(column = "level_code")
    private String levelCode;

    @HBaseColumn(column = "factory_code")
    private String factoryCode;

    @HBaseColumn(column = "process_code")
    private String processCode;

    @HBaseColumn(column = "area_code")
    private String areaCode;

    @HBaseColumn(column = "line_code")
    private String lineCode;

    @HBaseColumn(column = "work_dt")
    private String workDt;

    @HBaseColumn(column = "loss_dt")
    private String lossDt;

    @HBaseColumn(column = "pre_sn")
    private String preSn;

    @HBaseColumn(column = "next_sn")
    private String nextSn;

    @HBaseColumn(column = "pre_sn_partno")
    private String preSnPartNo;

    @HBaseColumn(column = "next_sn_partno")
    private String nextSnPartNo;

    @HBaseColumn(column = "start_time_seq")
    private String startTimeSeq;

    @HBaseColumn(column = "start_time")
    private String startTime;

    @HBaseColumn(column = "end_time")
    private String endTime;

    @HBaseColumn(column = "loss_start_time")
    private String lossStartTime;

    @HBaseColumn(column = "loss_end_time")
    private String lossEndTime;

    @HBaseColumn(column = "down_time")
    private String downTime;

    @HBaseColumn(column = "update_dt")
    private String updateDt;

    @HBaseColumn(column = "update_by")
    private String updateBy;

    @HBaseColumn(column = "data_from")
    private String dataFrom;

    @Override
    public String getBaseRowKey() {
        long workDtMills = DateUtil.parseDate(getWorkDt()).getTime();
        return KeyUtil.buildKey(getSiteCode(), getLevelCode(), getAreaCode()
                , getLineCode(), Long.toString(workDtMills), getPreSn(), getNextSn()
                , getStartTimeSeq());
    }


    /**
     * 用于比较 时间是否存在区间段的特殊字段
     * 真实报警开始时间  2020-07-03 07:48:23
     */
    private long lossStartTimeLong;

    /**
     * 真实报警结束时间  2020-07-03 08:48:23
     */
    private long lossEndTimeLong;

    /**
     * 时间段开始时间 2020-07-03 07:00:00
     */
    private long startTimeLong;

    /**
     * 时间段结束时间 2020-07-03 08:00:00
     */
    private long endTimeLong;

    private boolean startInDate;
    private boolean endInDate;

    /**
     * 分割时间 24 段 .
     * @param times
     * @param index
     * @author ws
     * @date 2020/7/6 14:11
     * @return java.util.List<com.foxconn.dpm.sprint5.dwd_dws.dto.DWSProductionLineDowntimeHHDTO>
     **/
    public List<DWSProductionLineDowntimeHHDTO> splitTime(List<DateRuleBean> times, int index) {
        String pattern = DateConst.YMD_HMS;
        String dayPattern = DateConst.YMD;
        List<DWSProductionLineDowntimeHHDTO> list = Lists.newArrayList();
        DateRuleBean date = times.get(index);
        buildCompareField(date);

        // 报警开始时间不在times时间区间段
        if(!this.isStartInDate()) {
            return list;
        }


        DWSProductionLineDowntimeHHDTO dd = new DWSProductionLineDowntimeHHDTO();
        BeanConvertTools.copyByName(this, dd);
        dd.setStartTime(date.getStartTime());
        dd.setEndTime(date.getEndTime());
        dd.setStartTimeSeq(date.getStartTimeSeq());
        dd.setLossStartTime(this.getLossStartTime());
        dd.setLossDt(DateTools.timeMillisToStr(this.getLossStartTime(), dayPattern));

        // 报警结束时间在times时间区间段
        if(this.isEndInDate()) {
            dd.setLossEndTime(this.getLossEndTime());
            long minutes = (this.getLossEndTimeLong() - this.getLossStartTimeLong()) / 1000;
            dd.setDownTime(String.valueOf(minutes));
            list.add(dd);
            return list;
        }


        // 报警结束时间 超出 时间区间段  a、重置报警结束时间为区间段结束时间
        long minutes = (this.getEndTimeLong() - this.getLossStartTimeLong()) / 1000;
        dd.setDownTime(String.valueOf(minutes));
        dd.setLossEndTime(DateTools.dateFormat(this.getEndTimeLong(), pattern));
        list.add(dd);


        // 报警结束时间 超出 时间区间段  b、重置报警开始时间为上一个时间段结束时间，用下一个times时间段递归
        this.setStartTime(DateTools.dateFormat(this.getEndTimeLong(), pattern));
        index = index >= times.size() ? 0 : index + 1;
        list.addAll(splitTime(times, index));

        return list;
    }


    private void buildCompareField(DateRuleBean date) {
        String pattern = DateConst.YMD_HMS;
        String dayPattern = DateConst.YMD;
        String hourMinutePattern = DateConst.YMD_HM_NO_COLON;

        String dateStartTime = date.getStartTime();
        String lossStartTime = this.getLossStartTime();
        String lossEndTime = this.getLossEndTime();

        this.setLossStartTimeLong(Long.parseLong(lossStartTime));
        this.setLossEndTimeLong(Long.parseLong(lossEndTime));
        this.setStartTimeLong(DateTools.dateStrToLong(DateTools.timeMillisToStr(lossStartTime, dayPattern) + " " + dateStartTime, hourMinutePattern));

        // 由于直接计算出的毫秒数不对。。 所以结束时间 = 开始时间 + （差值毫秒）
        this.setEndTimeLong(getStartTimeLong() + date.getOffsetMinutes() * 60 * 1000);

        // 0700                       0759                             0759                        0800
        boolean startIn = this.getStartTimeLong() <= this.getLossStartTimeLong() && this.getLossStartTimeLong() < this.getEndTimeLong();
        // 0700                       0813                             813                        0800
        boolean endIn = this.getStartTimeLong() < this.getLossEndTimeLong() && this.getLossEndTimeLong() <= this.getEndTimeLong();


        this.setStartInDate(startIn);
        this.setEndInDate(endIn);

    }


    public DWSProductionLineDowntimeHHDTO buildDefaultDWSProductionLineDowntimeHH(DateRuleBean date) {
        DWSProductionLineDowntimeHHDTO dd = new DWSProductionLineDowntimeHHDTO();
        BeanConvertTools.copyByName(this, dd);
        dd.setDownTime(SystemConst.ZERO);
        dd.setLossStartTime(SystemConst.NA);
        dd.setLossEndTime(SystemConst.NA);
        dd.setStartTime(date.getStartTime());
        dd.setEndTime(date.getEndTime());
        dd.setLossDt(SystemConst.NA);
        dd.setStartTimeSeq(date.getStartTimeSeq());
        return dd;
    }

}
