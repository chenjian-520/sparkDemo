package com.foxconn.dpm.sprint5.dwd_dws.bean;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.util.KeyUtil;
import com.foxconn.dpm.sprint5.dws_ads.bean.AdsProductionDowntimeDetail;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.UUID;

/**
 * Downtime_list .
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
@HBaseTable(level = "dws", table = "dpm_dws_production_line_downtime_list", family = "DPM_DWS_PRODUCTION_LINE_DOWNTIME_LIST")
public class DWSProductionLineDowntimeList extends HBaseBean {

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

    @HBaseColumn(column = "pre_sn")
    private String preSn;

    @HBaseColumn(column = "next_sn")
    private String nextSn;

    @HBaseColumn(column = "pre_sn_partno")
    private String preSnPartNo;

    @HBaseColumn(column = "next_sn_partno")
    private String nextSnPartNo;

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
        return KeyUtil.buildKey(Long.toString(workDtMills), getSiteCode(), getLevelCode()
                , getAreaCode(), getLineCode(), getPreSn(), getLossStartTime());
    }

    /**
     * 转为 ads 类 .
     * @author ws
     * @date 2020/7/6 17:14
     * @return R
     **/
    public AdsProductionDowntimeDetail convert2AdsProductionDowntimeDetail() {
        AdsProductionDowntimeDetail downtimeDetail = new AdsProductionDowntimeDetail();

        BeanUtil.copyProperties(this, downtimeDetail);

        downtimeDetail.setId(getPreSn() + getLossStartTime())
                .setEtlTime(DateUtil.now())
                .setWorkDate(getWorkDt())
                .setErrorStartTime(getLossStartTime())
                .setErrorEndTime(getLossEndTime());
        return downtimeDetail;
    }
}
