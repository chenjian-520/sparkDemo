package com.foxconn.dpm.sprint5.dws_ads.bean;

import cn.hutool.crypto.digest.MD5;
import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.tools.DateTools;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author HY
 * @Date 2020/6/28 17:30
 * @Description
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "dws", table = "dpm_dws_production_line_maintenance_hh", family = "DPM_DWS_PRODUCTION_LINE_MAINTENANCE_HH")
public class DWSProductionMachineMaintenanceHH extends HBaseBean {

    @HBaseColumn( column = "site_code")
    private String siteCode;

    @HBaseColumn( column = "level_code")
    private String levelCode;

    @HBaseColumn( column = "factory_code")
    private String factoryCode;

    @HBaseColumn( column = "process_code")
    private String processCode;

    @HBaseColumn( column = "area_code")
    private String  areaCode;

    @HBaseColumn( column = "line_code")
    private String  lineCode;

    @HBaseColumn( column = "work_dt")
    private String workDt;

    @HBaseColumn( column = "alarm_dt")
    private String alarmDt;

    @HBaseColumn( column = "request_no")
    private String requestNo;

    @HBaseColumn( column = "doc_type")
    private String docType;

    @HBaseColumn( column = "start_time_seq")
    private String startTimeSeq;

    @HBaseColumn( column = "start_time")
    private String startTime;

    @HBaseColumn( column = "end_time")
    private String endTime;

    @HBaseColumn( column = "alarm_start_time")
    private String alarmStartTime;

    @HBaseColumn( column = "alarm_end_time")
    private String alarmEndTime;

    @HBaseColumn( column = "breakdown_time")
    private String breakdownTime;

    @HBaseColumn( column = "update_dt")
    private String updateDt;

    @HBaseColumn( column = "update_by")
    private String updateBy;

    @HBaseColumn( column = "data_from")
    private String dataFrom;

    @Override
    public String getBaseRowKey() {
        return null;
    }

    public ADSProductionMachineFailureTimeDay buildADSProductionMachineFailureTimeDay() {
        String etlTime = DateTools.formatNow("yyyy-MM-dd HH:mm:ss");
        ADSProductionMachineFailureTimeDay ads = new ADSProductionMachineFailureTimeDay();
        BeanConvertTools.copyByName(this, ads);

        SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
        StringBuilder sb = new StringBuilder();
        try {
              sb.append(formatWorkDt.parse(this.workDt).getTime()).append(":")
                    .append(this.siteCode).append(":")
                    .append(this.levelCode).append(":")
                    .append(this.areaCode).append(":")
                    .append(this.lineCode).append(":")
                    .append(this.requestNo).append(":")
                    .append(this.alarmStartTime);
        } catch (ParseException e) {
            throw new RuntimeException("构建rowKey失败！");
        }

        ads.setId(MD5.create().digestHex(sb.toString()));
        ads.setWorkDate(this.getWorkDt());
        ads.setSectionId(this.getStartTimeSeq());
        ads.setSectionDesc(this.getStartTime() + "-" + this.getEndTime());
        ads.setMachineFailureTimeActual(Integer.parseInt(this.getBreakdownTime()));
        ads.setEtlTime(etlTime);

        return ads;
    }
}
