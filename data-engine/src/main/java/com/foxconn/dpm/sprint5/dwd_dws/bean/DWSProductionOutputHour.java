package com.foxconn.dpm.sprint5.dwd_dws.bean;

import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.util.KeyUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author HY
 * @Date 2020/6/28 17:30
 * @Description TODO
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "dws", table = "dpm_dws_production_output_hh", family = "DPM_DWS_PRODUCTION_OUTPUT_HH")
public class DWSProductionOutputHour extends HBaseBean {


    @HBaseColumn( column = "site_code")
    private String siteCode;

    @HBaseColumn( column = "level_code")
    private String levelCode;

    @HBaseColumn( column = "factory_code")
    private String factoryCode;

    @HBaseColumn( column = "process_code")
    private String processCode;

    @HBaseColumn( column = "area_code")
    private String areaCode;

    @HBaseColumn( column = "line_code")
    private String lineCode;

    @HBaseColumn( column = "part_no")
    private String partNo;

    @HBaseColumn( column = "customer")
    private String customer;

    @HBaseColumn( column = "platform")
    private String platform;

    @HBaseColumn( column = "work_dt")
    private String workDt;

    @HBaseColumn( column = "work_shift")
    private String workShift;

    @HBaseColumn( column = "station_code")
    private String stationCode;

    @HBaseColumn( column = "start_time_seq")
    private String startTimeSeq;

    @HBaseColumn( column = "start_time")
    private String startTime;

    @HBaseColumn( column = "end_time")
    private String endTime;

    @HBaseColumn( column = "output_qty")
    private String outputQty;

    @HBaseColumn( column = "output_qty_target")
    private String outputQtyTarget;

    @HBaseColumn( column = "update_dt")
    private String updateDt;

    @HBaseColumn( column = "update_by")
    private String updateBy;

    @HBaseColumn( column = "data_from")
    private String dataFrom;


    @Override
    public String getBaseRowKey() {
        SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
        StringBuilder sb = new StringBuilder();
        try {
            return sb.append(formatWorkDt.parse(this.workDt).getTime()).append(":")
                    .append(this.siteCode).append(":")
                    .append(this.levelCode).append(":")
                    .append(this.areaCode).append(":")
                    .append(this.lineCode).append(":")
                    .append(this.startTime).append(":")
                    .append(this.stationCode).toString();
        } catch (ParseException e) {
            throw new RuntimeException("构建rowKey失败！");
        }
    }


    public String leftJoinCondition() {
        if("L10".equals(this.getLevelCode())) {
            return KeyUtil.buildKey(this.getSiteCode(), this.getLevelCode(), this.getFactoryCode()
                    , this.getProcessCode(), this.getLineCode(), this.getPlatform());
        }else {
            return KeyUtil.buildKey(this.getSiteCode(), this.getLevelCode(), this.getFactoryCode()
                    , this.getProcessCode(), this.getLineCode(), this.getPartNo());
        }

    }

}
