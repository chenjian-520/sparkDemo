package com.foxconn.dpm.sprint5.dws_ads.bean;

import cn.hutool.crypto.digest.MD5;
import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.tools.DateTools;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.awt.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
       return null;
    }


    public ADSProductionOutputDay buildAdsProductionOutputDay() {

        ADSProductionOutputDay ads = new ADSProductionOutputDay();
        BeanConvertTools.copyByName(this, ads);

        String etlTime = DateTools.formatNow("yyyy-MM-dd HH:mm:ss");

        SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");
        StringBuilder sb = new StringBuilder();
        try {
             sb.append(formatWorkDt.parse(this.workDt).getTime()).append(":")
                    .append(this.siteCode).append(":")
                    .append(this.levelCode).append(":")
                    .append(this.areaCode).append(":")
                    .append(this.lineCode).append(":")
                    .append(this.startTime).append(":")
                    .append(this.stationCode);
        } catch (ParseException e) {
            throw new RuntimeException("构建rowKey失败！");
        }

        String uuid = sb.toString();
        // 保证每次生成ID有规则，后面有删除逻辑
        ads.setId(MD5.create().digestHex(uuid));
        ads.setEtlTime(etlTime);
        ads.setSectionId(this.getStartTimeSeq());
        ads.setSectionDesc(this.getStartTime() + "-" + this.getEndTime());
        ads.setOutputQtyActual(Integer.parseInt(this.getOutputQty()));
        ads.setWorkshiftCode(this.getWorkShift());
        // 不为0才计算
        if(SystemConst.ZERO.equals(this.getOutputQtyTarget()) || "".equals(this.getOutputQtyTarget())) {
            // 此处默认值全是 0 - 吴凯已确定
            ads.setOutputQtyTarget(0);
            ads.setGapQty(0);
            ads.setAchievementRate(0.00);
        } else {
            ads.setOutputQtyTarget(Integer.parseInt(this.getOutputQtyTarget()));
            // 差异
            ads.setGapQty(Integer.parseInt(this.getOutputQty()) - Integer.parseInt(this.getOutputQtyTarget()));
            // 达成率
            BigDecimal qty = new BigDecimal(this.getOutputQty());
            BigDecimal target = new BigDecimal(this.getOutputQtyTarget());
            BigDecimal divide = qty.divide(target, 2, RoundingMode.HALF_UP);
            ads.setAchievementRate(divide.doubleValue());
        }

        ads.setWorkshiftCode(this.getWorkShift());
        ads.setCustomerCode(this.getCustomer());
        ads.setWorkDate(this.getWorkDt());

        return  ads;
    }

}
