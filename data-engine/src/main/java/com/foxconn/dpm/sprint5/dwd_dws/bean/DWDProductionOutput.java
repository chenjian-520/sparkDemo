package com.foxconn.dpm.sprint5.dwd_dws.bean;

import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.util.KeyUtil;
import com.foxconn.dpm.core.enums.LevelEnum;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.DateRuleBean;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_output.FilterRuleBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * @Author HY
 * @Date 2020/6/28 17:30
 * @Description TODO
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@HBaseTable(level = "dwd", table = "dpm_dwd_production_output", family = "DPM_DWD_PRODUCTION_OUTPUT")
public class DWDProductionOutput extends HBaseBean {


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

    @HBaseColumn( column = "machine_id")
    private String machineId;

    @HBaseColumn( column = "part_no")
    private String partNo;

    @HBaseColumn( column = "sku")
    private String sku;

    @HBaseColumn( column = "platform")
    private String platform;

    @HBaseColumn( column = "customer")
    private String customer;

    @HBaseColumn( column = "wo")
    private String wo;

    @HBaseColumn( column = "workorder_type")
    private String workorderType;

    @HBaseColumn( column = "work_dt")
    private String workDt;

    @HBaseColumn( column = "work_shift")
    private String workShift;

    @HBaseColumn( column = "sn")
    private String sn;

    @HBaseColumn( column = "station_code")
    private String stationCode;

    @HBaseColumn( column = "station_name")
    private String stationName;

    @HBaseColumn( column = "is_fail")
    private String isFail;

    @HBaseColumn( column = "scan_by")
    private String scanBy;

    @HBaseColumn( column = "scan_dt")
    private String scanDt;

    @HBaseColumn( column = "output_qty")
    private String outputQty;

    @HBaseColumn( column = "update_dt")
    private String updateDt;

    @HBaseColumn( column = "update_by")
    private String updateBy;

    @HBaseColumn( column = "data_from")
    private String dataFrom;

    private String groupBy;


    public String getGroupBy() {
        return this.groupBy;
    }
    // 分组规则为 精确到客户
    public void setGroupBy(FilterRuleBean rule) {
        String group = this.siteCode
                + this.levelCode
                + this.factoryCode
                + this.processCode
                + this.areaCode
                + this.lineCode
                + this.stationCode
                + this.workDt;

        this.groupBy = group + rule.getCustomer();

    }




    /**
     * <H2>描述: 一条数据代表一个小时的产量， 可能存在有的小时段不存在数据 </H2>
     * @date 2020/6/29
     */
    public DWSProductionOutputHour buildDWSProductionOutputHour(DWDProductionOutput dwdProductionOutput, String qty, DateRuleBean date){
        DWSProductionOutputHour dwsProductionOutputHour = new DWSProductionOutputHour();

        BeanConvertTools.copyByName(dwdProductionOutput, dwsProductionOutputHour);
        // 产量
        dwsProductionOutputHour.setOutputQty(qty);

        // 时间
        dwsProductionOutputHour.setStartTime(date.getStartTime());
        dwsProductionOutputHour.setEndTime(date.getEndTime());
        dwsProductionOutputHour.setStartTimeSeq(date.getStartTimeSeq());
        dwsProductionOutputHour.setWorkShift(date.getShift());

        return dwsProductionOutputHour;
    }

    /**
     * <H2>描述: L10的关联逻辑不一样 </H2>
     * @date 2020/7/17
     */
    public String leftJoinCondition() {
        if(LevelEnum.L10.getCode().equals(this.getLevelCode())) {
            return KeyUtil.buildKey(this.getSiteCode(), this.getLevelCode(), this.getFactoryCode()
                    , this.getProcessCode(), this.getLineCode(), this.getPlatform());
        }else {
            return KeyUtil.buildKey(this.getSiteCode(), this.getLevelCode(), this.getFactoryCode()
                    , this.getProcessCode(), this.getLineCode(), this.getPartNo());
        }
    }

    @Override
    public String getBaseRowKey() {
        return null;
    }


    List<FilterRuleBean> filterList;
    public DWDProductionOutput(){}
    public DWDProductionOutput(List<FilterRuleBean> filterList) {
        this.filterList = filterList;
    }
    public Function<DWDProductionOutput, DWDProductionOutput> map = new Function<DWDProductionOutput, DWDProductionOutput>() {
        @Override
        public DWDProductionOutput call(DWDProductionOutput dwdProductionOutput)  {
            for(FilterRuleBean rule : filterList) {
                if(dwdProductionOutput.getStationCode().equals(rule.getOldStationCode())) {
                    dwdProductionOutput.setStationCode(rule.getNewStationCode());
                    dwdProductionOutput.setGroupBy(rule);
                }
            }
            return dwdProductionOutput;
        }
    };

    public Function<DWDProductionOutput, Boolean> filter = new Function<DWDProductionOutput, Boolean>() {
        @Override
        public Boolean call(DWDProductionOutput data)  {
            boolean flag = false;
            for(FilterRuleBean rule : filterList) {
                flag = rule.getFail().equals(data.getIsFail())
                        && rule.getLevelCode().equals(data.getLevelCode())
                        && rule.getSiteCode().equals(data.getSiteCode())
                        && rule.getNewStationCode().equals(data.getStationCode());
                if(flag) {
                    break;
                }
            }
            return flag;
        }
    };

}
