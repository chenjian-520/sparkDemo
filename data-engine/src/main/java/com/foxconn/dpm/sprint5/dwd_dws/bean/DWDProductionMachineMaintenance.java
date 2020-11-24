package com.foxconn.dpm.sprint5.dwd_dws.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.common.tools.BeanConvertTools;
import com.foxconn.dpm.common.tools.DateTools;
import com.foxconn.dpm.common.tools.JsonTools;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_machine_maintenance.DateRuleBean;
import com.foxconn.dpm.sprint5.dwd_dws.bean.group.production_machine_maintenance.GroupRuleBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author HY
 * @Date 2020/6/28 17:30
 * @Description
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "dwd", table = "dpm_dwd_production_machine_maintenance", family = "DPM_DWD_PRODUCTION_MACHINE_MAINTENANCE")
public class DWDProductionMachineMaintenance extends HBaseBean {

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

    @HBaseColumn( column = "request_no")
    private String requestNo;

    @HBaseColumn( column = "doc_type")
    private String docType;

    @HBaseColumn( column = "alarm_code")
    private String alarmCode;

    @HBaseColumn( column = "alarm_remark")
    private String alarmRemark;

    @HBaseColumn( column = "eqp_code")
    private String eqpCode;

    @HBaseColumn( column = "eqp_name")
    private String eqpName;

    @HBaseColumn( column = "start_time")
    private String startTime;

    @HBaseColumn( column = "end_time")
    private String endTime;

    @HBaseColumn( column = "created_by")
    private String createdBy;

    @HBaseColumn( column = "update_dt")
    private String updateDt;

    @HBaseColumn( column = "update_by")
    private String updateBy;

    @HBaseColumn( column = "data_from")
    private String dataFrom;

    // 用于比较 时间是否存在区间段的特殊字段
    private long alarmStartTimeLong; // 真实报警开始时间  2020-07-03 07:48:23
    private long alarmEndTimeLong; // 真实报警结束时间  2020-07-03 08:48:23
    private long startTimeLong; // 时间段开始时间 2020-07-03 07:00:00
    private long endTimeLong; //时间段结束时间 2020-07-03 08:00:00

    private boolean startInDate;
    private boolean endInDate;



    // 分组规则为  按天
    public String getGroupBy() {
        return this.siteCode
                + this.levelCode
                + this.factoryCode
                + this.processCode
                + this.areaCode
                + this.lineCode
                + this.workDt;
    }

    @Override
    public String getBaseRowKey() {
        return null;
    }

    /**
     * <H2>描述:  alarmStartTime* alarmEndTime 跨过时间区间拆N条 -- 递归实现 </H2>
     * @date 2020/7/3
     */
    public List<DWSProductionMachineMaintenanceHH> buildDWSProductionMachineMaintenanceHH(List<DateRuleBean> times, int index) {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        String dayPattern = "yyyy-MM-dd";
        List<DWSProductionMachineMaintenanceHH> list = new ArrayList<>();
        DateRuleBean date = times.get(index);
        this.buildCompareField(date);


        // 报警开始时间不在times时间区间段
        if(!this.isStartInDate()) {
            return list;
        }


        DWSProductionMachineMaintenanceHH dd = new DWSProductionMachineMaintenanceHH();
        BeanConvertTools.copyByName(this, dd);
        dd.setStartTime(date.getStartTime());
        dd.setEndTime(date.getEndTime());
        dd.setStartTimeSeq(date.getStartTimeSeq());
        dd.setAlarmStartTime(this.getStartTime());
        dd.setAlarmDt(DateTools.dateStrToStr(this.getStartTime(),dayPattern));

        // 报警结束时间在times时间区间段
        if(this.isEndInDate()) {
            dd.setAlarmEndTime(this.getEndTime());
            long minutes = (this.getAlarmEndTimeLong() - this.getAlarmStartTimeLong()) / 1000;
            dd.setBreakdownTime(String.valueOf(minutes));
            list.add(dd);
            return list;
        }


        // 报警结束时间 超出 时间区间段  a、重置报警结束时间为区间段结束时间
        long minutes = (this.getEndTimeLong() - this.getAlarmStartTimeLong()) / 1000;
        dd.setBreakdownTime(String.valueOf(minutes));
        dd.setAlarmEndTime(DateTools.dateFormat(this.getEndTimeLong(), pattern));
        list.add(dd);



        // 报警结束时间 超出 时间区间段  b、重置报警开始时间为上一个时间段结束时间，用下一个times时间段递归
        this.setStartTime(DateTools.dateFormat(this.getEndTimeLong(), pattern));
        index = index + 1;
        // 到最后一个时间段 [如果index超过size，证明跨天]
        if(index == times.size() ) {
            index = 0;
        }


        list.addAll(buildDWSProductionMachineMaintenanceHH(times, index));

        return list;
    }



    public DWSProductionMachineMaintenanceHH buildDefaultDWSProductionMachineMaintenanceHH(DateRuleBean date) {
        DWSProductionMachineMaintenanceHH dd = new DWSProductionMachineMaintenanceHH();
        BeanConvertTools.copyByName(this, dd);
        dd.setBreakdownTime(SystemConst.ZERO);
        dd.setAlarmStartTime(SystemConst.NA);
        dd.setAlarmEndTime(SystemConst.NA);
        dd.setAlarmDt(SystemConst.NA);
        dd.setStartTime(date.getStartTime());
        dd.setEndTime(date.getEndTime());
        dd.setStartTimeSeq(date.getStartTimeSeq());
        return dd;
    }


    private void buildCompareField( DateRuleBean date ) {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        String dayPattern = "yyyy-MM-dd";
        String hourMinutePattern = "yyyy-MM-dd HHmm";

        String dateStartTime = date.getStartTime();  // 0900
        String alarmStartTime = this.getStartTime(); //2020年7月6日10:33:53
        String alarmEndTime = this.getEndTime(); ////2020年7月6日10:55:53

        // 如果结束时间为空。 就取当前时间切分
        if(alarmEndTime == null || "".equals(alarmEndTime)) {
            alarmEndTime = DateTools.formatNow(pattern);
        }

        this.alarmStartTimeLong = DateTools.dateStrToLong(alarmStartTime, pattern);
        this.alarmEndTimeLong = DateTools.dateStrToLong(alarmEndTime, pattern);
        // 获取alarmStartTime的年月日+规则的时分 （2020-07-06 0900）  转毫秒
        this.startTimeLong = DateTools.dateStrToLong(DateTools.dateStrToStr(alarmStartTime, dayPattern) + " " + dateStartTime, hourMinutePattern);
        // 由于直接计算出的毫秒数不对。。 所以结束时间 = 开始时间 + （差值毫秒）
        this.endTimeLong = startTimeLong + date.getOffsetMinutes() * 60 * 1000;

        // 0700                       0759                             0759                        0800
        boolean startIn = this.getStartTimeLong() <= this.getAlarmStartTimeLong() && this.getAlarmStartTimeLong() < this.getEndTimeLong();
        // 0700                       0813                             813                        0800
        boolean endIn = this.getStartTimeLong() < this.getAlarmEndTimeLong() && this.getAlarmEndTimeLong() <= this.getEndTimeLong();

        this.setStartInDate(startIn);
        this.setEndInDate(endIn);

    }


    public static void main(String[] args) {
        // 读取配置文件规则
        List<GroupRuleBean> ruleList = JsonTools.readJsonArray("config/sprint5/dwd_dws/production_machine_maintenance/ProductionMachineMaintenance.json", GroupRuleBean.class);
        System.out.println("时间段配置文件：" + ruleList.size());
        GroupRuleBean data = null;
        for(GroupRuleBean bean : ruleList) {

            if(bean.getSiteCode().equals("WH") && bean.getLevelCode().equals("L5")) {
                data = bean;
            }
        }


        for(int i=0; i<data.getTimes().size(); i++) {
            DWDProductionMachineMaintenance dwd = new DWDProductionMachineMaintenance();
            dwd.setRequestNo("R200422144151339922");
            dwd.setStartTime("2020-04-22 14:41:51");
            dwd.setEndTime("2020-07-07 16:34:07");
            List<DWSProductionMachineMaintenanceHH> dwsProductionMachineMaintenanceHHS = dwd.buildDWSProductionMachineMaintenanceHH(data.getTimes(), i);

            System.out.println(dwsProductionMachineMaintenanceHHS);
        }

    }
}
