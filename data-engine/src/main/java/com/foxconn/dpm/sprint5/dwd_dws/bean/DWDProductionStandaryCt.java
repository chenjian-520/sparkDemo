package com.foxconn.dpm.sprint5.dwd_dws.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.util.JoinConditionUtil;
import com.foxconn.dpm.common.util.KeyUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 标准CT .
 *
 * @className: DWDProductionStandaryCt
 * @author: ws
 * @date: 2020/7/3 15:56
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "dwd", table = "dpm_dwd_production_standary_ct", family = "DPM_DWD_PRODUCTION_STANDARY_CT")
public class DWDProductionStandaryCt extends HBaseBean {

    @HBaseColumn( column = "site_code")
    private String siteCode;

    @HBaseColumn( column = "level_code")
    private String levelCode;

    @HBaseColumn( column = "factory_code")
    private String factoryCode;

    @HBaseColumn( column = "process_code")
    private String processCode;

    @HBaseColumn( column = "line_code")
    private String lineCode;

    @HBaseColumn( column = "platform")
    private String platform;

    @HBaseColumn( column = "part_no")
    private String partNo;

    /**
     * <H2>描述: 一个料号到成品的时间  秒 </H2>
     * @date 2020/7/10
     */
    @HBaseColumn( column = "cycle_time")
    private String cycleTime;

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

    
    /**
     * <H2>描述: L10的关联逻辑不一样 </H2>
     * @date 2020/7/17
     */
    public String leftJoinCondition() {
        if("L10".equals(this.getLevelCode())) {
            return KeyUtil.buildKey(this.getSiteCode(), this.getLevelCode(), this.getFactoryCode()
                    , this.getProcessCode(), this.getLineCode(), this.getPlatform());
        }else {
            return KeyUtil.buildKey(this.getSiteCode(), this.getLevelCode(), this.getFactoryCode()
                    , this.getProcessCode(), this.getLineCode(), this.getPartNo());
        }
    }


    public static PairFunction<DWDProductionStandaryCt, String, DWDProductionStandaryCt> mapToPair = new PairFunction<DWDProductionStandaryCt, String, DWDProductionStandaryCt>(){
        @Override
        public Tuple2<String, DWDProductionStandaryCt> call(DWDProductionStandaryCt item) throws Exception {
            return new Tuple2<String, DWDProductionStandaryCt>(JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLineArea(item), item);
        }
    };
}
