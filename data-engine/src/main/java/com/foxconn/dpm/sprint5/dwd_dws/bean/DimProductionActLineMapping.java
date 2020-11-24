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
 * line_code 映射表 .
 *
 * @className: DimProductionActLineMapping
 * @author: ws
 * @date: 2020/7/20 11:35
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@HBaseTable(level = "dwd", table = "dpm_dim_production_act_line_mapping", family = "DPM_DIM_PRODUCTION_ACT_LINE_MAPPING")
public class DimProductionActLineMapping extends HBaseBean {

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

    @HBaseColumn( column = "area_code")
    private String areaCode;

    @HBaseColumn( column = "act_line_code")
    private String actLineCode;

    @HBaseColumn( column = "big_act_line_code")
    private String bigActLineCode;

    @HBaseColumn( column = "update_dt")
    private String updateDt;

    @HBaseColumn( column = "update_by")
    private String updateBy;

    @HBaseColumn( column = "data_from")
    private String dataFrom;


    @Override
    public String getBaseRowKey() {
        return KeyUtil.buildKey(getLineCode(), getAreaCode());
    }

    public static PairFunction<DimProductionActLineMapping, String, DimProductionActLineMapping> mapToPair = new PairFunction<DimProductionActLineMapping, String, DimProductionActLineMapping>(){
        @Override
        public Tuple2<String, DimProductionActLineMapping> call(DimProductionActLineMapping item) throws Exception {
            return new Tuple2<String, DimProductionActLineMapping>(JoinConditionUtil.buildKeyBySiteLevelFactoryProcessLineArea(item), item);
        }
    };
}
