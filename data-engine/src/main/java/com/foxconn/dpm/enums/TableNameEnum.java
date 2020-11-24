package com.foxconn.dpm.enums;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;

/**
 * 表名 枚举 .
 *
 * @className: TableNameEnum
 * @author: ws
 * @date: 2020/7/6 13:36
 * @version: 1.0.0
 */
public interface TableNameEnum {

    /**
     * ODS 层 .
     * @author ws
     * @date 2020/7/6 13:46
     **/
    @Getter
    enum ODS {

        /**
         * 线体异常缓冲时间配置表
         */
        PRODUCTION_DOWNTIME_BUFFER("dpm_ods_production_downtime_buffer"
                , Lists.newArrayList("DPM_ODS_PRODUCTION_DOWNTIME_BUFFER"))
        ;

        /**
         * 表名
         */
        private String tableName;

        /**
         * 列簇
         */
        private List<String> columnCluster;

        /**
         * 构造方法 .
         * @param tableName
         * @param columnCluster
         * @author ws
         * @date 2020/7/6 13:50
         * @return
         **/
        ODS(String tableName, List<String> columnCluster) {
            this.tableName = tableName;
            this.columnCluster = columnCluster;
        }
    }

    /**
     * DWD 层 .
     * @author ws
     * @date 2020/7/6 13:46
     **/
    @Getter
    enum DWD {

        /**
         * 日产出SN基础资料
         */
        PRODUCTION_OUTPUT("dpm_dwd_production_output"
                , Lists.newArrayList("DPM_DWD_PRODUCTION_OUTPUT")),

        /**
         * 标准CT
         */
        PRODUCTION_STANDARY_CT("dpm_dwd_production_standary_ct"
                , Lists.newArrayList("DPM_DWD_PRODUCTION_STANDARY_CT")),

        /**
         * 机台保养维修明细表
         */
        PRODUCTION_MACHINE_MAINTENANCE("dpm_dwd_production_machine_maintenance"
                , Lists.newArrayList("DPM_DWD_PRODUCTION_MACHINE_MAINTENANCE")),
        ;

        /**
         * 表名
         */
        private String tableName;

        /**
         * 列簇
         */
        private List<String> columnCluster;

        /**
         * 构造方法 .
         * @param tableName
         * @param columnCluster
         * @author ws
         * @date 2020/7/6 13:50
         * @return
         **/
        DWD(String tableName, List<String> columnCluster) {
            this.tableName = tableName;
            this.columnCluster = columnCluster;
        }
    }

    /**
     * DWS 层 .
     * @author ws
     * @date 2020/7/6 13:46
     **/
    @Getter
    enum DWS {

        /**
         * 线体停机小时统计
         */
        PRODUCTION_LINE_DOWNTIME_HH("dpm_dws_production_line_downtime_hh"
                , Lists.newArrayList("DPM_DWS_PRODUCTION_LINE_DOWNTIME_HH")),

        /**
         * Downtime_list
         */
        PRODUCTION_LINE_DOWNTIME_LIST("dpm_dws_production_line_downtime_list"
                , Lists.newArrayList("DPM_DWS_PRODUCTION_LINE_DOWNTIME_LIST")),
        ;

        /**
         * 表名
         */
        private String tableName;

        /**
         * 列簇
         */
        private List<String> columnCluster;

        /**
         * 构造方法 .
         * @param tableName
         * @param columnCluster
         * @author ws
         * @date 2020/7/6 13:50
         * @return
         **/
        DWS(String tableName, List<String> columnCluster) {
            this.tableName = tableName;
            this.columnCluster = columnCluster;
        }
    }

    /**
     * ADS 层 .
     * @author ws
     * @date 2020/7/6 13:46
     **/
    @Getter
    enum ADS {

        /**
         * 生产downtime
         */
        production_downtime_day("dpm_ads_production_downtime_day"
                , Lists.newArrayList("DPM_ADS_PRODUCTION_DOWNTIME_DAY")),

        /**
         * downtime明细资料
         */
        PRODUCTION_DOWNTIME_DETAIL("dpm_ads_production_downtime_detail"
                , Lists.newArrayList("DPM_ADS_PRODUCTION_DOWNTIME_DETAIL")),

        /**
         * issue_category 配置表
         */
        SYS_ISSUETRACKING_CATEGORY("sys_issuetracking_category"
                , Lists.newArrayList("SYS_ISSUETRACKING_CATEGORY")),

        ;

        /**
         * 表名
         */
        private String tableName;

        /**
         * 列簇
         */
        private List<String> columnCluster;

        /**
         * 构造方法 .
         * @param tableName
         * @param columnCluster
         * @author ws
         * @date 2020/7/6 13:50
         * @return
         **/
        ADS(String tableName, List<String> columnCluster) {
            this.tableName = tableName;
            this.columnCluster = columnCluster;
        }
    }

    /**
     * DIM 层 .
     * @author ws
     * @date 2020/7/6 13:46
     **/
    @Getter
    enum DIM {

        /**
         * 线体异常缓冲时间配置表
         */
        DIM_PRODUCTION_ACT_LINE_MAPPING("dpm_dim_production_act_line_mapping"
                , Lists.newArrayList("DPM_DIM_PRODUCTION_ACT_LINE_MAPPING"))
        ;

        /**
         * 表名
         */
        private String tableName;

        /**
         * 列簇
         */
        private List<String> columnCluster;

        /**
         * 构造方法 .
         * @param tableName
         * @param columnCluster
         * @author ws
         * @date 2020/7/6 13:50
         * @return
         **/
        DIM(String tableName, List<String> columnCluster) {
            this.tableName = tableName;
            this.columnCluster = columnCluster;
        }
    }

}
