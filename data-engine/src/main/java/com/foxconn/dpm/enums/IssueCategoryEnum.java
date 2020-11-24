package com.foxconn.dpm.enums;

import lombok.Getter;

import java.util.Arrays;

/**
 * downtime issueCategory 枚举 .
 *
 * @className: IssueCategoryEnum
 * @author: ws
 * @date: 2020/7/13 10:25
 * @version: 1.0.0
 */
@Getter
public enum IssueCategoryEnum {

    /**
     * 交接班
     */
    HANDOVER("1", "29aae643-c5a2-11ea-a7a5-00505682ad31"),

    /**
     * 机故 id 为  dpm_dwd_production_machine_maintenance alarm_remark 字段
     */
    MACHINE_BREAKDOWN("2", ""),

    /**
     * 机种切换
     */
    SWITCH_MODELS("3", "29aaea62-c5a2-11ea-a7a5-00505682ad31"),

    /**
     * 保养
     */
    MAINTENANCE("4", "29aaf6fe-c5a2-11ea-a7a5-00505682ad31"),

    /**
     * 未排产
     */
    UNSCHEDULED("5", "e507278a-c250-11ea-a7a5-00505682ad31"),

    ;

    /**
     * 编码
     */
    private String code;

    /**
     * 名称
     */
    private String id;

    /**
     * 构造方法 .
     * @param code
     * @author ws
     * @date 2020/7/2 11:35
     * @return
     **/
    IssueCategoryEnum(String code, String id) {
        this.code = code;
        this.id = id;
    }

    /**
     * 根据 code 获取枚举 .
     * @param code
     * @author ws
     * @date 2020/7/2 13:50
     * @return com.foxconn.dpm.enums.IssueCategoryEnum
     **/
    public static IssueCategoryEnum getByCode(String code) {
        return Arrays.stream(values())
                .filter(item -> item.getCode().equals(code))
                .findFirst()
                .orElse(null);
    }
}
