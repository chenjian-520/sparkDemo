package com.foxconn.dpm.sprint5.enums;

import lombok.Getter;

/**
 * 单据类型 枚举 .
 *
 * @className: MaintenanceDocTypeEnum
 * @author: ws
 * @date: 2020/7/14 17:14
 * @version: 1.0.0
 */
@Getter
public enum MaintenanceDocTypeEnum {

    /**
     * 报警
     */
    ALARM("R"),

    /**
     * 保养
     */
    MAINTENANCE("M"),
    ;

    /**
     * 编码
     */
    private String code;

    /**
     * 构造方法 .
     * @param code
     * @author ws
     * @date 2020/6/29 11:40
     * @return
     **/
    MaintenanceDocTypeEnum(String code) {
        this.code = code;
    }

}
