package com.foxconn.dpm.util.enums;

import lombok.Getter;

/**
 * site 枚举 .
 *
 * @className: SiteEnum
 * @author: ws
 * @date: 2020/6/29 11:36
 * @version: 1.0.0
 */
@Getter
public enum SiteEnum {

    /**
     * 武汉
     */
    WH("WH"),

    /**
     * 重庆
     */
    CQ("CQ"),
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
    SiteEnum(String code) {
        this.code = code;
    }

}
