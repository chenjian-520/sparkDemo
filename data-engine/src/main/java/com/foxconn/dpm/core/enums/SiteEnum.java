package com.foxconn.dpm.core.enums;


import lombok.Getter;

import java.util.Arrays;

/**
 * 策略 枚举 .
 *
 * @className: SiteEnum
 * @author: ws
 * @date: 2020/7/2 11:35
 * @version: 1.0.0
 */
@Getter
public enum SiteEnum implements Strategy {

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
     * @date 2020/7/2 11:35
     * @return
     **/
    SiteEnum(String code) {
        this.code = code;
    }

    /**
     * 根据 code 获取枚举 .
     * @param code
     * @author ws
     * @date 2020/7/2 13:50
     * @return com.foxconn.dpm.core.enums.Strategy.Site
     **/
    public static SiteEnum getByCode(String code) {
        return Arrays.stream(values())
                .filter(item -> item.getCode().equals(code))
                .findFirst()
                .orElse(null);
    }
}
