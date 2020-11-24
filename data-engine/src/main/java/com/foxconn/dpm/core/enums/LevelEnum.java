package com.foxconn.dpm.core.enums;

import lombok.Getter;

import java.util.Arrays;

/**
 * level 枚举 .
 *
 * @className: LevelEnum
 * @author: ws
 * @date: 2020/7/2 14:12
 * @version: 1.0.0
 */
@Getter
public enum LevelEnum implements Strategy {

    /**
     * L5
     */
    L5("L5"),

    /**
     * L6
     */
    L6("L6"),

    /**
     * L10
     */
    L10("L10"),
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
    LevelEnum(String code) {
        this.code = code;
    }

    /**
     * 根据 code 获取枚举 .
     * @param code
     * @author ws
     * @date 2020/7/2 13:50
     * @return com.foxconn.dpm.core.enums.Strategy.Site
     **/
    public static LevelEnum getByCode(String code) {
        return Arrays.stream(values())
                .filter(item -> item.getCode().equals(code))
                .findFirst()
                .orElse(null);
    }
}
