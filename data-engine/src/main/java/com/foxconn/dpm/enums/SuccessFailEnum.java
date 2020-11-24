package com.foxconn.dpm.enums;

import lombok.Getter;

import java.util.Arrays;

/**
 * TODO .
 *
 * @className: SuccessFailEnum
 * @author: ws
 * @date: 2020/7/3 14:03
 * @version: 1.0.0
 */
@Getter
public enum SuccessFailEnum {

    /**
     * 成功
     */
    SUCCESS("0"),

    /**
     * 失败
     */
    FAIL("1"),
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
    SuccessFailEnum(String code) {
        this.code = code;
    }

    /**
     * 根据 code 获取枚举 .
     * @param code
     * @author ws
     * @date 2020/7/2 13:50
     * @return com.foxconn.dpm.enums.SuccessFailEnum
     **/
    public static SuccessFailEnum getByCode(String code) {
        return Arrays.stream(values())
                .filter(item -> item.getCode().equals(code))
                .findFirst()
                .orElse(null);
    }
}
