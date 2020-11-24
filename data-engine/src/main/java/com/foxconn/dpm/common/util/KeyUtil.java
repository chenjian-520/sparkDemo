package com.foxconn.dpm.common.util;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.foxconn.dpm.common.consts.SystemConst;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * key 工具类 .
 *
 * @className: KeyUtil
 * @author: ws
 * @date: 2020/7/3 17:11
 * @version: 1.0.0
 */
public class KeyUtil {

    /**
     * 构建 key .
     * @param keys
     * @author ws
     * @date 2020/7/3 17:13
     * @return java.lang.String
     **/
    public static String buildKey(String ... keys) {
        if (ArrayUtil.isEmpty(keys)) {
            return "";
        }
        return Arrays.stream(keys)
                .map(key -> {
                    if (StrUtil.isBlank(key)) {
                        return "";
                    }
                    return key;
                })
                .collect(Collectors.joining(SystemConst.KEY_CONCAT_SYMBOL));
    }

    /**
     * 构建 key .
     * @param keys
     * @author ws
     * @date 2020/7/20 10:34
     * @return java.lang.String
     **/
    public static String buildKey(Object ... keys) {
        if (ArrayUtil.isEmpty(keys)) {
            return "";
        }
        return Arrays.stream(keys)
                .map(key -> {
                    if (key == null) {
                        return "";
                    }
                    if (StrUtil.isBlank(key.toString())) {
                        return "";
                    }
                    return key.toString();
                })
                .collect(Collectors.joining(SystemConst.KEY_CONCAT_SYMBOL));

    }

}
