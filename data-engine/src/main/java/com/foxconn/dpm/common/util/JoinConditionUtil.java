package com.foxconn.dpm.common.util;

import cn.hutool.core.util.ReflectUtil;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.consts.SystemConst;
import com.foxconn.dpm.core.enums.LevelEnum;

import java.io.Serializable;

/**
 * 连接条件 工具 .
 *
 * @className: JoinConditionUtil
 * @author: ws
 * @date: 2020/7/20 10:19
 * @version: 1.0.0
 */
public class JoinConditionUtil {

    /**
     * 根据  siteCode levelCode factoryCode processCode 构建 key .
     * @param t
     * @author ws
     * @date 2020/7/20 10:37
     * @return java.lang.String
     **/
    public static <T extends Serializable> String buildKeyBySiteLevelFactoryProcess(T t) {
        if (t == null) {
            return "";
        }
        Object siteCode = ReflectUtil.getFieldValue(t, SystemConst.KEY_SITE_CODE);
        Object levelCode = ReflectUtil.getFieldValue(t, SystemConst.KEY_LEVEL_CODE);
        Object factoryCode = ReflectUtil.getFieldValue(t, SystemConst.KEY_FACTORY_CODE);
        Object processCode = ReflectUtil.getFieldValue(t, SystemConst.KEY_PROCESS_CODE);

        return KeyUtil.buildKey(siteCode, levelCode, factoryCode, processCode);
    }

    /**
     * 根据  siteCode levelCode factoryCode processCode lineCode 构建 key .
     * @param t
     * @author ws
     * @date 2020/7/20 10:37
     * @return java.lang.String
     **/
    public static <T extends Serializable> String buildKeyBySiteLevelFactoryProcessLine(T t) {
        if (t == null) {
            return "";
        }
        String keyTemp = buildKeyBySiteLevelFactoryProcess(t);
        Object lineCode = ReflectUtil.getFieldValue(t, SystemConst.KEY_LINE_CODE);

        return KeyUtil.buildKey(keyTemp, lineCode);
    }

    /**
     * 根据  siteCode levelCode factoryCode processCode lineCode areaCode 构建 key .
     * @param t
     * @author ws
     * @date 2020/7/20 10:37
     * @return java.lang.String
     **/
    public static <T extends Serializable> String buildKeyBySiteLevelFactoryProcessLineArea(T t) {
        if (t == null) {
            return "";
        }
        String keyTemp = buildKeyBySiteLevelFactoryProcessLine(t);
        Object areaCode = ReflectUtil.getFieldValue(t, SystemConst.KEY_AREA_CODE);
        return KeyUtil.buildKey(keyTemp, areaCode);
    }

    /**
     * 根据  siteCode levelCode factoryCode processCode lineCode partNo 或 platform 构建 key .
     * @param t
     * @author ws
     * @date 2020/7/20 11:29
     * @return java.lang.String
     **/
    public static <T extends Serializable> String buildKeyBySiteLevelFactoryProcessLinePartNoOrPlatform(T t) {
        if (t == null) {
            return "";
        }
        String keyTemp = buildKeyBySiteLevelFactoryProcessLine(t);

        Object levelCode = ReflectUtil.getFieldValue(t, SystemConst.KEY_LEVEL_CODE);
        if (levelCode == null) {
            return keyTemp;
        }
        LevelEnum levelEnum = LevelEnum.getByCode(levelCode.toString());
        if (levelEnum == LevelEnum.L10) {
            Object platform = ReflectUtil.getFieldValue(t, SystemConst.KEY_PLATFORM);
            return KeyUtil.buildKey(keyTemp, platform);
        }
        Object partNo = ReflectUtil.getFieldValue(t, SystemConst.KEY_PART_NO);
        return KeyUtil.buildKey(keyTemp, partNo);
    }

}
