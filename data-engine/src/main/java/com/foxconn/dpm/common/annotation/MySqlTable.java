package com.foxconn.dpm.common.annotation;

import java.lang.annotation.*;

/**
 * @Author HY
 * @Date 2020/7/6 11:58
 * @Description TODO
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MySqlTable {

    /**
     * <H2>描述: 表名  </H2>
     */
    String table();
}
