package com.foxconn.dpm.common.annotation;

import java.lang.annotation.*;

@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseTable {

    /**
     * <H2>描述: 表的级别 [ODS DWD DWS ADS] </H2>
     * @date 2020/6/30
     */
    String level();

    /**
     * <H2>描述: 表名 </H2>
     * @date 2020/6/30
     */
    String table();

    /**
     * <H2>描述: 列族 </H2>
     * @date 2020/6/30
     */
    String family();

}
