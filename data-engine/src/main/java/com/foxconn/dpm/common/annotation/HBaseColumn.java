package com.foxconn.dpm.common.annotation;

import java.lang.annotation.*;

@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseColumn {

    /**
     * <H2>描述: 列名 </H2>
     * @date 2020/6/30
     */
    String column();

}
