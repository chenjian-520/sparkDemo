package com.foxconn.dpm.common.annotation.filter;

import java.lang.annotation.*;

/**
 * @Author HY
 * @Date 2020/6/28 11:50
 * @Description TODO
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Filter {

    FilterEquals filterEquals();

}
