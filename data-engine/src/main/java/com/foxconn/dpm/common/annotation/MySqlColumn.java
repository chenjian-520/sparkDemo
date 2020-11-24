package com.foxconn.dpm.common.annotation;

import com.foxconn.dpm.common.enums.MySqlDataTypes;

import java.lang.annotation.*;

/**
 * @Author HY
 * @Date 2020/7/6 11:58
 * @Description TODO
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MySqlColumn {

    /**
     * <H2>描述: 列名 </2>
     */
    String column();

    /**
     * <H2>描述: 数据类型 </H2>
     */
    MySqlDataTypes dataType() default MySqlDataTypes.STRING;

    /**
     * <H2>描述: 是否插入 </H2>
     */
    boolean insert() default true;

    /**
     * <H2>描述: 是否是主键 </H2>
     * @date 2020/7/22
     */
    boolean primaryKey() default  false;

}
