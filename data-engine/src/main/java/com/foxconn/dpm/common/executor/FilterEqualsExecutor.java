package com.foxconn.dpm.common.executor;

import org.apache.spark.api.java.JavaRDD;

/**
 * @Author HY
 * @Date 2020/6/28 13:54
 * @Description TODO
 */
public class FilterEqualsExecutor implements FilterExecutor {


    @Override
    public <T> JavaRDD<T> doFilter(JavaRDD<T> params, Class<T> classType) {
        return null;
    }
}
