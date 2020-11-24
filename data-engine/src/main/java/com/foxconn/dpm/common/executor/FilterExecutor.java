package com.foxconn.dpm.common.executor;

import org.apache.spark.api.java.JavaRDD;

/**
 * @Author HY
 * @Date 2020/6/28 13:58
 * @Description TODO
 */
public interface FilterExecutor {

    <T> JavaRDD<T> doFilter(JavaRDD<T> params, Class<T> classType);

}
