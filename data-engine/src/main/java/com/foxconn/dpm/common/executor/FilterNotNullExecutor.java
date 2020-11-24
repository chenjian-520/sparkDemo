package com.foxconn.dpm.common.executor;

import com.foxconn.dpm.common.annotation.filter.FilterNotNull;
import org.apache.spark.api.java.JavaRDD;

import java.lang.reflect.Field;

/**
 * @Author HY
 * @Date 2020/6/28 13:54
 * @Description TODO
 */
public class FilterNotNullExecutor  implements FilterExecutor {


    @Override
    public <T> JavaRDD<T> doFilter(JavaRDD<T> params, Class<T> classType) {
        FilterNotNull annotation = classType.getAnnotation(FilterNotNull.class);
        if(annotation == null) {
            return params;
        }

        String[] keyArray = annotation.fields();

        params.filter(param -> {
            boolean hasNotNull = true;
            for(String key : keyArray) {
                Field declaredField = classType.getDeclaredField(key);
                Object o = declaredField.get(param);

                if(o == null) {
                    hasNotNull = false;
                }
            }
            return hasNotNull;
        });

        return params;
    }
}
