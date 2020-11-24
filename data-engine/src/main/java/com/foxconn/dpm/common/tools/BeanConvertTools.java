package com.foxconn.dpm.common.tools;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.annotation.HBaseColumn;
import com.foxconn.dpm.common.annotation.HBaseTable;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author HY
 * @Date 2020/6/28 9:32
 * @Description TODO
 */
public class BeanConvertTools {

    private BeanConvertTools() {}

    public static <Bean extends HBaseBean> JavaRDD<Bean> convertResultToBean(JavaRDD<Result> result, Class<Bean> beanType) {

        HBaseTable annotation = beanType.getAnnotation(HBaseTable.class);
        if(annotation == null) {
            throw new RuntimeException("转换失败:" + beanType.getName() + "不存在HBaseTable注解!");
        }

        String family = annotation.family();
        return result.map(r -> {
            Bean bean = beanType.newInstance();
            Field[] fields = beanType.getDeclaredFields();

            for (Field field : fields) {
                HBaseColumn columnAnnotation = field.getAnnotation(HBaseColumn.class);
                if(columnAnnotation == null) {
                    continue;
                }

                String columnName =  columnAnnotation.column();
                String valueStr = Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes(columnName)));

                field.setAccessible(true);
                field.set(bean, notNull(valueStr));
            }

            return bean;

        });

    }

    /**
     * list 转 rdd 一般用于从 Excel 加载数据 .
     * @param collect
     * @param beanType
     * @author ws
     * @date 2020/7/7 14:31
     * @return org.apache.spark.api.java.JavaRDD<Bean>
     **/
    public static <Bean extends HBaseBean> JavaRDD<Bean> convertResultToBean(List<Map<String, Object>> collect, Class<Bean> beanType) {
        if (CollUtil.isEmpty(collect)) {
            return null;
        }
        HBaseTable annotation = beanType.getAnnotation(HBaseTable.class);
        if(annotation == null) {
            throw new RuntimeException("转换失败:" + beanType.getName() + "不存在HBaseTable注解!");
        }

        List<Bean> list = collect.stream().map(map -> {
            Bean bean = null;
            try {
                bean = beanType.newInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
            Field[] fields = beanType.getDeclaredFields();
            Map<String, Object> beanMap = new LinkedHashMap<>();

            for (Field field : fields) {
                HBaseColumn columnAnnotation = field.getAnnotation(HBaseColumn.class);
                if (columnAnnotation == null) {
                    continue;
                }

                String key = columnAnnotation.column();
                Object value = map.get(key);

                beanMap.put(key, value);
            }
            BeanUtil.copyProperties(beanMap, bean);

            return bean;
        }).collect(Collectors.toList());

        JavaSparkContext context = DPSparkApp.getContext();
        return context.parallelize(list);
    }


    public static void copyByName(Object src, Object target) {
        if (src == null || target == null || src == target) {
            return;
        }
        try {
            Map<String, Field> srcFieldMap= getAssignableFieldsMap(src);
            Map<String, Field> targetFieldMap = getAssignableFieldsMap(target);
            for (String srcFieldName : srcFieldMap.keySet()) {
                Field srcField = srcFieldMap.get(srcFieldName);
                if (srcField == null) {
                    continue;
                }
                // 变量名需要相同
                if (!targetFieldMap.containsKey(srcFieldName)) {
                    continue;
                }
                Field targetField = targetFieldMap.get(srcFieldName);
                if (targetField == null) {
                    continue;
                }
                // 类型需要相同
                if (!srcField.getType().equals(targetField.getType())) {
                    continue;
                }
                targetField.setAccessible(true);
                targetField.set(target, srcField.get(src));
            }
        }catch (Exception e) {
            throw new RuntimeException("数据拷贝异常：" + e.getMessage());
        }
    }

    private static Map<String, Field> getAssignableFieldsMap(Object obj) {
        if (obj == null) {
            return new HashMap<>();
        }
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field field : obj.getClass().getDeclaredFields()) {
            // 过滤不需要拷贝的属性
            if (Modifier.isStatic(field.getModifiers())
                    || Modifier.isFinal(field.getModifiers())) {
                continue;
            }
            field.setAccessible(true);
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }


    public static String notNull(String str) {
        if (StringUtils.isEmpty(str) || "null".equalsIgnoreCase(str)) {
            return "";
        }
        return str;
    }

    public static <Bean extends HBaseBean> JavaRDD<Put> covertBeanToPut(JavaRDD<Bean> javaRDD, Class<Bean> beanType) {
        HBaseTable annotation = beanType.getAnnotation(HBaseTable.class);
        if(annotation == null) {
            throw new RuntimeException("转换失败:" + beanType.getName() + "不存在HBaseTable注解!");
        }

        String family = annotation.family();
        return javaRDD.map(rdd -> {
            Put put = new Put(Bytes.toBytes(rdd.getSalt() + ":" + rdd.getBaseRowKey()));
            Field[] fields = beanType.getDeclaredFields();

            for (Field field : fields) {
                HBaseColumn columnAnnotation = field.getAnnotation(HBaseColumn.class);
                if(columnAnnotation == null) {
                    continue;
                }
                field.setAccessible(true);
                String columnName =  columnAnnotation.column();
                String filedValue = String.valueOf(field.get(rdd) == null ? "" : field.get(rdd));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(filedValue));
            }

            return put;
        });
    }
}
