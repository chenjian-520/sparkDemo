package com.foxconn.dpm.common.tools;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.foxconn.dpm.common.bean.DateBean;
import com.foxconn.dpm.common.bean.HBaseBean;
import com.foxconn.dpm.common.consts.SystemConst;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author HY
 * @Date 2020/7/2 14:29
 * @Description TODO
 */
public class HBaseTools {

    private HBaseTools(){}

    /**
     * 从 Excel 中读取数据 .
     * @param tableName
     * @param classType
     * @param other
     * @author ws
     * @date 2020/7/7 14:40
     * @return org.apache.spark.api.java.JavaRDD<Bean>
     **/
    public static <Bean extends HBaseBean>  JavaRDD<Bean> getRddByExcel
            (String tableName, Class<Bean> classType, Map<String, Object> other) {
        ExcelReader reader = ExcelUtil.getReader("excel/" + tableName + ".xlsx");
        List<Map<String,Object>> readAll = reader.readAll();
        if (CollUtil.isEmpty(readAll)) {
            System.out.println("excel/" + tableName + ".xlsx 中无数据");
            return null;
        }
        // 去掉列簇
        List<Map<String, Object>> collect = readAll.stream().map(item -> {
            Map<String, Object> map = new LinkedHashMap<>();
            item.entrySet().forEach(entry -> {
                String key = entry.getKey();
                String[] keyArr = key.split("\\|");
                if (ArrayUtil.isNotEmpty(keyArr) && keyArr.length > 1) {
                    key = keyArr[1];
                }
                map.put(key, entry.getValue());
            });
            return map;
        }).collect(Collectors.toList());

        return BeanConvertTools.convertResultToBean(collect, classType);
    }

    public static <Bean extends HBaseBean>  JavaRDD<Bean> getSaltRdd(String tableName, Class<Bean> classType, Map<String, Object> other) throws Exception {
        // 获取传入的时间
        DateBean date = DateBeanTools.buildDateBean(other);
        System.out.println("入参：" + other);

        if (SystemConst.EXCEL.equals(other.get(SystemConst.KEY_DATASOURCE_TYPE))) {
            return getRddByExcel(tableName, classType, other);
        }

        // 读取hbase表数据
        JavaRDD<Result> result = DPHbase.saltRddRead(tableName, date.getStartTime(), date.getEndTime(), new Scan(),false).cache();
//        System.out.println("表"+tableName+"数据条数为：" + result.count());

        // Result转换为javaBean
        JavaRDD<Bean> javaRDD = BeanConvertTools.convertResultToBean(result, classType);
//        System.out.println("得到"+classType.getSimpleName()+"对象数据条数为：" + javaRDD.count());
        return javaRDD;

    }

    public static <Bean extends HBaseBean>  JavaRDD<Bean> getRdd(String tableName, Class<Bean> classType) throws Exception {
        return getRdd(tableName, classType, null);
    }

    public static <Bean extends HBaseBean>  JavaRDD<Bean> getRdd(String tableName, Class<Bean> classType, Map<String, Object> parameterMap) throws Exception {
        if (parameterMap != null && SystemConst.EXCEL.equals(parameterMap.get(SystemConst.KEY_DATASOURCE_TYPE))) {
            return getRddByExcel(tableName, classType, parameterMap);
        }
        // 读取hbase表数据
        JavaRDD<Result> result = DPHbase.rddRead(tableName,  new Scan(),false).cache();
//        System.out.println("表"+tableName+"数据条数为：" + result.count());

        // Result转换为javaBean
        JavaRDD<Bean> javaRDD = BeanConvertTools.convertResultToBean(result, classType);
//        System.out.println("得到"+classType.getSimpleName()+"对象数据条数为：" + javaRDD.count());

        return javaRDD;

    }

}
