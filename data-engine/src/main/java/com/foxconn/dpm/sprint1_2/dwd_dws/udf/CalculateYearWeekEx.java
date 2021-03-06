package com.foxconn.dpm.sprint1_2.dwd_dws.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import java.text.SimpleDateFormat;

/**
 * @author HS
 * @className CalculateYearWeek
 * @description 该方法用于计算每一年的周:解决跨年周的问题
 *
 * @date 2020/1/2 18:29
 */
public class CalculateYearWeekEx implements UDF2<String, String, Integer> {
    public static SimpleDateFormat sepSimpleDateFormat =  new SimpleDateFormat("yyyy-MM-dd");
    @Override
    public Integer call(String date, String regex) throws Exception {
        return new CalculateYearWeek().call(sepSimpleDateFormat.format(new SimpleDateFormat(regex).parse(date)));
    }
}
