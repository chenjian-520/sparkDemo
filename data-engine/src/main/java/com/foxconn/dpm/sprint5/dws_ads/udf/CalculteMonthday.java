package com.foxconn.dpm.sprint5.dws_ads.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-06-24
 * 获取当周 当月 当季 当年 到今天有多少天
 */
public class CalculteMonthday implements UDF1<String, Integer> {
    @Override
    public Integer call(String day) throws Exception {
        // 获取当前年份、月份、日期
        Calendar cale = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        // 获取前月的第一天
        cale = Calendar.getInstance();
        cale.add(Calendar.MONTH, 0);
        cale.set(Calendar.DAY_OF_MONTH, 1);
        String firstday = format.format(cale.getTime());
        Date parse = format.parse(day);
        Integer integer = Integer.valueOf( String.valueOf((parse.getTime() - format.parse(firstday).getTime()) / (1000 * 60 * 60 * 24)));
        return integer;
    }
}
