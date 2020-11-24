package com.foxconn.dpm.sprint5.dws_ads.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.text.ParseException;
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
public class CalculteYearday implements UDF1<String, Integer> {
    @Override
    public Integer call(String day) throws Exception {
        Calendar cale = Calendar.getInstance();
        cale.set(Calendar.DAY_OF_MONTH, 1);
        Calendar startQuarter = getStartYear(cale);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String format1 = format.format(startQuarter.getTime());
        Date parse = format.parse(day);
        Integer integer = Integer.valueOf( String.valueOf((parse.getTime() - format.parse(format1).getTime()) / (1000 * 60 * 60 * 24)));
        return integer;
    }

    /**
     * 获取当年起始时间
     */
    public static Calendar getStartYear(Calendar today){
        try {
            today.set(Calendar.MONTH, 0);
            today.set(Calendar.DAY_OF_MONTH, today.getActualMinimum(Calendar.DAY_OF_MONTH));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return today;
    }
}
