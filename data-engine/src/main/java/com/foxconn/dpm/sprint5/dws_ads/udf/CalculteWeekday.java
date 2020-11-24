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
public class CalculteWeekday implements UDF1<String, Integer> {
    @Override
    public Integer call(String day) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date parse = format.parse(day);
        int num;
        Calendar calendar = Calendar.getInstance();
        int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        if (dayOfWeek == 1) {
            num = 0;
        } else if (dayOfWeek == 0){
            num = -6;
        }else {
            num = 1 - dayOfWeek;
        }
        calendar.add(Calendar.DATE, num);
        Date monday = calendar.getTime();
        String startStr = format.format(monday);
        calendar.add(Calendar.DATE, num+7);
        Date sunDay = calendar.getTime();
        String endStr = format.format(sunDay);
        Integer integer = Integer.valueOf( String.valueOf((parse.getTime() - format.parse(startStr).getTime()) / (1000 * 60 * 60 * 24)));
        return integer;
    }
}
