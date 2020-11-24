package com.foxconn.dpm.sprint5.dws_ads.udf;

import org.apache.spark.sql.api.java.UDF1;

import javax.xml.crypto.Data;
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
public class CalculteQuarterday implements UDF1<String, Integer> {
    @Override
    public Integer call(String day) throws Exception {
        Calendar cale = Calendar.getInstance();
        cale.set(Calendar.DAY_OF_MONTH, 1);
        Calendar startQuarter = getStartQuarter(cale);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String format1 = format.format(startQuarter.getTime());
        Date parse = format.parse(day);
        Integer integer = Integer.valueOf( String.valueOf((parse.getTime() - format.parse(format1).getTime()) / (1000 * 60 * 60 * 24)));
        return integer;
    }

    public static Calendar getStartQuarter(Calendar today){
        int currentMonth = today.get(Calendar.MONTH) + 1;
        try {
            if (currentMonth >= 1 && currentMonth <= 3) {
                today.set(Calendar.MONTH, 0);
            } else if (currentMonth >= 4 && currentMonth <= 6) {
                today.set(Calendar.MONTH, 3);
            } else if (currentMonth >= 7 && currentMonth <= 9) {
                today.set(Calendar.MONTH, 4);
            } else if (currentMonth >= 10 && currentMonth <= 12) {
                today.set(Calendar.MONTH, 9);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return today;
    }

}
