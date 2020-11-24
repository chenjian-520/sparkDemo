package com.foxconn.dpm;


import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class AppTest
{
    //初始化环境
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    @Test
    public void test(){
        String yesterday = batchGetter.getStDateDayAdd(-1);
        String today = batchGetter.getStDateDayAdd(0);
        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
        String todayStamp = batchGetter.getStDateDayStampAdd(0);
        System.out.println(yesterdayStamp+" -- "+todayStamp);
        String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(yesterdayStamp)));
        String format1 = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String format2 = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(batchGetter.getStDateDayStampAdd(-1))));

        System.out.println(format);
        System.out.println(format1 + format2);

        String lastweekday = batchGetter.getStDateWeekAdd(-1,"-")._1;

        String dateWeek = String.valueOf(batchGetter.getDateWeek(lastweekday));

        System.out.println("".matches("DL.*"));
        SimpleDateFormat simpleDateFormat12 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String month = simpleDateFormat12.format(new Date(1590109883356L));
        System.out.println(month);
        Pattern pattern = Pattern.compile("^\\d{4}\\-\\d{2}\\-\\d{2}$");
        System.out.println(pattern.matcher("2012-5-2").matches());

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String todayFormatStr = simpleDateFormat.format(new Date(Long.valueOf(todayStamp)));
        String str = todayFormatStr + " 20:02:00.000";

        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long time = 0;
        try {
            time = simpleDateFormat1.parse(str).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Dataset<Row> dateset = null;
        if ((1590076800000l+60*60*20*1000) > time) {
            System.out.println("晚上考勤");
            System.out.println(str);
            System.out.println(time);
        } else {
            System.out.println("白天考勤");
            System.out.println(str);
            System.out.println(System.currentTimeMillis());
        }

    }



}
