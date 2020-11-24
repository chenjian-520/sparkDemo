package com.foxconn.dpm.common.tools;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author HY
 * @Date 2020/6/30 13:38
 * @Description TODO
 */
public class DateTools {

    private DateTools(){}

    /**
     * <H2>描述: 将毫秒时间转换为小时制
     *  如 1593394574731(2020-6-29 09:35:00)  转换为 (1593392400000)2020-6-29 09:00:00
     * </H2>
     * timeMillis ： 毫秒时间
     */
    public static String dateFormat(long dateTime, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date date = new Date(dateTime);
        return sdf.format(date);
    }


    /**
     * <H2>描述: 根据模板转换时间 -- 根据pattern来计算， 可以只取 天
     *  152342342345 --> 2020-6-30 15:24:44 -->  152342342345
     *  152342342345 --> 2020-6-30  -->  15234200000
     * </H2>
     * @date 2020/6/30
     */
    public static long timeMillisToLong(String timeMillis, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            long time = Long.parseLong(timeMillis);
            Date date = new Date(time);
            String dateStr = sdf.format(date);

            return sdf.parse(dateStr).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(timeMillis + "无法正确转换时间！");
        }
    }

    /**
     * <H2>描述: 根据模板转换时间 -- 根据pattern来计算， 可以只取 天
     *  2020-6-30 15:24:44 -->  152342342345
     *  2020-6-30  -->  15234200000
     * </H2>
     * @date 2020/6/30
     */
    public static long dateStrToLong(String dateStr, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            Date date = sdf.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
            throw new RuntimeException("[" +dateStr + "]日期转换出错！");
        }
    }

    /**
     * <H2>描述: 根据模板转换时间 -- 根据pattern来计算， 可以只取 天
     *  2020-6-30 15:24:44 -->  152342342345
     *  2020-6-30  -->  15234200000
     * </H2>
     * @date 2020/6/30
     */
    public static String dateStrToStr(String dateStr, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            Date date = sdf.parse(dateStr);
            return sdf.format(date);
        } catch (ParseException e) {
            throw new RuntimeException("日期转换出错！");
        }
    }

    /**
     * <H2>描述: 根据模板转换时间
     *  152342342345 --> 2020-6-30 15:24:44
     * </H2>
     * @date 2020/6/30
     */
    public static String timeMillisToStr(String timeMillis, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        long time = Long.parseLong(timeMillis);
        Date date = new Date(time);
        return sdf.format(date);
    }

    public static String formatNow(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date date = new Date();
        return sdf.format(date);
    }
}
