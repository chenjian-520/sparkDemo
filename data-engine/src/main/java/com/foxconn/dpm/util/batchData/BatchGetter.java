package com.foxconn.dpm.util.batchData;

import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsProductionOutputDD;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import scala.Int;
import scala.Tuple2;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author HS
 * @className BatchGetterFY
 * @description TODO
 * @date 2019/12/17 8:51
 */
public class BatchGetter implements MetaGetterRegistry, Serializable {
    private static Gson gson = new Gson();
    private String LEFT_BRACE = "{";
    private String RIGHT_BRACE = "}";
    private String LEFT_BRACKETS = "[";
    private String RIGHT_BRACKETS = "]";
    private String COMMA = ",";
    private String DOUBLE_QUOTATION_MARK = "\"";

    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat unSepSimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    private static Calendar calendar = Calendar.getInstance();
    private static HashMap<Integer, Tuple2<String, String>> yearWeekCache = new HashMap<>();
    private static HashMap<String, Integer> dayWeekCache = new HashMap<>();

    /*
     * ====================================================================
     * 描述:
     *      初始化该类时缓存当前年前10年和后10年的week信息
     * ====================================================================
     */
    static {
        calendar.setTime(new Date());
        int year = calendar.get(Calendar.YEAR);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        for (int i = 0; i < 11; i++) {
            calendar.set(Calendar.YEAR, year + i);
            loadYearInfo(calendar);
        }

        for (int i = 1; i < 11; i++) {
            calendar.set(Calendar.YEAR, year - i);
            loadYearInfo(calendar);
        }

    }

    private static void loadYearInfo(Calendar calendar) {
        int yearWeek = 1;
        int maxDay = calendar.getActualMaximum(Calendar.DAY_OF_YEAR);

        String weekStart = "";
        String weekEnd = "";
        int nowYear = calendar.get(Calendar.YEAR);
        for (int j = 1; j <= maxDay; j++) {
            calendar.set(Calendar.DAY_OF_YEAR, j);
            int i = calendar.get(Calendar.YEAR);
            String nowDate = simpleDateFormat.format(calendar.getTimeInMillis());
            int nowDay = calendar.get(Calendar.DAY_OF_WEEK);
            nowDay = nowDay == 1 ? 7 : nowDay - 1;

            if (j == 1 || nowDay == 1) {
                weekStart = nowDate;
            }
            if (j == maxDay || nowDay == 7) {
                weekEnd = nowDate;
            }


            dayWeekCache.put(nowDate, nowYear * 100 + yearWeek);
            if (!weekStart.equals("") && !weekEnd.equals("")) {
                yearWeekCache.put(nowYear * 100 + yearWeek, new Tuple2<>(weekStart, weekEnd));
                weekStart = "";
                weekEnd = "";
                yearWeek++;
            }

        }

    }

    public static Integer loadYear(String date) throws ParseException {
        calendar.setTime(simpleDateFormat.parse(date));
        int year = calendar.get(Calendar.YEAR);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        for (int i = 0; i < 1; i++) {
            calendar.set(Calendar.YEAR, year + i);
            loadYearInfo(calendar);
        }
        return dayWeekCache.get(date);
    }


    public static void main(String[] args) {
    }


    /*
     * ====================================================================
     * 描述:
     *      方法只能计算当年
     * ====================================================================
     */
    public String getStData(String... sep) {
        if (sep != null && sep.length == 1) {
            return new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(new Date());
        } else {
            return new SimpleDateFormat("yyyyMMdd").format(new Date());
        }
    }

    public Tuple2<String, String> getStDateYear(int year, String... sep) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        String startDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(calendar.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(calendar.getTime()));

        calendar.add(Calendar.YEAR, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        String endDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(calendar.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(calendar.getTime()));

        return new Tuple2<>(startDate, endDate);
    }


    public Tuple2<String, String> getStDateQuarter(int quarter, String... sep) {
        if (quarter < 1) {

            int quarterSub = Math.abs(quarter);
            int targetYear = getNowYear() - 1;
            int targetQuarter = 4;

            for (int i = 1; i <= quarterSub; i++) {
                if (targetQuarter == 1) {
                    targetQuarter = 4;
                    targetYear--;
                } else {
                    targetQuarter--;
                }
            }
            return getStTargetQuarterYear(targetYear, targetQuarter, sep);

        }
        if (quarter > 4) {
            int quarterAdd = quarter - 4;
            int targetYear = getNowYear() + 1;
            int targetQuarter = 0;

            for (int i = 1; i <= quarterAdd; i++) {
                if (targetQuarter == 4) {
                    targetQuarter = 1;
                    targetYear++;
                } else {
                    targetQuarter++;
                }
            }
            return getStTargetQuarterYear(targetYear, targetQuarter, sep);
        }
        String startDate = "";
        String endDate = "";
        switch (quarter) {
            case 1:
                startDate = getStDateMonth(1, sep)._1;
                endDate = getStDateMonth(3, sep)._2;
                break;
            case 2:
                startDate = getStDateMonth(4, sep)._1;
                endDate = getStDateMonth(6, sep)._2;
                break;
            case 3:
                startDate = getStDateMonth(7, sep)._1;
                endDate = getStDateMonth(9, sep)._2;
                break;
            case 4:
                startDate = getStDateMonth(10, sep)._1;
                endDate = getStDateMonth(12, sep)._2;
                break;
        }
        if (startDate.equals("") || endDate.equals("")) {
            return null;
        }
        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<String, String> getStTargetQuarterYear(int year, int quarter, String... sep) {
        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date());
        instance.set(Calendar.YEAR, year);
        String startDate = "";
        String endDate = "";
        switch (quarter) {
            case 1:
                instance.set(Calendar.MONTH, 0);
                instance.set(Calendar.DAY_OF_MONTH, 1);
                startDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                instance.set(Calendar.MONTH, 2);
                instance.set(Calendar.DAY_OF_MONTH, instance.getActualMaximum(Calendar.DAY_OF_MONTH));
                endDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                break;
            case 2:
                instance.set(Calendar.MONTH, 3);
                instance.set(Calendar.DAY_OF_MONTH, 1);
                startDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                instance.set(Calendar.MONTH, 5);
                instance.set(Calendar.DAY_OF_MONTH, instance.getActualMaximum(Calendar.DAY_OF_MONTH));
                endDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                break;
            case 3:
                instance.set(Calendar.MONTH, 6);
                instance.set(Calendar.DAY_OF_MONTH, 1);
                startDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                instance.set(Calendar.MONTH, 8);
                instance.set(Calendar.DAY_OF_MONTH, instance.getActualMaximum(Calendar.DAY_OF_MONTH));
                endDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                break;
            case 4:
                instance.set(Calendar.MONTH, 9);
                instance.set(Calendar.DAY_OF_MONTH, 1);
                startDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                instance.set(Calendar.MONTH, 11);
                instance.set(Calendar.DAY_OF_MONTH, instance.getActualMaximum(Calendar.DAY_OF_MONTH));
                endDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(instance.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(instance.getTime()));

                break;
        }
        if (startDate.equals("") || endDate.equals("")) {
            return null;
        }
        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<String, String> getStDateMonth(int month, String... sep) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        String startDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(calendar.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(calendar.getTime()));

        calendar.add(Calendar.MONTH, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        String endDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(calendar.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(calendar.getTime()));
        if (startDate.equals("") || endDate.equals("")) {
            return null;
        }
        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<String, String> getStDateWeek(int week) {
        calendar.setTime(new Date());
        int weekId = calendar.get(Calendar.YEAR) * 100 + week;
        return yearWeekCache.get(weekId);
    }

    public int getDateWeek(String date) {
        Integer weekYear = dayWeekCache.get(date);
        try {
            return weekYear != null ? weekYear : loadYear(date);
        } catch (ParseException e) {
            return 0;
        }
    }

    public int getStDateDayCount(String startDay, String endDay) {
        Double v = -1.0;
        try {
            v = (simpleDateFormat.parse(endDay).getTime() + 0.0 - simpleDateFormat.parse(startDay).getTime() + 0.0) / 1000 / 60 / 60 / 24;
            if (v.intValue() < v) {
                v += 1;
            }
            return v.intValue();
        } catch (ParseException e) {
        }
        return -1;
    }

    public int getWeekDayCount(int yearWeek) {
        //202031
        Tuple2<String, String> dayRange = yearWeekCache.get(yearWeek);
        return getStDateDayCount(dayRange._1, dayRange._2);
    }


    //yyyy-MM-dd
    public int getStDateMonthDayCount(String dayDate, String... sep) {
        String sepstr = (sep == null || sep.length == 0 ? "" : sep[0]);
        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(new SimpleDateFormat("yyyy" + sepstr + "MM" + sepstr + "dd").parse(dayDate));
            return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
        } catch (ParseException e) {
        }
        return -1;
    }

    public int getStDateQuarterDayCount(String dayDate) {
        String sepstr = "-";
        Tuple2<String, String> quarterRange = getStTargetQuarterYear(Integer.valueOf(dayDate.replace(sepstr, "").substring(0, 4)), getTargetDateQuarter(dayDate, sepstr), sepstr);
        return getStDateDayCount(quarterRange._1, quarterRange._2);
    }

    public int getTargetDateQuarter(String dayDate, String... sep) {
        String sepstr = (sep == null || sep.length == 0 ? "" : sep[0]);
        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(new SimpleDateFormat("yyyy" + sepstr + "MM" + sepstr + "dd").parse(dayDate));
        } catch (ParseException e) {
        }
        int quarter = 1;
        int m = calendar.get(Calendar.MONTH) + 1;
        if (m >= 1 && m <= 3) {
            quarter = 1;
        }
        if (m >= 4 && m <= 6) {
            quarter = 2;
        }
        if (m >= 7 && m <= 9) {
            quarter = 3;
        }
        if (m >= 10 && m <= 12) {
            quarter = 4;
        }
        return quarter;
    }

    public int getStDateYearDayCount(String dayDate, String... sep) {
        String sepstr = (sep == null || sep.length == 0 ? "" : sep[0]);
        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(new SimpleDateFormat("yyyy" + sepstr + "MM" + sepstr + "dd").parse(dayDate));
            return calendar.getActualMaximum(Calendar.DAY_OF_YEAR);
        } catch (ParseException e) {
        }
        return -1;
    }

    public Tuple2<String, String> getStDateWeekAdd(int weekAdd, String... sep) {
        Tuple2<String, String> weekAddSep = getStDateWeekAddSep(weekAdd);
        if (sep == null || sep.length == 0) {
            return new Tuple2<String, String>(weekAddSep._1.replace("-", ""), weekAddSep._2.replace("-", ""));
        } else {
            return new Tuple2<String, String>(weekAddSep._1.replace("-", sep[0]), weekAddSep._2.replace("-", sep[0]));
        }

    }

    //最大不超过530周，因为一年最少53周，所以正负10年界限为530周
    //注意该方法返回的日期是使用 - 分隔的。。。。没有sep约束
    public Tuple2<String, String> getStDateWeekAddSep(int weekAdd) {
        try {
            int nowWeekId = getNowWeek();

            if (weekAdd == 0) {
                return yearWeekCache.get(nowWeekId);
            }

            int nowYear = nowWeekId / 100;
            int nowWeek = nowWeekId % 100;


            //遍历近10年的weekid，找得到则返回对应的tuple，找不到就去逐年load。。。。
            //该处如果超过正负10年则第一次获取会蛮消耗性能，此处性能指的是分布式性能，单对象则没啥
            if (weekAdd > 0) {
                for (int i = 0; i <= weekAdd; i++) {
                    if (i == weekAdd) {
                        return yearWeekCache.get(nowYear * 100 + nowWeek);
                    } else {
                        if (yearWeekCache.get(nowYear * 100 + nowWeek) == null) {
                            nowYear++;
                            nowWeek = 1;
                        } else {
                            nowWeek++;
                        }
                    }
                }
            } else {
                for (int i = 0; i <= -weekAdd; i++) {
                    if (i == -weekAdd) {
                        return yearWeekCache.get(nowYear * 100 + nowWeek);
                    } else {
                        if (nowWeek > 0) {
                            nowWeek--;
                        }
                        if (nowWeek == 0) {
                            nowYear--;
                            for (int k = 60; k >= 50; k--) {
                                if (yearWeekCache.get(nowYear * 100 + k) != null) {
                                    nowWeek = k;
                                    break;
                                }
                            }
                            if (nowWeek == 0) {
                                return null;
                            }
                        }
                    }
                }
            }
        } catch (ParseException e) {
            return null;
        }
        return null;
    }

    public Tuple2<String, String> getStDateWeekStampAdd(int weekAdd, String... sep) {
        Tuple2<String, String> stDateWeekAdd = getStDateWeekAdd(weekAdd, "-");
        if (stDateWeekAdd == null) {
            return null;
        }
        try {
            String sepStr = "-";
            String startDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateWeekAdd._1).getTime());
            String endDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateWeekAdd._2).getTime());
            return new Tuple2<String, String>(startDate, endDate);
        } catch (ParseException e) {
            return null;
        }
    }

    public Tuple2<String, String> getStDateMonthStampAdd(int monthAdd, String... sep) {
        Tuple2<String, String> stDateMonthAdd = getStDateMonthAdd(monthAdd, sep);
        if (stDateMonthAdd == null) {
            return null;
        }
        try {
            String sepStr = sep == null || sep.length == 0 ? "" : sep[0];
            String startDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateMonthAdd._1).getTime());
            String endDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateMonthAdd._2).getTime());
            return new Tuple2<String, String>(startDate, endDate);
        } catch (ParseException e) {
            return null;
        }
    }


    public Tuple2<String, String> getStDateMonthAdd(int monthAdd, String... sep) {
        return getStDateMonth(getNowMonth() + monthAdd, sep);
    }

    public Tuple2<String, String> getStDateQuarterAdd(int quarterAdd, String... sep) {
        return getStDateQuarter(getNowQuarter() + quarterAdd, sep);
    }

    public Tuple2<String, String> getStDateQuarterStampAdd(int quarterAdd, String... sep) {
        Tuple2<String, String> stDateQuarterAdd = getStDateQuarterAdd(quarterAdd, sep);
        if (stDateQuarterAdd == null) {
            return null;
        }
        try {
            String sepStr = sep == null || sep.length == 0 ? "" : sep[0];
            String startDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateQuarterAdd._1).getTime());
            String endDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateQuarterAdd._2).getTime());
            return new Tuple2<String, String>(startDate, endDate);
        } catch (ParseException e) {
            return null;
        }
    }

    public Tuple2<String, String> getStDateYearAdd(int yearAdd, String... sep) {
        return getStDateYear(getNowYear() + yearAdd, sep);
    }

    public String getStDateDayStrAdd(String sourceDate, int addDay, String sepIn) {
        try {
            String sep = sepIn != null && !sepIn.equals("") ? sepIn : "";
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new SimpleDateFormat("yyyy" + sep + "MM" + sep + "dd").parse(sourceDate));
            calendar.add(Calendar.DAY_OF_YEAR, addDay);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return new SimpleDateFormat("yyyy" + sep + "MM" + sep + "dd").format(calendar.getTime());
        } catch (ParseException e) {
            return null;
        }
    }


    public String getStDateDayAdd(int addDay, String... sep) {
        try {

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.add(Calendar.DAY_OF_YEAR, addDay);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            if (sep != null && sep.length == 1) {
                return new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(calendar.getTime());
            } else {
                return new SimpleDateFormat("yyyyMMdd").format(calendar.getTime());
            }
        } catch (Exception e) {
            return null;
        }
    }

    public String getStDateDayStampAdd(int addDay, String... sep) {
        try {
            String stDateDayStrAdd = getStDateDayAdd(addDay, sep);
            if (stDateDayStrAdd == null) {
                return null;
            }
            if (sep != null && sep.length == 1) {

                return String.valueOf(new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").parse(stDateDayStrAdd).getTime());
            } else {
                return String.valueOf(new SimpleDateFormat("yyyyMMdd").parse(stDateDayStrAdd).getTime());
            }
        } catch (ParseException e) {
            return null;
        }
    }

    public Tuple2<String, String> getStDateYearStampAdd(int addYear, String... sep) {
        Tuple2<String, String> stDateYearAdd = getStDateYearAdd(addYear, sep);
        if (stDateYearAdd == null) {
            return null;
        }
        try {
            String sepStr = sep == null || sep.length == 0 ? "" : sep[0];
            String startDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateYearAdd._1).getTime());
            String endDate = String.valueOf(new SimpleDateFormat("yyyy" + sepStr + "MM" + sepStr + "dd").parse(stDateYearAdd._2).getTime());
            return new Tuple2<String, String>(startDate, endDate);
        } catch (ParseException e) {
            return null;
        }
    }

    public Long formatTimestampMilis(String dateTime, String regex) {

        try {
            return new SimpleDateFormat(regex).parse(dateTime).getTime();
        } catch (ParseException e) {
            return null;
        }

    }

    public String getNowMinuteAdd(int minute) {
        //yyyy-MM-dd HH:mm:ss
        return String.valueOf(formatTimestampMilis(getStDataMiniteAdd(minute, "-", ":"), "yyyy-MM-dd HH:mm:ss"));
    }

    public String getStDataMiniteTimestampAdd(int addMinite, String... sep) {
        String stDataMiniteAdd = getStDataMiniteAdd(addMinite, sep);
        try {
            if (sep != null && sep.length == 2) {
                return String.valueOf(new SimpleDateFormat("yyyy" + sep[0] + "mm" + sep[0] + "dd" + " " + "HH" + sep[1] + "mm" + sep[1] + "ss").parse(stDataMiniteAdd).getTime());
            } else {
                return String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(stDataMiniteAdd).getTime());
            }
        } catch (ParseException e) {
            return null;
        }
    }

    public String getStDataMiniteAdd(int addMinite, String... sep) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.MINUTE, addMinite);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        if (sep != null && sep.length == 2) {
            return new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd" + " " + "HH" + sep[1] + "mm" + sep[1] + "ss").format(calendar.getTime());
        } else {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        }
    }

    public String getStDataHourAdd(int addHour, String... sep) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, addHour);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        if (sep != null && sep.length == 2) {
            return new SimpleDateFormat("yyyy" + sep[0] + "mm" + sep[0] + "dd" + " " + "HH" + sep[1] + "mm" + sep[1] + "ss").format(calendar.getTime());
        } else {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        }
    }

    public int getNowWeek() throws ParseException {
        calendar.setTime(new Date());
        String date = simpleDateFormat.format(calendar.getTimeInMillis());
        Integer weekYear = dayWeekCache.get(date);
        weekYear = weekYear != null ? weekYear : loadYear(date);
        calendar.add(Calendar.YEAR, 1);
        String nexDate = simpleDateFormat.format(calendar.getTimeInMillis());
        if (dayWeekCache.get(nexDate) == null) {
            loadYear(nexDate);
        }
        calendar.add(Calendar.YEAR, -2);
        String preDate = simpleDateFormat.format(calendar.getTimeInMillis());
        if (dayWeekCache.get(preDate) == null) {
            loadYear(preDate);
        }
        return weekYear;
    }

    public int getNowMonth() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        return calendar.get(Calendar.MONTH) + 1;
    }

    public int getNowQuarter() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int quarter = 1;
        int m = calendar.get(Calendar.MONTH) + 1;
        if (m >= 1 && m <= 3) {
            quarter = 1;
        }
        if (m >= 4 && m <= 6) {
            quarter = 2;
        }
        if (m >= 7 && m <= 9) {
            quarter = 3;
        }
        if (m >= 10 && m <= 12) {
            quarter = 4;
        }
        return quarter;
    }

    public int getNowYear() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        return calendar.get(Calendar.YEAR);
    }

    public Integer formatInteger(Object obj) {
        try {
            return Integer.parseInt((String.valueOf(obj)).replace(",", "").trim().replace(" ", ""));
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                }
            } catch (Exception eIn) {
            }
            return 0;
        }
    }

    public Double formatDouble(Object obj) {
        try {
            return Double.parseDouble((String.valueOf(obj)).replace(",", "").trim().replace(" ", ""));
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                }
            } catch (Exception eIn) {
                return 0.0;
            }
            return 0.0;
        }
    }

    public Float formatFloat(Object obj) {
        try {
            float v = Float.parseFloat((String.valueOf(obj)).replace(",", "").trim().replace(" ", ""));
            if (Float.isNaN(v) || Float.isInfinite(v)) {
                v = 0f;
            }
            return v;
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                }
            } catch (Exception eIn) {
            }
            return 0.0f;
        }
    }

    public Long formatLong(Object obj) {
        try {
            return Long.parseLong((String.valueOf(obj)).replace(",", "").trim().replace(" ", ""));
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                }
            } catch (Exception eIn) {
            }
            return 0L;
        }
    }

    public String formatDecimal(Object obj) {
        NumberFormat numberInstance = NumberFormat.getNumberInstance();
        NumberFormat decimalInstance = DecimalFormat.getNumberInstance();
        numberInstance.setGroupingUsed(false);
        try {
            return decimalInstance.format(obj).replace(",", "");
        } catch (Exception e) {
            try {
                return ((String) obj).replace(",", "");
            } catch (Exception e1) {
                return null;
            }
        }
    }

    public String preFormatZeroStrLen(String w, int len, String... appendS) {
        String append = appendS != null && appendS.length == 1 ? appendS[0] : "0";
        if (w.length() < len) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len - w.length(); i++) {
                sb.append(append);
            }
            return sb.toString() + w;
        } else {
            return w;
        }
    }

    public String preFormatSepStrLen(String w, int len, String append) {
        return preFormatZeroStrLen(w, len, append);
    }

    public String endFormatZeroStrLen(String w, int len, String... appendS) {
        String append = appendS != null && appendS.length == 1 ? appendS[0] : "0";
        if (w.length() < len) {
            StringBuilder sb = new StringBuilder(w);
            for (int i = 0; i < len - w.length(); i++) {
                sb.append(append);
            }
            return sb.toString();
        } else {
            return w;
        }
    }

    public String endFormatAppendStrLen(String w, int len, String append) {
        return endFormatZeroStrLen(w, len, append);
    }

    public Date formatDateStr(String dateStr, String... udfRegexDate) {
        String regex = "yyyy-MM-dd";
        if (udfRegexDate != null && udfRegexDate.length == 1) {
            regex = udfRegexDate[0];
        }
        try {
            return new SimpleDateFormat(regex).parse(dateStr);
        } catch (ParseException e) {
            return null;
        }
    }

    public String formatDateStrTo(String dateStr, String sourcePattern, String destPattern) {
        Date date = formatDateStr(dateStr, sourcePattern);
        if (date == null) {
            return null;
        } else {
            try {
                return formatDate(date, destPattern);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public String formatDate(Date date, String regex) {
        if (regex == null && regex.equals("")) {
            return null;
        }
        return new SimpleDateFormat(regex).format(date);
    }

    public Boolean dateStrCompare(String srcDate, String targetDate, String regex, String compareRule) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(regex);
            long srcTimestamp = simpleDateFormat.parse(srcDate).getTime();
            long targetTimestamp = simpleDateFormat.parse(targetDate).getTime();
            boolean result = false;
            switch (compareRule) {
                case ">":
                    result = srcTimestamp > targetTimestamp;
                    break;
                case "<":
                    result = srcTimestamp < targetTimestamp;
                    break;
                case "=":
                    result = srcTimestamp == targetTimestamp;
                    break;
                case ">=":
                    result = srcTimestamp >= targetTimestamp;
                    break;
                case "<=":
                    result = srcTimestamp <= targetTimestamp;
                    break;
                default:
                    return null;
            }
            return result;
        } catch (ParseException e) {
            return false;
        }
    }

    public String getStrArrayOrg(String sep, String replaceNullStr, String... columnValues) {
        getReplaceNullStr(replaceNullStr, columnValues);
        try {
            StringBuilder sb = null;
            if (columnValues == null || columnValues.length == 0) {
                return null;
            } else {
                String sepStr = ",";
                if (sep != null) {
                    sepStr = sep;
                }
                sb = new StringBuilder();
                for (int i = 0; i < columnValues.length; i++) {
                    sb.append(columnValues[i]);
                    if (i < columnValues.length - 1) {
                        sb.append(sepStr);
                    }
                }
                return sb.toString();
            }
        } catch (Exception e) {
            return null;
        }
    }

    public boolean dateInstanceCompare(Date srcDate, Date targetDate) {
        return srcDate.getTime() > targetDate.getTime();
    }

    public String strBinaryAnd(String s1, String s2) {
        try {
            if (s1.length() != s2.length()) {
                return null;
            }

            char[] ch1s = s1.toCharArray();
            char[] ch2s = s2.toCharArray();
            char[] newChs = new char[s1.length()];
            for (int i = 0; i < ch1s.length; i++) {
                if (ch1s[i] == '1' || ch2s[i] == '1') {
                    newChs[i] = '1';
                }
            }
            return String.valueOf(newChs);
        } catch (Exception e) {
            return null;
        }
    }

    public LinkedList<String> releaseBatch(String batchData) {
        try {
            return this.gson.fromJson(batchData, LinkedList.class);
        } catch (Exception e) {
            return null;
        }
    }

    public LinkedList<String> releaseBatch(String batchData, String sep) {
        try {
            String[] splits = batchData.split(sep);

            if (splits.length == 0) {
                return null;
            }
            LinkedList<String> data = new LinkedList<>();
            for (int i = 0; i < splits.length; i++) {
                data.add(splits[i]);
            }
            return data.size() > 0 ? data : null;
        } catch (Exception e) {
            return null;
        }
    }

    public String packBatch(String... datas) {
        try {
            return this.gson.toJson(datas);
        } catch (Exception e) {
            return null;
        }
    }

    public String[] formatRowToStringArray(Row row) {
        try {
            return (String[]) gson.fromJson(formatRowDataToJson(row), ArrayList.class).toArray(new String[0]);
        } catch (Exception e) {
            return null;
        }
    }

    public Row replaceRowValueIsOrder(Row row, Object... values) {
        return RowDataUtil.replaceRowValueIsOrder(row, values);
    }

    public Row replaceRowValueIsMap(Row row, HashMap<String, Object> values) {
        return RowDataUtil.replaceRowValueIsMap(row, values);
    }

    public String generatePrimaryMD5(String primaryKey, String assistKey) {
        String mainKey = generateMD5Seq(primaryKey);
        String otherKey = generateMD5Seq(assistKey);

        try {
            if (mainKey != null) {
                if (otherKey != null) {
                    return mainKey + otherKey;
                } else {
                    return mainKey + generateMD5Seq(System.currentTimeMillis() + "");
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public Row resultGetColumnsRow(Result result, String family, String... columnNames) {
        Row row = RowFactory.create(resultGetColumns(result, family, columnNames));
        return row != null ? row : null;
    }

    public Integer resultGetColumnsCount(Result result, String family) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family.getBytes());
        if (familyMap == null) {
            return null;
        } else {
            return familyMap.keySet().size();
        }
    }

    public String[] getReplaceNullStr(String replaceStr, String... columnValues) {
        if (columnValues == null || columnValues.length == 0) {
            return null;
        }
        for (int i = 0; i < columnValues.length; i++) {
            if (columnValues[i] == null || columnValues[i].equals("")) {
                columnValues[i] = replaceStr;
            }
        }
        return columnValues;
    }

    /*
     * ====================================================================
     * 描述:
     *      通过值列表自动给javabean赋值
     *      注意bean中的字段顺序序号和给定的值列表顺序一致
     *
     * ====================================================================
     */
    public <T> T getBeanDeftInit(T obj, ArrayList<String> rvs) {
        try {
            Field[] fields = ((T) obj).getClass().getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                try {
                    field.set(obj, getStringTargetTypeObj(field.getType().getSimpleName(), rvs.get(i)));
                } catch (Exception e) {
                    try {
                        field.set(obj, null);
                    } catch (IllegalAccessException e1) {
                    }
                }
            }
        } catch (Exception e) {
            return null;
        }
        return obj;
    }

    public Object getStringTargetTypeObj(String typeSimpleName, String value) {
        try {
            switch (typeSimpleName) {
                case "String":
                    return value;
                case "Integer":
                    return Integer.valueOf(value);
                case "Long":
                    return Long.valueOf(value);
                case "Float":
                    return Float.valueOf(value);
                case "Double":
                    return Double.valueOf(value);
                case "Date":
                    return value.matches("((^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(10|12|0?[13578])([-\\/\\._])(3[01]|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(11|0?[469])([-\\/\\._])(30|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(0?2)([-\\/\\._])(2[0-8]|1[0-9]|0?[1-9])$)|(^([2468][048]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([3579][26]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$))")
                            ?
                            DateFormat.getDateInstance().parse(value) != null
                            : false;
                case "Boolean":
                    return Boolean.valueOf(value);
                case "BigDecimal":
                    return new BigDecimal(value);
                case "Timestamp":
                    return Timestamp.valueOf(value);
            }
        } catch (Exception e) {
        }
        return null;
    }


    //获取指定时间段的result
    public boolean getFilterRangeTimeStampHBeans(Result result, String family, String timeColumnName, String targetTimeRegex, Long startStamp, Long endStamp) {
        SimpleDateFormat format = new SimpleDateFormat(targetTimeRegex);
        String work_dt = resultGetColumn(result, family, timeColumnName);
        long work_dt_stamp = 0;
        try {
            work_dt_stamp = format.parse(work_dt).getTime();
        } catch (ParseException e) {
            return false;
        }

        if (work_dt_stamp >= startStamp && work_dt_stamp < endStamp) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Description
     * 该方法默认使用空串代替值
     * Example
     * Author HS
     * Version
     * Time 10:02 2019/12/30
     */
    public ArrayList<String> resultGetColumns(Result result, String family, String... columnNames) {
        try {

            ArrayList<String> columnValues = new ArrayList<>();
            for (String columnName : columnNames) {
                String columnValue = null;
                if (columnName.toLowerCase().equals("rowkey")) {
                    columnValue = Bytes.toString(result.getRow());
                } else {
                    columnValue = Bytes.toString(result.getValue(family.getBytes(), columnName.getBytes()));
                }
                columnValues.add(columnValue != null ? columnValue : "");
            }
            return columnValues.size() == columnNames.length ? columnValues : null;
        } catch (Exception e) {
            return null;
        }
    }


    public String resultGetColumn(Result result, String family, String columnName) {
        try {
            String columnValue = null;
            if (columnName.toLowerCase().equals("rowkey")) {
                columnValue = Bytes.toString(result.getRow());
            } else {
                columnValue = Bytes.toString(result.getValue(family.getBytes(), columnName.getBytes()));
            }
            return columnValue != null ? columnValue : "";
        } catch (Exception e) {
            return null;
        }
    }

    /*
     * ====================================================================
     * 描述:
     *      1.请使用此函数检查实体完整性后再进行数据操作，过滤异常数据
     *      2.其余获取数据函数如果获取不到值返回空串避免程序空指针异常
     * ====================================================================
     */
    public boolean checkColumns(Result result, String family, String... columnNames) {
        try {

            for (String columnName : columnNames) {
                if (columnName.toLowerCase().equals("rowkey")) {
                    if (Bytes.toString(result.getRow()) == null) {
                        return false;
                    }
                } else {
                    if (Bytes.toString(result.getValue(family.getBytes(), columnName.getBytes())) == null) {
                        return false;
                    }
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    public String generateMD5Seq(String word) {
        StringBuilder sb = new StringBuilder();
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
            md5.update(word.getBytes());
            for (byte b : md5.digest()) {
                sb.append(String.format("%02X", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
        }
        return null;
    }

    public HashMap<String, String> formatRowToHashMap(Row row, List<StructField> schema) {
        try {
            HashMap<String, String> fieldNameValues = new HashMap<>();
            String[] values = formatRowToStringArray(row);
            for (int i = 0; i < row.size(); i++) {
                fieldNameValues.put(schema.get(i).name(), values[i]);
            }
            return fieldNameValues.size() == 0 ? null : fieldNameValues;
        } catch (Exception e) {
            return null;
        }
    }

    public String formatRowDataToJson(Row row) {
        try {
            return row.toString().replace(LEFT_BRACKETS, LEFT_BRACKETS + DOUBLE_QUOTATION_MARK).replace(RIGHT_BRACKETS, DOUBLE_QUOTATION_MARK + RIGHT_BRACKETS).replace(COMMA, DOUBLE_QUOTATION_MARK + COMMA + DOUBLE_QUOTATION_MARK);
        } catch (Exception e) {
            return null;
        }
    }

    public String formatUnSepStanderDate(String dateStr) {
        StringBuilder sb = new StringBuilder();
        if (dateStr.matches("^[\\d]+-[\\d]+-[\\d]+$")) {
            String[] splitDate = dateStr.split("-");
            if (splitDate.length == 3) {
                Integer year = formatInteger(splitDate[0]);
                Integer month = formatInteger(splitDate[1]);
                Integer day = formatInteger(splitDate[2]);
                return sb.append(year <= 9 ? "0" + year : year).append(month <= 9 ? "0" + month : month).append(day <= 9 ? "0" + day : day).toString();
            }
        } else if (dateStr.matches("^[\\d]+\\[\\d]+\\[\\d]+$")) {
            String[] splitDate = dateStr.split("\\");
            if (splitDate.length == 3) {
                Integer year = formatInteger(splitDate[0]);
                Integer month = formatInteger(splitDate[1]);
                Integer day = formatInteger(splitDate[2]);

                return sb.append(year <= 9 ? "0" + year : year).append(month <= 9 ? "0" + month : month).append(day <= 9 ? "0" + day : day).toString();
            }
        }
        return dateStr;
    }

    private BatchGetter() {
        if (null != StaticNestedInstance.instance) {
            throw new RuntimeException();
        }
    }

    private static final class StaticNestedInstance {
        private static final BatchGetter instance = new BatchGetter();
    }

    public static BatchGetter getInstance() {
        return StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return StaticNestedInstance.instance;
    }

    public String replaceEmpStr(String str, String... replcaceStr) {
        String rp = "";
        if (replcaceStr != null && replcaceStr.length > 0) {
            return str == null ? replcaceStr[0] : str;
        } else {
            return str == null ? rp : str;
        }

    }


    public boolean matchesData(String dateStr) {
        try {
            return dateStr.matches("((^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(10|12|0?[13578])([-\\/\\._])(3[01]|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(11|0?[469])([-\\/\\._])(30|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(0?2)([-\\/\\._])(2[0-8]|1[0-9]|0?[1-9])$)|(^([2468][048]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([3579][26]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$))")
                    ?
                    true
                    : false;
        } catch (Exception e) {
            return false;
        }
    }

    public void writeOrAppendLocalFile(String filePath, String writeLine) {


        File file = new File(filePath);
        try {
            if (file.exists() && file.isDirectory()) {
                return;
            } else {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                bufferedWriter.newLine();
                bufferedWriter.write(writeLine);
                bufferedWriter.flush();
                bufferedWriter.close();
            }

        } catch (Exception e) {

        }
    }

    public String getOldYear(int year, int month) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(System.currentTimeMillis());
        c.add(Calendar.YEAR, -year);
        c.add(Calendar.MONTH, -month);
        return String.valueOf(c.getTime().getTime());
    }

    public String cleanChinese(String words) {
        //return Pattern.compile("[\u4e00-\u9fa5]").matcher(words).replaceAll("");
        return words.replace("[\u4e00-\u9fa5]", "");
    }

    //fieldNames : =FieldA=FieldB=FieldC=     sep 为 =
    public <T> T toUpperValue(T obj, String fieldNames, String sep) {


        try {
            Class<?> aClass = obj.getClass();
            Field[] declaredFields = aClass.getDeclaredFields();
            for (Field declaredField : declaredFields) {
                declaredField.setAccessible(true);

                if (fieldNames.matches(sep.concat(declaredField.getName()).concat(sep))) {
                    declaredField.set(obj, ((String) declaredField.get(obj)).toUpperCase());
                }
            }
            return (T) obj;
        } catch (Exception e) {

            return null;
        }
    }

    public String concatWords(String sep, boolean isHeadTail, String... words) {

        try {

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < words.length; i++) {
                sb.append(words[i]);
                if (i < words.length - 1) {
                    sb.append(sep);
                }
            }

            if (isHeadTail) {
                sb.insert(0, sep);
                sb.insert(sb.length() - 1, sep);
            }

            return sb.toString();

        } catch (Exception e) {
            return null;
        }

    }

}
