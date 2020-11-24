package com.foxconn.dpm.util;

import com.foxconn.dpm.sprint1_2.dwd_dws.L6Oee1DwdToDws;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class GetMonthTime {

    public static Long getStartTimeInMillis(){
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, 0); //获取当前月第一天
        c.set(Calendar.DAY_OF_MONTH, 1); //设置为1号,当前日期既为本月第一天
        c.set(Calendar.HOUR_OF_DAY, 0); //将小时至0
        c.set(Calendar.MINUTE, 0); //将分钟至0
        c.set(Calendar.SECOND,0); //将秒至0
        c.set(Calendar.MILLISECOND, 0); //将毫秒至0
        return c.getTimeInMillis();
    }

    public static Long getEndTimeInMillis(){
        Calendar c2 = Calendar.getInstance();
        c2.set(Calendar.DAY_OF_MONTH, c2.getActualMaximum(Calendar.DAY_OF_MONTH)); //获取当前月最后一天
        c2.set(Calendar.HOUR_OF_DAY, 23); //将小时至23
        c2.set(Calendar.MINUTE, 59); //将分钟至59
        c2.set(Calendar.SECOND,59); //将秒至59
        c2.set(Calendar.MILLISECOND, 999); //将毫秒至999
        return c2.getTimeInMillis();
    }

}
