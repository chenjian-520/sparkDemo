package com.foxconn.dpm.sprint1_2.dwd_dws.udf;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.jcodings.util.Hash;
import scala.Tuple2;
import scala.Tuple3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import static com.foxconn.dpm.util.batchData.BatchGetter.loadYear;

/**
 * @author HS
 * @className CalculateYearWeek
 * @description 该方法用于计算每一年的周:解决跨年周的问题
 *
 * @date 2020/1/2 18:29
 */
public class CalculateYearWeek implements UDF1<String, Integer> {
    private static BatchGetter batchGetter = BatchGetter.getInstance();

    @Override
    public Integer call(String date) throws Exception {
        try {
            //该格式为 yyyy-MM-dd
            Integer weekYear = batchGetter.getDateWeek(date);
            return weekYear != null ? weekYear : loadYear(date);
        }catch (Exception e){
            return  -1;
        }
    }
}
