package com.foxconn.dpm.sprint1_2.dws_ads.udf;

import com.foxconn.dpm.util.batchData.BatchGetter;
import org.apache.spark.sql.api.java.UDF1;

import java.text.SimpleDateFormat;

import static com.foxconn.dpm.util.batchData.BatchGetter.loadYear;

/**
 * @author HS
 * @className CalculateYearWeek
 * @description 该方法用于计算每一年的周:解决跨年周的问题
 *
 * @date 2020/1/2 18:29
 */
public class CalculateYearWeek implements UDF1<String, Integer> {
    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static BatchGetter batchGetter = BatchGetter.getInstance();

    @Override
    public Integer call(String date) throws Exception {
        try {
            Integer weekYear = batchGetter.getDateWeek(date);
            return weekYear != null ? weekYear : loadYear(date);
        }catch (Exception e){
            return  -1;
        }
    }
}
