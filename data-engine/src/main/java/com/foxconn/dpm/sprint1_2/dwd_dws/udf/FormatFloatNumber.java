package com.foxconn.dpm.sprint1_2.dwd_dws.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.Decimal;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * @author HS
 * @className FormatFloatNumber
 * @description TODO
 * @date 2020/1/2 18:14
 */
public class FormatFloatNumber implements UDF1<Object, String> {
    public  static NumberFormat numberInstance = NumberFormat.getNumberInstance();
    public static NumberFormat decimalInstance = DecimalFormat.getNumberInstance();
    static {
        numberInstance.setGroupingUsed(false);
    }

    @Override
    public String call(Object number) throws Exception {
        try {
            return decimalInstance.format(number).replace(",", "");
        }catch (Exception e){
            return ((String) number).replace(",", "");
        }

    }
}
