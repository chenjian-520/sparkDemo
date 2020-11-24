package com.foxconn.dpm.sprint1_2.dwd_dws.comparator;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author HS
 * @className AscIntegerComparator
 * @description 自定义Key比较器
 * @date 2020/1/13 13:30
 */
public class AscIntegerComparator implements Comparator<String>, Serializable {
    /*
     * ====================================================================
     * 描述:
     *
     *
     *
     * 参数:
     * @param keyInfo_1 keyInfo_2  ==>>>> Sep By  -   ===>>>> (hour, scandt)
     *
     *
     * 返回值:
     * ====================================================================
     */
    @Override
    public int compare(String keyInfo_1, String keyInfo_2) {
        //小于优先：降序
        /*a negative integer, zero, or a positive
         *          integer as the first argument is less than
         *         ,equal to, or greater than the
         *         second.*/
        Long comopareKey_1 = Long.valueOf(keyInfo_1.split("-")[1]);
        Long comopareKey_2 = Long.valueOf(keyInfo_2.split("-")[1]);
        return comopareKey_1 < comopareKey_2 ? Integer.valueOf(-1) : (comopareKey_1 > comopareKey_2 ? Integer.valueOf(1) : Integer.valueOf(0));
    }
}
