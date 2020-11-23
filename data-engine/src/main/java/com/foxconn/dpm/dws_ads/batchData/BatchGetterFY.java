package com.foxconn.dpm.dws_ads.batchData;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className BatchGetterFY
 * @description TODO
 * @date 2019/12/17 8:51
 */
public class BatchGetterFY implements MetaGetterRegistry, Serializable {
    private static Gson gson = new Gson();
    private String LEFT_BRACE = "{";
    private String RIGHT_BRACE = "}";
    private String LEFT_BRACKETS = "[";
    private String RIGHT_BRACKETS = "]";
    private String COMMA = ",";
    private String DOUBLE_QUOTATION_MARK = "\"";


    public static void main(String[] args) {
        System.out.println(MetaGetter.getBatchGetter().getStDataMiniteAdd(-1));
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

    public Tuple2<Long, Long> getStampDateYear(int year) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        Long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.YEAR, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        Long endDate = calendar.getTimeInMillis();

        return new Tuple2<>(startDate, endDate);
    }


    public Tuple2<String, String> getStDateQuarter(int quarter, String... sep) {
        if (quarter <= 1) {
            quarter = 1;
        }
        if (quarter >= 4) {
            quarter = 4;
        }
        String startDate = "";
        String endDate = "";
        switch (quarter) {
            case 1:
                startDate = getStDateMonth(1)._1;
                endDate = getStDateMonth(3)._2;
                break;
            case 2:
                startDate = getStDateMonth(4)._1;
                endDate = getStDateMonth(6)._2;
                break;
            case 3:
                startDate = getStDateMonth(7)._1;
                endDate = getStDateMonth(9)._2;
                break;
            case 4:
                startDate = getStDateMonth(10)._1;
                endDate = getStDateMonth(12)._2;
                break;
        }
        if (startDate == "" || endDate == "") {
            return null;
        }
        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<Long, Long> getStampDateQuarter(int quarter) {
        if (quarter <= 1) {
            quarter = 1;
        }
        if (quarter >= 4) {
            quarter = 4;
        }
        Long startDate = null;
        Long endDate = null;
        switch (quarter) {
            case 1:
                startDate = getStampDateMonth(1)._1;
                endDate = getStampDateMonth(3)._2;
                break;
            case 2:
                startDate = getStampDateMonth(4)._1;
                endDate = getStampDateMonth(6)._2;
                break;
            case 3:
                startDate = getStampDateMonth(7)._1;
                endDate = getStampDateMonth(9)._2;
                break;
            case 4:
                startDate = getStampDateMonth(10)._1;
                endDate = getStampDateMonth(12)._2;
                break;
        }
        if (startDate == null || endDate == null) {
            return null;
        }
        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<String, String> getStDateMonth(int month, String... sep) {
        if (month == 0 || month == 1) {
            month = 12;
        }
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
        if (startDate == "" || endDate == "") {
            return null;
        }
        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<Long, Long> getStampDateMonth(int month) {
        if (month == 0 || month == 1) {
            month = 12;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        Long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.MONTH, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        Long endDate = calendar.getTimeInMillis();

        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<String, String> getStDateWeek(int week, String... sep) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        if (week > calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)) {
            calendar.set(Calendar.WEEK_OF_YEAR, week - 1);
            calendar.add(Calendar.WEEK_OF_YEAR, 1);
            week = calendar.get(Calendar.WEEK_OF_YEAR);
            //跨年
            calendar.add(Calendar.YEAR, 1);
        }
        if (week < calendar.getActualMinimum(Calendar.WEEK_OF_YEAR)) {
            calendar.set(Calendar.WEEK_OF_YEAR, week + 1);
            calendar.add(Calendar.WEEK_OF_YEAR, -1);
            week = calendar.get(Calendar.WEEK_OF_YEAR);
            //去年
            calendar.add(Calendar.YEAR, -1);
        }
        calendar.set(Calendar.WEEK_OF_YEAR, week);
        calendar.set(Calendar.DAY_OF_WEEK, 2);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        String startDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(calendar.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(calendar.getTime()));

        calendar.add(Calendar.WEEK_OF_YEAR, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        String endDate = (sep != null && sep.length == 1) ? (new SimpleDateFormat("yyyy" + sep[0] + "MM" + sep[0] + "dd").format(calendar.getTime())) : (new SimpleDateFormat("yyyyMMdd").format(calendar.getTime()));

        if (startDate == "" || endDate == "") {
            return null;
        }

        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<Long, Long> getStampDateWeek(int week) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        if (week > calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)) {
            calendar.set(Calendar.WEEK_OF_YEAR, week - 1);
            calendar.add(Calendar.WEEK_OF_YEAR, 1);
            week = calendar.get(Calendar.WEEK_OF_YEAR);
            //跨年
            calendar.add(Calendar.YEAR, 1);
        }
        if (week < calendar.getActualMinimum(Calendar.WEEK_OF_YEAR)) {
            calendar.set(Calendar.WEEK_OF_YEAR, week + 1);
            calendar.add(Calendar.WEEK_OF_YEAR, -1);
            week = calendar.get(Calendar.WEEK_OF_YEAR);
            //去年
            calendar.add(Calendar.YEAR, -1);
        }
        calendar.set(Calendar.WEEK_OF_YEAR, week);
        calendar.set(Calendar.DAY_OF_WEEK, 2);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        Long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.WEEK_OF_YEAR, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        Long endDate = calendar.getTimeInMillis();

        if (startDate == null || endDate == null) {
            return null;
        }

        return new Tuple2<>(startDate, endDate);
    }

    public Tuple2<String, String> getStDateWeekAdd(int weekAdd, String... sep) {
        return getStDateWeek(getNowWeek() + weekAdd, sep);
    }

    public Tuple2<String, String> getStDateMonthAdd(int monthAdd, String... sep) {
        return getStDateMonth(getNowMonth() + monthAdd, sep);
    }

    public Tuple2<String, String> getStDateQuarterAdd(int quarterAdd, String... sep) {
        return getStDateQuarter(getNowQuarter() + quarterAdd, sep);
    }

    public Tuple2<String, String> getStDateYearAdd(int yearAdd, String... sep) {
        return getStDateYear(getNowYear() + yearAdd, sep);
    }

    public String getStDateDayAdd(int addDay, String... sep) {
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
    }

    public Long getStampDateDayAdd(int addDay) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR, addDay);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
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

    public int getNowWeek() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int week = calendar.get(Calendar.WEEK_OF_YEAR);
        int mouth = calendar.get(Calendar.MONTH);
        if (mouth >= 11 && week <= 1) {
            week += 52;
        }
        return week;
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
        return calendar.get(calendar.YEAR);
    }

    public Integer formatInteger(Object obj) {
        try {
            return Integer.parseInt(((String) obj).replace(",", "").trim().replace(" ", ""));
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                    System.err.println("NumberFormatErr==========>>>>>" + obj);
                }
            } catch (Exception eIn) {
                e.printStackTrace();
            }
            return 0;
        }
    }

    public Double formatDouble(Object obj) {
        try {
            return Double.parseDouble(((String) obj).replace(",", "").trim().replace(" ", ""));
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                    System.err.println("NumberFormatErr==========>>>>>" + obj);
                }
            } catch (Exception eIn) {
                e.printStackTrace();
            }
            return 0.0;
        }
    }

    public Float formatFloat(Object obj) {
        try {
            return Float.parseFloat(((String) obj).replace(",", "").trim().replace(" ", ""));
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                    System.err.println("NumberFormatErr==========>>>>>" + obj);
                }
            } catch (Exception eIn) {
                e.printStackTrace();
            }
            return 0.0f;
        }
    }

    public Long formatLong(Object obj) {
        try {
            return Long.parseLong(((String) obj).replace(",", "").trim().replace(" ", ""));
        } catch (Exception e) {
            try {

                if (String.valueOf(obj).length() != 0) {
                    System.err.println("NumberFormatErr==========>>>>>" + obj);
                }
            } catch (Exception eIn) {
                e.printStackTrace();
            }
            return 0L;
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
            e.printStackTrace();
            return null;
        }
    }

    public Boolean dateStrCompare(String srcDate, String targetDate, String regex, String compareRule) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(regex);
            long srcTimestamp = simpleDateFormat.parse(srcDate).getTime();
            long targetTimestamp = simpleDateFormat.parse(targetDate).getTime();
            switch (compareRule) {
                case ">":
                    return srcTimestamp > targetTimestamp;
                case "<":
                    return srcTimestamp < targetTimestamp;
                case "=":
                    return srcTimestamp == targetTimestamp;
                case ">=":
                    return srcTimestamp >= targetTimestamp;
                case "<=":
                    return srcTimestamp <= targetTimestamp;
                default:
                    return null;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getStrArrayOrg(String sep, String... columnValues) {
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
            e.printStackTrace();
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
            e.printStackTrace();
            return null;
        }
    }

    public LinkedList<String> releaseBatch(String batchData) {
        try {
            return this.gson.fromJson(batchData, LinkedList.class);
        } catch (Exception e) {
            e.printStackTrace();
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
            e.printStackTrace();
            return null;
        }
    }

    public String packBatch(String... datas) {
        try {
            return this.gson.toJson(datas);
        } catch (Exception e) {
            e.printStackTrace();
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
            e.printStackTrace();
            return null;
        }
    }

    public Row resultGetColumnsRow(Result result, String family, String... columnNames) {
        Row row = RowFactory.create(resultGetColumns(result, family, columnNames));
        return row != null ? row : null;
    }

    public ArrayList<String> resultGetColumnsCount(Result result, String family, String... columnNames) {
        ArrayList<String> columnValues = resultGetColumns(result, family, columnNames);
        if (columnValues == null) {
            return null;
        } else {
            columnValues.add("1");
            return columnValues;
        }
    }

    /**
     * Description
     * 该方法默认使用空串代替值
     * <p>
     * Example
     * <p>
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
            e.printStackTrace();
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


    private BatchGetterFY() {
        if (null != StaticNestedInstance.instance) {
            throw new RuntimeException();
        }
    }

    private static final class StaticNestedInstance {
        private static final BatchGetterFY instance = new BatchGetterFY();
    }

    public static BatchGetterFY getInstance() {
        return StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return StaticNestedInstance.instance;
    }
}
