package com.foxconn.dpm.util.hbaseread;

import com.foxconn.dpm.util.MetaGetterRegistry;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.jcodings.util.Hash;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Stack;

/**
 * @author HS
 * @className HGetter
 * @description TODO
 * @date 2019/12/24 16:04
 */
public class HGetter implements Serializable, MetaGetterRegistry {

    public static Integer SALT_READ_THREAD_COUNTER = 0;

    public JavaRDD<Result> saltRead(String tableName, String unSaltStartRowKey, String unSaltEndRowKey, boolean isReadOldTable, String saltSep, int region, String... prefixRowKey) {
        int defaultRegionCount = region <= 0 ? 20 : region;
        JavaRDD<Result> resultJavaRDD = null;
        ArrayList<JavaRDD<Result>> scanResultArray = new ArrayList<>();
        try {
            /**
             * Description
             *   For 开启线程，
             */
            for (int i = 0; i <= defaultRegionCount; i++) {
                Integer rg = new Integer(i);
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        ++HGetter.SALT_READ_THREAD_COUNTER;
                        Scan scan = null;
                        StringBuilder sb = null;
                        scan = new Scan();
                        if (prefixRowKey != null && prefixRowKey.length == 1) {
                            scan.setRowPrefixFilter(prefixRowKey[0].getBytes());
                        }
                        sb = new StringBuilder();
                        String salt = (rg <= 9 ? sb.append("0").append(rg) : sb.append(rg)).toString();
                        scan.withStartRow((salt + saltSep + unSaltStartRowKey).getBytes(), true);
                        scan.withStopRow((salt + saltSep + unSaltEndRowKey).getBytes(), true);
                        try {
                            JavaRDD<Result> threadResultRDD = DPHbase.rddRead(tableName, scan, isReadOldTable);
                            if (threadResultRDD != null){
                                scanResultArray.add(threadResultRDD);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }finally {
                            --HGetter.SALT_READ_THREAD_COUNTER;
                        }
                    }
                }).start();
            }
            Thread.sleep(30);
            while (HGetter.SALT_READ_THREAD_COUNTER != 0){
                Thread.yield();
            }
            if (scanResultArray == null || scanResultArray.size() == 0){return null;}
            for (JavaRDD<Result> scanRDD : scanResultArray) {
                if (resultJavaRDD == null) {
                    resultJavaRDD = scanRDD;
                } else {
                    if (resultJavaRDD == null) {
                        return null;
                    } else {
                        resultJavaRDD = resultJavaRDD.union(scanRDD);
                    }
                }
            }
            return resultJavaRDD;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public JavaRDD<Result> commonRead(String tableName, String startRowKey, String endRowKey, boolean isReadOldTable, String... prefixRowKey) throws Exception {
        Scan scan = new Scan();
        if (prefixRowKey != null && prefixRowKey.length == 1) {
            scan.setRowPrefixFilter(prefixRowKey[0].getBytes());
        }
        scan.withStartRow(startRowKey.getBytes(), true);
        scan.withStopRow(endRowKey.getBytes(), true);

        return DPHbase.rddRead(tableName, scan, isReadOldTable);
    }

    private HGetter() {
        if (null != StaticNestedInstance.instance) {
            throw new RuntimeException();
        }
    }

    private static final class StaticNestedInstance {
        private static final HGetter instance = new HGetter();
    }

    public static HGetter getInstance() {
        return StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return StaticNestedInstance.instance;
    }
}