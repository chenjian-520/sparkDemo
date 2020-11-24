package com.foxconn.dpm.sprint3.ods_dwd.udf;

import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

/**
 * @author HS
 * @className GenerateRowKey
 * @description TODO
 * @date 2020/5/20 9:29
 */
public class GenerateRowKey implements UDF2<String, Integer, String> {
    @Override
    public String call(String baseRowKey, Integer nodeCount) throws Exception {
        return (new ConsistentHashLoadBalance(nodeCount).selectNode(baseRowKey)).concat(":").concat(baseRowKey);
    }
}
