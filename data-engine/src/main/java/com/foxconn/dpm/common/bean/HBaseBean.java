package com.foxconn.dpm.common.bean;

import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;

import java.io.Serializable;

/**
 * @Author HY
 * @Date 2020/6/30 13:34
 * @Description TODO
 */
public abstract class HBaseBean implements Serializable {

    public abstract String getBaseRowKey();

    public String getSalt() {
        ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
        return consistentHashLoadBalance.selectNode(getBaseRowKey());
    }
}
