package com.foxconn.dpm.common.bean;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

/**
 * @Author HY
 * @Date 2020/7/16 14:02
 * @Description TODO
 */
@Data
@Accessors(chain = true)
public class KafkaMessage {

    private String tableName;

    private String family;

    private Map<String, Object> data;

}
