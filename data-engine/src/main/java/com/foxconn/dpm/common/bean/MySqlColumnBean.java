package com.foxconn.dpm.common.bean;

import com.foxconn.dpm.common.enums.MySqlDataTypes;
import lombok.Data;

/**
 * @Author HY
 * @Date 2020/7/6 12:35
 * @Description TODO
 */
@Data
public class MySqlColumnBean {

    private String fieldName;

    private String columnName;

    private MySqlDataTypes mySqlDataType;

}
