package com.foxconn.dpm.sprint5.dws_ads.bean;

import com.foxconn.dpm.common.annotation.MySqlColumn;
import com.foxconn.dpm.common.annotation.MySqlTable;
import com.foxconn.dpm.common.enums.MySqlDataTypes;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.Date;

/**
 * issueCategory 配置表 .
 *
 * @className: SysIssuetrackingCategory
 * @author: ws
 * @date: 2020/7/16 16:56
 * @version: 1.0.0
 */
@Data
@Accessors(chain = true)
public class SysIssuetrackingCategory implements Serializable {

    private String id;

    private String name;

}
