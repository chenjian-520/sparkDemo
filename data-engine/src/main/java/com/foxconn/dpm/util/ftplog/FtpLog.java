package com.foxconn.dpm.util.ftplog;

import com.foxconn.dpm.util.MetaGetterRegistry;

/**
 * Description 对外接口
 * Example
 * Author HS
 * Version
 * Time 9:36 2019/12/12
 */
public interface FtpLog extends MetaGetterRegistry {
    public boolean info(String message) ;
    public void setIsAsynchronousLog(boolean isAsynchronousLog);
    public boolean isFtpAlive();
    public boolean initFtp();
    public void renameLog(String newFileName);
}
