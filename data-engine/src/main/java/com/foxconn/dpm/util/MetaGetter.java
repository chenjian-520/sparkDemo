package com.foxconn.dpm.util;

import com.foxconn.dpm.util.autobuild.AutoCreateDeftTableBusiness;
import com.foxconn.dpm.util.autobuild.AutoCreateProjectSchedule;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.dbmeta.DBMetaGetter;
import com.foxconn.dpm.util.ftplog.FtpLog;
import com.foxconn.dpm.util.ftplog.LogStoreFtp;
import com.foxconn.dpm.util.sql.SqlGetter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author HS
 * @className MetaGetter
 * @description TODO
 * @date 2019/12/16 12:46
 */
public class MetaGetter {
    public static Properties properties = new Properties();

    static {
        try {
            properties.load(MetaGetter.class.getClassLoader().getResourceAsStream("metafile.properties"));
        } catch (IOException e) {
        }
    }

    private static HashMap<Class<? extends MetaGetterRegistry>, Object> tools = new HashMap<>();

    public static void main(String[] args) {
    }


    static {
        MetaGetter.tools.put(BatchGetter.class, BatchGetter.getInstance());
        MetaGetter.tools.put(SqlGetter.class, SqlGetter.getInstance());
        MetaGetter.tools.put(BeanGetter.class, BeanGetter.getInstance());
        MetaGetter.tools.put(DBMetaGetter.class, DBMetaGetter.getInstance());
    }

    public static SqlGetter getSql() {
        return (SqlGetter) tools.get(SqlGetter.class);
    }

    public static FtpLog getFtpLog() {
        return getFtpLog(null);
    }

    public static FtpLog getFtpLog(String fileName, boolean... isReInit) {
        if (fileName != null) {
            FtpLog instance = LogStoreFtp.getInstance(fileName, isReInit[0]);
            MetaGetter.tools.put(FtpLog.class, instance);
        }
        return (FtpLog) tools.get(FtpLog.class);
    }

    public static BeanGetter getBeanGetter() {
        return (BeanGetter) tools.get(BeanGetter.class);
    }

    public static BatchGetter getBatchGetter() {
        return (BatchGetter) tools.get(BatchGetter.class);
    }

    public static DBMetaGetter getDBMetaGetter() {
        return (DBMetaGetter) tools.get(DBMetaGetter.class);
    }
}
