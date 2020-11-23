package com.foxconn.dpm.util.ftplog;

import com.foxconn.dpm.util.MetaGetter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * Description FTPLog对象真实类
 * Example
 * Author HS
 * Version
 * Time 9:36 2019/12/12s
 */
public class LogStoreFtp implements Serializable, FtpLog {

    /*===============================================================================================================*/
    //真实业务逻辑区
    /*===============================================================================================================*/


    @Override
    public boolean info(String message) {


        try {
            if (!this.writeIsAppend) {
                final FTPFile[] ftpFiles = this.ftpClientInstance.listFiles(this.logFileName);
                if (ftpFiles != null && ftpFiles.length > 0) {
                    this.writeIsAppend = true;
                }
            }
            BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream((this.writeIsAppend ? "\r\n" + message : message).getBytes()));
            return (this.writeIsAppend ? this.ftpClientInstance.appendFile(this.ftpRootDir + "/" + this.logFileName, bis) : this.ftpClientInstance.storeFile(this.ftpRootDir + "/" + this.logFileName, bis));
        } catch (IOException e) {
            if (StaticNestedInstance.proxyInstance.messages != null) {
                StaticNestedInstance.proxyInstance.messages.add(e.getStackTrace().toString());
            } else {
                e.printStackTrace();
            }
        }
        return false;
    }

    /*===============================================================================================================*/

    /**
     * Description FTPLog对象代理类
     * Example
     * Author HS
     * Version
     * Time 9:37 2019/12/12
     */
    private class LogStoreFtpProxy implements Serializable, FtpLog {
        private InnerRunnable storeFailMsgInstance = null;
        private Thread storeFailMsgInstanceThread = null;
        private LinkedList<String> messages = new LinkedList<>();

        /*===============================================================================================================*/
        //代理业务逻辑区
        /*===============================================================================================================*/

        @Override
        public boolean info(String message) {
            if (isAsynchronousLog || !StaticNestedInstance.realInstance.info(message)) {
                if (!StaticNestedInstance.proxyInstance.storeFailMsgInstanceThread.isAlive()) {
                    StaticNestedInstance.proxyInstance.initStoreThread();
                }
                this.messages.add(System.currentTimeMillis() + "=============>>" + message);
                return false;
            } else {
                System.out.println("=============>>Log Success<<=============");
                return true;
            }
        }

        @Override
        public void setIsAsynchronousLog(boolean isAsynchronousLog) {
            StaticNestedInstance.realInstance.setIsAsynchronousLog(isAsynchronousLog);
        }

        @Override
        public boolean isFtpAlive() {
            return StaticNestedInstance.realInstance.isFtpAlive();
        }

        public boolean initFtp() {
            return StaticNestedInstance.realInstance.initFtp();
        }

        @Override
        public void renameLog(String newFileName) {
            StaticNestedInstance.realInstance.renameLog(newFileName);
        }

        /*===============================================================================================================*/
        private LogStoreFtpProxy() {
            if (null != StaticNestedInstance.realInstance) {
                throw new RuntimeException("请不要重复声明实例!");
            } else {
                this.storeFailMsgInstanceThread = new Thread(this.storeFailMsgInstance = new InnerRunnable());
                this.storeFailMsgInstanceThread.start();
            }
        }

        private Object readResolve() throws ObjectStreamException {
            return StaticNestedInstance.proxyInstance;
        }

        private class InnerRunnable implements Runnable {
            public boolean isRun = true;
            private int releaseCount = 0;

            @Override
            public void run() {
                while (this.isRun) {
                    if (StaticNestedInstance.proxyInstance.messages.size() > 0 && StaticNestedInstance.realInstance.info(StaticNestedInstance.proxyInstance.messages.getFirst())) {
                        StaticNestedInstance.proxyInstance.messages.removeFirst();
                    }
                    if (StaticNestedInstance.proxyInstance.messages.size() == 0) {
                        if (releaseCount > 15) {
                            this.isRun = false;
                        } else {
                            releaseCount++;
                        }
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        StaticNestedInstance.realInstance.info(e.getMessage());
                    }
                }
            }
        }

        private void initStoreThread() {
            this.storeFailMsgInstance.isRun = false;
            if (this.storeFailMsgInstance != null) {
                this.storeFailMsgInstanceThread = new Thread(this.storeFailMsgInstance = new InnerRunnable());
            }
        }
    }


    @Override
    public void finalize() {
        if (StaticNestedInstance.proxyInstance != null && StaticNestedInstance.proxyInstance.storeFailMsgInstance != null) {
            StaticNestedInstance.proxyInstance.storeFailMsgInstance.isRun = false;
        }
        if (this.ftpClientInstance.isConnected() && this.ftpClientInstance.isAvailable()) {
            try {
                this.ftpClientInstance.logout();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * ====================================================================
     * 描述: 初始化ftp日志对象
     * ====================================================================
     */

    private FTPClient ftpClientInstance;
    private String logFileName;
    private boolean writeIsAppend;
    private boolean isAsynchronousLog = true;
    private String ftpHost;
    private Integer ftpPort;
    private String ftpUser;
    private String ftpPass;
    private String ftpRootDir;
    private static HashMap<String, HashMap<String, String>> ftpProperties;

    private LogStoreFtp() {
        if (null != StaticNestedInstance.realInstance) {
            throw new RuntimeException("请不要重复声明实例!");
        } else {
            synchronized (this) {
                if (StaticNestedInstance.proxyInstance == null) {
                    StaticNestedInstance.proxyInstance = new LogStoreFtpProxy();
                }
                ftpProperties = new HashMap<>();
                this.isAsynchronousLog = false;
            }
            initPropertiesFile();
        }
    }

    private static final class StaticNestedInstance {
        private static LogStoreFtpProxy proxyInstance = null;
        private static final LogStoreFtp realInstance = new LogStoreFtp();
    }

    private Object readResolve() throws ObjectStreamException {
        return StaticNestedInstance.realInstance;
    }

    private void initPropertiesFile() {
        MetaGetter.properties.forEach(new BiConsumer<Object, Object>() {
            @Override
            public void accept(Object k, Object v) {
                String key = (String) k;
                String[] ks = key.split("\\.");
                Properties properties = new Properties();
                try {
                    if (!(((String) k).startsWith("metafile.ftp"))) {
                        return;
                    }
                    properties.load(LogStoreFtp.class.getClassLoader().getResourceAsStream("metafile/ftp/" + ((String) v)));
                    properties.forEach(new BiConsumer<Object, Object>() {
                        @Override
                        public void accept(Object key, Object value) {
                            String keyStr = ((String) key);
                            String valueStr = ((String) value);
                            if (keyStr.matches("^ftp\\.[\\d]+\\..*")) {
                                String[] splitKeys = keyStr.split("\\.");
                                String preKey = splitKeys[0] + "." + splitKeys[1];
                                if (ftpProperties.get(preKey) != null) {
                                    ftpProperties.get(preKey).put(keyStr, valueStr);
                                } else {
                                    HashMap<String, String> buf = new HashMap<>();
                                    buf.put(keyStr, valueStr);
                                    ftpProperties.put(preKey, buf);
                                }
                            }
                        }
                    });
                    HashMap<String, HashMap<String, String>> ftpPropertiesTmep = new HashMap<>();
                    ftpProperties.forEach(new BiConsumer<String, HashMap<String, String>>() {
                        @Override
                        public void accept(String prekey, HashMap<String, String> ftpGroup) {
                            for (String ftpNameK : ftpGroup.keySet()) {
                                if (ftpNameK.endsWith(".name")) {
                                    String ftpName = ftpGroup.get(ftpNameK);
                                    ftpGroup.remove(ftpName);
                                    ftpPropertiesTmep.put(ftpName, ftpGroup);

                                }
                            }
                        }
                    });
                    ftpProperties = ftpPropertiesTmep;
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });
    }

    public static FtpLog getInstance(String ftpName, boolean ... isReInit) {
        if (ftpName == null || ftpName == "") {
            return null;
        }
        if (isReInit != null  || isReInit.length != 0 || isReInit[0]){initFtp(ftpName);}
        return StaticNestedInstance.proxyInstance;
    }

    public boolean isFtpAlive() {
        try {
            if (this.ftpClientInstance != null && this.ftpClientInstance.isConnected() && this.ftpClientInstance.isAvailable() && this.ftpClientInstance.listDirectories("/") != null) {
                return true;
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

    @Override
    public boolean initFtp() {
        return StaticNestedInstance.realInstance.initFtpInstance();
    }

    @Override
    public void renameLog(String newFileName) {
        this.logFileName = newFileName;
    }

    public void setIsAsynchronousLog(boolean isAsynchronousLog) {
        StaticNestedInstance.realInstance.isAsynchronousLog = isAsynchronousLog;
    }

    private static boolean initFtp(String ftpName) {
        if (ftpName == null || ftpName == "") {
            return false;
        }
        LogStoreFtp realInstance = StaticNestedInstance.realInstance;
        ftpProperties.get(ftpName).forEach(new BiConsumer<String, String>() {
            @Override
            public void accept(String k, String v) {
                String[] split = v.split("\\.");
                switch (split[split.length - 1]) {
                    case "fileName":
                        realInstance.logFileName = v;
                        break;
                    case "host":
                        realInstance.ftpHost = v;
                        break;
                    case "port":
                        realInstance.ftpPort = Integer.valueOf(v);
                        break;
                    case "user":
                        realInstance.ftpUser = v;
                        break;
                    case "pass":
                        realInstance.ftpPass = v;
                        break;
                    case "ftpRootDir":
                        realInstance.ftpRootDir = v;
                        break;
                }
            }
        });
        StaticNestedInstance.realInstance.initFtpInstance();
        return StaticNestedInstance.realInstance != null && StaticNestedInstance.proxyInstance.isFtpAlive() ? true : false;
    }

    private boolean initFtpInstance() {
        Long begin_time = System.currentTimeMillis();
        this.ftpClientInstance = new FTPClient();
        boolean flag = true;
        while (flag) {
            try {
                this.ftpClientInstance.connect(this.ftpHost, this.ftpPort);
                if ((ftpClientInstance != null && ftpClientInstance.isAvailable())) {
                    this.ftpClientInstance.login(this.ftpUser, this.ftpPass);
                    if (this.ftpClientInstance != null && this.ftpClientInstance.isAvailable() && this.ftpClientInstance.listDirectories("/") != null) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    Thread.sleep(20);
                }
                if (System.currentTimeMillis() - begin_time > 30000) {
                    flag = false;
                }
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

}
