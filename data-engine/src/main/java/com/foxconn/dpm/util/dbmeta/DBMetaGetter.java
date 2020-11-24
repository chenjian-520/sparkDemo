package com.foxconn.dpm.util.dbmeta;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * @author HS
 * @className DBMetaGetter
 * @description TODO
 * @date 2019/12/17 16:50
 */
public class DBMetaGetter implements Serializable, MetaGetterRegistry {

    private HashMap<String, DBMeta> dbMetas;

    private void initDBMetas() {
        MetaGetter.properties.forEach(new BiConsumer<Object, Object>() {
            @Override
            public void accept(Object key, Object value) {
                if (((String) key).startsWith("metafile.db")) {
                    Properties properties = new Properties();
                    try {
                        properties.load(DBMetaGetter.class.getClassLoader().getResourceAsStream("metafile/db" + "/" + ((String) value)));
                    } catch (IOException e) {
                    }
                    HashMap<String, HashMap<String, String>> datakeys = new HashMap<>();
                    properties.forEach(new BiConsumer<Object, Object>() {
                        @Override
                        public void accept(Object key, Object value) {
                            String keyStr = ((String) key);
                            String valueStr = ((String) value);
                            if (keyStr.matches("^db\\.[\\d]+\\..*")) {
                                String[] splitKeys = keyStr.split("\\.");
                                String preKey = splitKeys[0] + "." + splitKeys[1];
                                if (datakeys.get(preKey) != null) {
                                    datakeys.get(preKey).put(keyStr, valueStr);
                                } else {
                                    HashMap<String, String> buf = new HashMap<>();
                                    buf.put(keyStr, valueStr);
                                    datakeys.put(preKey, buf);
                                }
                            }
                        }
                    });
                    datakeys.forEach(new BiConsumer<String, HashMap<String, String>>() {
                        @Override
                        public void accept(String prekey, HashMap<String, String> dbGroup) {
                            String name = "";
                            String host = "";
                            String port = "";
                            String user = "";
                            String pass = "";
                            String db = "";
                            String url = "";
                            for (String k : dbGroup.keySet()) {
                                String[] ks = k.split("\\.");
                                String v = dbGroup.get(k);
                                switch (ks[ks.length - 1]) {
                                    case "name":
                                        name = v;
                                        break;
                                    case "host":
                                        host = v;
                                        break;
                                    case "port":
                                        port = v;
                                        break;
                                    case "user":
                                        user = v;
                                        break;
                                    case "pass":
                                        pass = v;
                                        break;
                                    case "db":
                                        db = v;
                                        break;
                                    case "url":
                                        url = v;
                                        break;
                                }
                            }
                            dbMetas.put(name, new DBMeta(name, host, port, user, pass, db, url, dbGroup));
                        }
                    });

                }
            }
        });
    }

    public DBMeta getDBMeta(String metaName) {
        return dbMetas.get(metaName);
    }


    public class DBMeta {
        public String name;
        public String host;
        public String port;
        public String user;
        public String pass;
        public String db;
        public String url;
        private HashMap<String, String> keys;

        public DBMeta(String name, String host, String port, String user, String pass, String db, String url, HashMap<String, String> keys) {
            this.name = name;
            this.host = host;
            this.port = port;
            this.user = user;
            this.pass = pass;
            this.db = db;
            this.url = url;
            this.keys = keys;
        }

        public String get(String k) {
            return this.keys.get(k);
        }
    }

    private DBMetaGetter() {
        if (null != StaticNestedInstance.instance) {
            throw new RuntimeException();
        } else {
            this.dbMetas = new HashMap<>();
            initDBMetas();
        }
    }

    private static final class StaticNestedInstance {
        private static final DBMetaGetter instance = new DBMetaGetter();
    }

    public static DBMetaGetter getInstance() {
        return StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return StaticNestedInstance.instance;
    }
}
