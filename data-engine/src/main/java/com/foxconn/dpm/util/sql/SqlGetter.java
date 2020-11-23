package com.foxconn.dpm.util.sql;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;
import org.ho.yaml.Yaml;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.function.BiConsumer;

/**
 * @author HS
 * @className SqlGetter
 * @description TODO
 * @date 2019/12/15 20:11
 */
public class SqlGetter implements Serializable, MetaGetterRegistry {
    private HashMap<String, String> sqlMap;


    public String Get(String sqlName) {
        return this.sqlMap.get(sqlName);
    }

    public static void main(String[] args) {
        final SqlGetter instance = getInstance();
        System.out.println(instance);
    }

    private SqlGetter() {
        if (null != StaticNestedInstance.instance) {
            throw new RuntimeException();
        } else {
            if (!MetaGetter.properties.isEmpty()) {
                this.sqlMap = new HashMap<>();
                MetaGetter.properties.forEach(new BiConsumer<Object, Object>() {
                    @Override
                    public void accept(Object o, Object o2) {
                        if (!(((String) o).startsWith("metafile.sqls"))) {
                            return;
                        }
                        String[] sqlDirs = ((String) o).split("\\.");
                        String sqlDir = sqlDirs[0] + "/" + sqlDirs[1] + "/" + sqlDirs[2];

                        if (((String) o2).endsWith(".sql")){
                            initSqlFile(sqlDir, (String) o, ((String) o2));
                        }
                        if (((String) o2).endsWith(".yml")){
                            initYmlFile(sqlDir, ((String) o), (String) o2);
                        }
                    }
                });
            }
        }
    }

    private void initYmlFile(String sqlDir, String o , String o2){
        try {

            HashMap<String, String> sqlYmls = (HashMap<String, String>) new Yaml().load(SqlGetter.class.getClassLoader().getResourceAsStream(sqlDir + "/" + o2));
            if (sqlYmls == null){
                return;
            }else{
                sqlYmls.forEach(new BiConsumer<String, String>() {
                    @Override
                    public void accept(String name, String sql) {
                        sqlMap.put(name, sql.replace("\n", " "));
                    }
                });
            }
        }catch (Exception e){
            return;
        }
    }

    private void initSqlFile(String sqlDir, String o, String o2){


        URL resource = SqlGetter.class.getClassLoader().getResource(sqlDir + "/" + ((String) o2));
        if (resource == null) {
            return;
        }
        ByteArrayOutputStream bao = null;
        BufferedOutputStream bos = null;
        BufferedInputStream bis = null;
        InputStream is = null;
        try {
            is = resource.openStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (is == null) {
            return;
        }
        try {
            bao = new ByteArrayOutputStream();
            bos = new BufferedOutputStream(bao);
            bis = new BufferedInputStream(is);
            int len = -1;
            byte[] buf = new byte[1024];
            while ((len = bis.read(buf)) != -1) {
                bos.write(buf, 0, len);
            }
            bos.flush();
            String sql = new String(bao.toByteArray(), "UTF-8");
            if (sql == null || sql == "") {
                return;
            }
            if (sql.substring(sql.length() - 1, sql.length()).equals(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            sqlMap.put(((String) o2), sql.replace("\n", " "));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static final class StaticNestedInstance {
        private static final SqlGetter instance = new SqlGetter();
    }

    public static SqlGetter getInstance() {
        return StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return StaticNestedInstance.instance;
    }
}

