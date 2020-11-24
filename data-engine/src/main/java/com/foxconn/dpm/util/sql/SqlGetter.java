package com.foxconn.dpm.util.sql;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;
import org.ho.yaml.Yaml;
import scala.Tuple2;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
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
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < sqlDirs.length - 2; i++) {
                            sb.append(sqlDirs[i]);
                            if (i < sqlDirs.length - 3) {
                                sb.append("/");
                            }
                        }
                        String sqlDir = sb.toString();
                        try {
                            if (((String) o2).endsWith(".sql")) {
                                initSqlFile(sqlDir, (String) o, ((String) o2));
                            }
                            if (((String) o2).endsWith(".yml")) {
                                initYmlFile(sqlDir, ((String) o), (String) o2);
                            }
                        } catch (Exception e) {
                        }

                    }
                });
            }
        }
    }

    private void initYmlFile(String sqlDir, String o, String o2) {
        try {

            HashMap<String, String> sqlYmls = (HashMap<String, String>) new Yaml().load(SqlGetter.class.getClassLoader().getResourceAsStream(sqlDir + "/" + o2));
            if (sqlYmls == null) {
                return;
            } else {
                sqlYmls.forEach(new BiConsumer<String, String>() {
                    @Override
                    public void accept(String name, String sql) {
                        sqlMap.put(name, cleanSql(sql));
                    }
                });
            }
        } catch (Exception e) {
            return;
        }
    }


    private void initSqlFile(String sqlDir, String o, String o2) {


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
            sqlMap.put(((String) o2), cleanSql(sql));
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        } finally {
            try {
                bis.close();
                bos.close();
            } catch (IOException e) {
            }
        }
    }

    public ArrayList<String> getRuleTempleSqls(String sqlKey, String ruleCode, Object ... ruleKeyParams) {
        try {
            switch (ruleCode) {
                case "DEFAULT":
                    return getDeftTempleSqls(Get(sqlKey), new Tuple2[]{
                            new Tuple2<>("$ETL_TIME$", new String[0]),
                            new Tuple2<>("$FORMAT_TIME_RANGE$", new String[]{"work_dt","calculateYearWeek"})
                    });
                default:
                    return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * ====================================================================
     * 描述:
     * <p>
     * 按照指定规则进行默认sql模板构造
     * <p>
     * 参数:params
     * 0: 规则key
     * 1: SQL
     * 2 ...: 替换内容
     * <p>
     * 返回值: SQL集合
     * ====================================================================
     */
    public ArrayList<String> getDeftTempleSqls(String sql, Tuple2<String, String[]>[] ruleKeyParams) {
        ArrayList<String> rsSql = new ArrayList<>();
        if (ruleKeyParams == null || ruleKeyParams.length == 0) {
            rsSql.add(sql);
            return rsSql;
        }
        Stack<String> inStack = new Stack<>();
        inStack.push(sql);
        for (int i = 0; i < ruleKeyParams.length; i++) {
            deftStructCase(ruleKeyParams[i], inStack);
        }
        rsSql.addAll(inStack);
        return rsSql;
    }

    public void deftStructCase(Tuple2<String, String[]> kV, Stack<String> inStack) {
        Stack<String> tempStack = new Stack<>();
        switch (kV._1) {
            case "$ETL_TIME$":
                String etl_time = String.valueOf(System.currentTimeMillis());
                while (!inStack.isEmpty()) {
                    tempStack.push(inStack.pop().replace("$ETL_TIME$", etl_time));
                }
                break;
            case "$FORMAT_TIME_RANGE$":
                while (!inStack.isEmpty()) {
                    String sql = inStack.pop();
                    String dateColumnName = kV._2[0];
                    String weekCalculateUDFName = kV._2[1];
                    tempStack.push(sql.replace("$FORMAT_TIME_RANGE$", "cast( "+ dateColumnName +" as VARCHAR(32))"));
                    tempStack.push(sql.replace("$FORMAT_TIME_RANGE$", weekCalculateUDFName + "( "+ dateColumnName +" )"));
                    tempStack.push(sql.replace("$FORMAT_TIME_RANGE$", "cast(from_unixtime(to_unix_timestamp( "+ dateColumnName +" , 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER)"));
                    tempStack.push(sql.replace("$FORMAT_TIME_RANGE$", "cast(concat(year( "+ dateColumnName +" ), quarter( "+ dateColumnName +" )) AS INTEGER)"));
                    tempStack.push(sql.replace("$FORMAT_TIME_RANGE$", "year( "+ dateColumnName +" )"));
                }
                break;
        }
        inStack.addAll(tempStack);
    }

    private String cleanSql(String prepareCleanSql) {
        /*
         * ====================================================================
         * 描述:
         *      去除YAML文件中的起始和结束段
         * ====================================================================
         */
        prepareCleanSql = prepareCleanSql.trim().replace("\n", " ");
        if (prepareCleanSql.startsWith("\"")) {
            prepareCleanSql = prepareCleanSql.substring(1, prepareCleanSql.length());
        }
        if (prepareCleanSql.endsWith("\"")) {
            prepareCleanSql = prepareCleanSql.substring(0, prepareCleanSql.length() - 1);
        }
        if (prepareCleanSql.endsWith(";")) {
            prepareCleanSql = prepareCleanSql.substring(0, prepareCleanSql.length() - 1);
        }
        return prepareCleanSql;
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

