package com.foxconn.dpm;

import cn.hutool.db.Entity;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.handler.NumberHandler;
import cn.hutool.db.sql.SqlExecutor;
import com.foxconn.dpm.common.annotation.MySqlColumn;
import com.foxconn.dpm.common.annotation.MySqlTable;
import com.foxconn.dpm.common.bean.MySqlColumnBean;
import com.foxconn.dpm.common.enums.MySqlDataTypes;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author HS
 * @className DpMysql
 * @description TODO
 * @date 2020/4/23 9:15
 */
public class DpMysql extends DPMysql {


    static Driver mysqldriver = null;
    static Properties dbpro = new Properties();
    static Connection con = null;

    // 正式环境
    static String dburl = "jdbc:mysql://10.124.160.28:3306/dp_ads?useSSL=false&amp;allowMultiQueries=true;";
    static String dbuser = "dp_ads";
    static String dbPassword = "Foxconn!@34";

    // 测试环境
//    static String dburl = "jdbc:mysql://10.60.136.155:3306/dp_ads?useSSL=false&amp;allowMultiQueries=true;";
//    static String dbuser = "root";
//    static String dbPassword = "Foxconn1@#";

    public static void main(String[] args) throws SQLException {
        System.out.println(mysqldriver.connect(dburl, dbpro));
    }
    static {
        try {
            mysqldriver = (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        dbpro = new Properties();
        dbpro.put("user", dbuser);
        dbpro.put("password", dbPassword);
        getConnection();
    }

    private static void getConnection() {
        try {
            if (con == null || con.isClosed()){
                con = mysqldriver.connect(dburl, dbpro);
                con.setAutoCommit(true);
            }
//            System.out.println(con);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void closeConn() {

        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 根据 sql 执行查询 .
     * @param sql
     * @param params
     * @author ws
     * @date 2020/7/16 17:42
     * @return java.util.List<cn.hutool.db.Entity>
     **/
    public static List<Entity> queryBySql(String sql, Object... params) {
        try {
            getConnection();
            List<Entity> list = SqlExecutor.query(con, sql, new EntityListHandler(), params);
            return list;
        } catch (Exception e) {
            throw new RuntimeException("sql执行异常" + sql + e.getMessage());
        } finally {
            closeConn();
        }

    }

    /**
     * 根据 sql 执行查询 .
     * @param sql
     * @param params
     * @author ws
     * @date 2020/7/16 17:42
     * @return java.util.List<cn.hutool.db.Entity>
     **/
    public static Long countBySql(String sql, Object... params) {
        try {
            getConnection();
            Number number = SqlExecutor.query(con, sql, new NumberHandler(), params);
            return number.longValue();
        } catch (Exception e) {
            throw new RuntimeException("sql执行异常" + sql + e.getMessage());
        } finally {
            closeConn();
        }

    }


    /**
     * 根据主键删除数据 .
     * @param javaRdd
     * @param classType
     * @author ws
     * @date 2020/7/16 17:42
     * @return java.util.List<cn.hutool.db.Entity>
     **/
    public static  <T> int deleteByPrimaryKey(JavaRDD<T> javaRdd, Class<T> classType) {

        SparkSession sparkSession = DPSparkApp.getSession();
        Dataset<Row> dataSet = sparkSession.createDataFrame(javaRdd, classType);
        Dataset<Row> rows = sparkSession.createDataFrame(dataSet.javaRDD(),  dataSet.schema());
        List<Row> collect = rows.javaRDD().collect();
        if(collect == null || collect.isEmpty()) {
            return 0;
        }

        MySqlTable annotation = classType.getAnnotation(MySqlTable.class);
        if(annotation == null) {
            throw new RuntimeException("无法获取表名 :" + classType.getName());
        }
        String tableName = annotation.table();

        Field[] fields = classType.getDeclaredFields();
        MySqlColumnBean bean = null;
        for (Field field : fields) {
            field.setAccessible(true);
            MySqlColumn columnAnnotation = field.getAnnotation(MySqlColumn.class);
            if(columnAnnotation != null && columnAnnotation.primaryKey()) {
                bean = new MySqlColumnBean();
                bean.setFieldName(field.getName());
                bean.setColumnName(columnAnnotation.column());
                bean.setMySqlDataType(columnAnnotation.dataType());
            }
        }
        if(bean == null) {
            throw new RuntimeException("MySqlColumn注解上位配置primaryKey :" + classType.getName());
        }

        String sql = "DELETE FROM " + tableName + " WHERE " + bean.getColumnName() + " IN (";

        StringBuilder sb = new StringBuilder(sql);

        for(int i=0; i<collect.size(); i++) {
            Row row = collect.get(i);
            MySqlDataTypes dataType = bean.getMySqlDataType();
            String id =  row.getAs(bean.getFieldName());
            if (dataType.equals(MySqlDataTypes.STRING)) {
                id = "'" + id + "'";
            }

            if(i == collect.size() - 1) {
                sb.append(id ).append(") ");
            }else {
                sb.append(id).append(",");
            }
        }
        sql = sb.toString();


        try {
            getConnection();
            int count = SqlExecutor.execute(con, sql);
            System.out.println("删除数据：" + count);
            return count;
        } catch (Exception e) {
            throw new RuntimeException("sql执行异常" + sql + e.getMessage());
        } finally {
            closeConn();
        }
    }


    public static <T> void insertData(JavaRDD<T> javaRdd, Class<T> classType) {
        getConnection();
        SparkSession sparkSession = DPSparkApp.getSession();
        Dataset<Row> dataSet = sparkSession.createDataFrame(javaRdd, classType);
        Dataset<Row> rows = sparkSession.createDataFrame(dataSet.javaRDD(),  dataSet.schema());
        List<Row> collect = rows.javaRDD().collect();


        MySqlTable annotation = classType.getAnnotation(MySqlTable.class);
        if(annotation == null) {
            throw new RuntimeException("无法获取表名 :" + classType.getName());
        }
        String tableName = annotation.table();

        Field[] fields = classType.getDeclaredFields();
        List<MySqlColumnBean> columnList = new ArrayList<>(16);

        for (Field field : fields) {
            MySqlColumn columnAnnotation = field.getAnnotation(MySqlColumn.class);
            if(columnAnnotation == null) {
                continue;
            }

            // 过滤不需要插入对的字段
            if(!columnAnnotation.insert()) {
                continue;
            }

            MySqlColumnBean bean = new MySqlColumnBean();
            bean.setFieldName(field.getName());
            bean.setColumnName(columnAnnotation.column());
            bean.setMySqlDataType(columnAnnotation.dataType());
            columnList.add(bean);
        }


        try {

            StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + "(  ");
            StringBuilder values =  new StringBuilder("values ( ");

            for(int i=0; i<columnList.size(); i++) {
                String key = columnList.get(i).getColumnName();
                if(i == columnList.size() - 1) {
                    sql.append(key).append(" ) ");
                    values.append("? ) ");
                }else {
                    sql.append(key).append(", ");
                    values.append("?, ");
                }
            }

            String insertSql = sql.append(values).toString();
            try (PreparedStatement preparedStatement = con.prepareStatement(insertSql)) {
                AtomicInteger count = new AtomicInteger(0);

                for (Row row : collect) {

                    for(int i=0; i<columnList.size(); i++) {
                        MySqlColumnBean bean = columnList.get(i);
                        String fieldName = bean.getFieldName();
                        MySqlDataTypes dataType = bean.getMySqlDataType();

                        if (dataType.equals(MySqlDataTypes.INTEGER)) {
                            preparedStatement.setInt(i + 1, (Integer) row.getAs(fieldName));
                        } else if (dataType.equals(MySqlDataTypes.BOOLEAN)) {
                            preparedStatement.setBoolean(i + 1, (Boolean) row.getAs(fieldName));
                        } else if (dataType.equals(MySqlDataTypes.LONG)) {
                            preparedStatement.setLong(i + 1, (Long) row.getAs(fieldName));
                        } else if (dataType.equals(MySqlDataTypes.DOUBLE)) {
                            preparedStatement.setDouble(i + 1, (Double) row.getAs(fieldName));
                        } else if (dataType.equals(MySqlDataTypes.FLOAT)) {
                            preparedStatement.setFloat(i + 1, (Float) row.getAs(fieldName));
                        } else if (dataType.equals(MySqlDataTypes.SHORT)) {
                            preparedStatement.setShort(i + 1, (Short) row.getAs(fieldName));
                        } else {
                            preparedStatement.setString(i + 1, (String) row.getAs(fieldName));
                        }
                    }
                    preparedStatement.addBatch();
                    if (count.addAndGet(1) >= 2000) {
                        preparedStatement.executeBatch();
                        System.out.println("执行成功条数:" + count.get());
                        count.set(0);
                    }
                }

                preparedStatement.executeBatch();
                System.out.println("执行成功条数:" + count.get());
            }
        } catch (Exception var24) {
            throw new RuntimeException("插入数据库" + tableName + "异常：" + var24.getMessage());
        } finally {
            closeConn();
        }
    }

    /**
     * 插入或更新 .
     * @param javaRdd
     * @param classType
     * @author ws
     * @date 2020/7/22 14:24
     * @return void
     **/
    public static <T> void insertOrUpdateData(JavaRDD<T> javaRdd, Class<T> classType) {
        deleteByPrimaryKey(javaRdd, classType);
        insertData(javaRdd, classType);
    }


    public static void commonOdbcWriteBatch(String mysqltablename, JavaRDD<Row> insertdata, HashMap<String, StructField> dbcolums, StructType rowAgeNameSchema) {
        getConnection();
        SparkSession sparkSession = DPSparkApp.getSession();
        Dataset<Row> insertdataDs = sparkSession.createDataFrame(insertdata, rowAgeNameSchema);
        List<Row> collect = insertdataDs.javaRDD().collect();


        try {

            String sql = "INSERT INTO " + mysqltablename + "(  ";
            String values = "values ( ";
            Iterator iter = dbcolums.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String key = entry.getKey().toString();
                if (!iter.hasNext()) {
                    sql = sql + key + " ) ";
                    values = values + "? ) ";
                } else {
                    sql = sql + key + ",";
                    values = values + "?,";
                }
            }

            sql = sql + values;
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            AtomicInteger cout = new AtomicInteger(0);

            for (Row row : collect) {
                Row p = row;
                //        System.out.println(p);
                iter = dbcolums.entrySet().iterator();

                for (int i = 0; iter.hasNext(); ++i) {
                    Map.Entry ent = (Map.Entry) iter.next();
                    StructField structField = (StructField) ent.getValue();
                    if (structField.dataType().equals(DataTypes.IntegerType)) {
                        preparedStatement.setInt(i + 1, (Integer) p.getAs(ent.getKey().toString()));
                    } else if (structField.dataType().equals(DataTypes.BooleanType)) {
                        preparedStatement.setBoolean(i + 1, (Boolean) p.getAs(ent.getKey().toString()));
                    } else if (structField.dataType().equals(DataTypes.LongType)) {
                        preparedStatement.setLong(i + 1, (Long) p.getAs(ent.getKey().toString()));
                    } else if (structField.dataType().equals(DataTypes.DoubleType)) {
                        preparedStatement.setDouble(i + 1, (Double) p.getAs(ent.getKey().toString()));
                    } else if (structField.dataType().equals(DataTypes.FloatType)) {
                        preparedStatement.setFloat(i + 1, (Float) p.getAs(ent.getKey().toString()));
                    } else if (structField.dataType().equals(DataTypes.ShortType)) {
                        preparedStatement.setShort(i + 1, (Short) p.getAs(ent.getKey().toString()));
                    } else {
                        preparedStatement.setString(i + 1, (String) p.getAs(ent.getKey().toString()));
                    }
                }
                preparedStatement.addBatch();
                if (cout.addAndGet(1) >= 2000) {
                    cout.set(0);
                    preparedStatement.executeBatch();
                }
            }

            try {
                preparedStatement.executeBatch();
            } catch (Exception var22) {
                System.out.println(var22.getMessage());
            } finally {
                preparedStatement.close();
            }
        } catch (Exception var24) {
            System.out.println("sql server connect error:" + var24.getMessage());
        }
        closeConn();
    }

}
