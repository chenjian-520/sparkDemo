package com.foxconn.dpm.util.other;

import com.foxconn.dpm.util.dbmeta.DBMetaGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

/**
 * @author HS
 * @className MysqlUtil
 * @description TODO
 * @date 2019/12/21 10:54
 */
public class MysqlUtil extends DPMysql {

    public static boolean commonDatasetWriteBatchMysqlJdbc(DBMetaGetter.DBMeta dbMeta, String tablename, JavaRDD<Row> insertdata, StructType schema, SaveMode saveMode){
        return commonDatasetWriteBatchMysqlJdbc(dbMeta.url, dbMeta.user, dbMeta.pass, tablename, insertdata, schema, saveMode);
    }

    public static boolean commonDatasetWriteBatchMysqlJdbc(String dburl, String dbuser, String dbPassword, String tablename, JavaRDD<Row> insertdata, StructType schema, SaveMode saveMode){
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        SparkSession sparkSession = DPSparkApp.getSession();
        Dataset<Row> insertdataDs = sparkSession.createDataFrame(insertdata, schema);
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbuser);
        connectionProperties.put("password", dbPassword);
        insertdataDs.write().mode(saveMode).jdbc(dburl, tablename, connectionProperties);
        return true;
    }

    public static JavaRDD<Row> creMysqlRDD(String dburl, String dbuser, String dbPassword, String query){
        return DPSparkApp.getSession().read().format("jdbc").option("url", dburl).option("driver", "com.mysql.jdbc.Driver").option("user", dbuser).option("password", dbPassword).option("query", query).load().toJavaRDD();
    }

    public static JavaRDD<Row> creMysqlRDD(DBMetaGetter.DBMeta dbMeta, String query){
        return creMysqlRDD( dbMeta.url,  dbMeta.user,  dbMeta.pass, query);
    }
}
