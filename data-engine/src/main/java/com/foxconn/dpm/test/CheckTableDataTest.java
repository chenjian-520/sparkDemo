package com.foxconn.dpm.test;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.PermissionManager;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.hdfs.DPHdfs;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import lombok.SneakyThrows;
import lombok.var;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.client.cli.RMAdminCLI;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className CheckTableDataTest
 * @description TODO
 * @date 2020/5/6 8:20
 */
public class CheckTableDataTest extends DPSparkBase {

    public static String tableName;
    public static String family;
    public static String startRowKey;
    public static String endRowKey;
    public static Boolean isSalt;
    public static Boolean isStoreHDFS;
    public static String pathDir;
    public static Boolean is_use_sql;
    public static String search_sql;
    public static Boolean is_use_default_columns;
    public static String filted_column_names;
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    HDFSInnerClass hdfsInnerClass = new HDFSInnerClass();
    SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

    static class HDFSInnerClass extends DPHdfs implements Serializable {
        private FileSystem fileSystem = null;
        private String hdfs_url_ac = null;
        private String permitionRootPath = null;

        private void getHDFSSystemInformation() throws Exception {
            System.out.println("==============================>>>HDFS FILE SYSTEM Start<<<==============================");
            Configuration configurationAC = new Configuration();
            FileSystem fileSystemAC = FileSystem.get(configurationAC);
            InetSocketAddress addressOfActive = HAUtil.getAddressOfActive(fileSystemAC);
            InetAddress addressAC = addressOfActive.getAddress();
            String ip = addressAC.getHostAddress();
            int port = addressOfActive.getPort();
            hdfs_url_ac = "hdfs://" + ip + ":" + port;
            System.out.println("ACTIVE_NAMENODE====>>>>" + hdfs_url_ac);
            PermissionManager pm = DPSparkApp.getDpPermissionManager();
            permitionRootPath = pm.getRootHdfsUri();
            System.out.println("PERMITION_PATH====>>>>" + permitionRootPath);
            System.out.println("==============================>>>GET ACTIVE NAMENODE End<<<==============================");

            String user = "hadoop";
            System.setProperty("HADOOP_USER_NAME", user);
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", hdfs_url_ac);
            this.fileSystem = FileSystem.get(URI.create(hdfs_url_ac), configuration, user);
            System.out.println(this.fileSystem);
            System.out.println("==============================>>>HDFS FILE SYSTEM End<<<==============================");

            Collection<String> rmhaIds = org.apache.hadoop.yarn.conf.HAUtil.getRMHAIds(fileSystem.getConf());
            RMAdminCLI rmAdminCLI = new RMAdminCLI(fileSystem.getConf());

            /*rmAdminCLI.main(new String[]{"-getServiceState", "rm12"});
            rmAdminCLI.main(new String[]{"-getServiceState", "rm13"});*/
            System.out.println("==============================>>>Resourcemanager collect Status End<<<==============================");
        }

        public void readHBaseDefColumnsStoreHDFS() throws Exception {
            getHDFSSystemInformation();
            Broadcast<String> tableNameB = DPSparkApp.getContext().broadcast(tableName.trim());
            Broadcast<String> familyB = DPSparkApp.getContext().broadcast(family.trim());
            Broadcast<String> filted_column_namesB = DPSparkApp.getContext().broadcast(filted_column_names.trim());


            JavaRDD<Result> rs = null;
            if (isSalt) {
                rs = DPHbase.saltRddRead(tableName, startRowKey, endRowKey, new Scan(), true);
            } else {
                Scan scan = new Scan();
                scan.withStartRow(startRowKey.getBytes());
                scan.withStopRow(endRowKey.getBytes());
                rs = DPHbase.rddRead(tableName, scan, true);
            }
            try {
                for (Result result : rs.take(5)) {
                    System.out.println(result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("==============================>>>LoadData End<<<==============================");
            Accumulator<Integer> accumulator = DPSparkApp.getContext().accumulator(0);
            JavaRDD<Row> rowJavaRDD = rs.filter(r -> {
//                return is_use_default_columns ? true : MetaGetter.getBatchGetter().checkColumns(r, family, filted_column_names);
                return true;
            }).mapPartitions(batchResult -> {
                ArrayList<Row> lines = new ArrayList<>();
                BeanGetter beanGetter = MetaGetter.getBeanGetter();
                while (batchResult.hasNext()) {
                    accumulator.add(1);
                    Result next = batchResult.next();
                    ArrayList<String> vs = beanGetter.resultGetConfDeftColumnsValues(next, tableNameB.getValue(), familyB.getValue());
                    lines.add(beanGetter.creDeftSchemaRow(tableNameB.getValue(), vs));
                }
                return lines.iterator();
            });

            if (accumulator.value() > 20000000) {
                System.out.println("==============================>>>TO MUCH LINE End<<<==============================");
                return;
            }

            try {
                for (Row r : rowJavaRDD.take(5)) {
                    System.out.println(r.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (is_use_sql) {
                DPSparkApp.getSession().sqlContext().createDataFrame(rowJavaRDD, MetaGetter.getBeanGetter().getDeftSchemaStruct(tableName)).registerTempTable(tableName);
                rowJavaRDD = DPSparkApp.getSession().sqlContext().sql(search_sql).toJavaRDD();
            }
            System.out.println("==============================>>>LoadRowData End<<<==============================");


          /*  ArrayDeque<String> stack = new ArrayDeque<>();
            stack.push("/dpuserdata");
            int loop_level = 0;
            while (!stack.isEmpty()) {
                if (loop_level == 5) {
                    break;
                }
                FileStatus[] fileStatuses = null;
                try {
                    fileStatuses = lsAll(stack.pop());
                } catch (Exception e) {
                    fileStatuses = new FileStatus[0];
                    e.printStackTrace();
                }
                for (int i = 0; i < fileStatuses.length; i++) {
                    try {

                        if (!fileStatuses[i].isFile()) {
                            //hdfs://10.124.160.20:8020/user
                            String nowPath = fileStatuses[i].getPath().toString();
//                                if (nowPath.contains("jyaml-1.3.jar")){
                            System.out.println(nowPath);
                            System.out.println(fileStatuses[i].getGroup());
                            System.out.println(fileStatuses[i].getOwner());
                            System.out.println(fileStatuses[i].getPermission());
//                                }
                            String addPath = nowPath.substring(nowPath.lastIndexOf("/"), nowPath.length());
                            System.out.println(addPath);
                            stack.add(addPath);
                        }
                    } catch (Exception e) {
                        System.out.println("ERR PATH====>>>>" + fileStatuses[i].getPath().toString());
                    }
                }
                loop_level++;
            }


            System.out.println("==============================>>>ListDirs End<<<==============================");
*/

            if (!isStoreHDFS) {
                return;
            }
            //System.setProperty("hadoop.home.dir", hdfs_url_ac + "/dpuserdata/41736e50-883d-42e0-a484-0633759b92/");
            String writePath = (hdfs_url_ac + "/dpuserdata/41736e50-883d-42e0-a484-0633759b92/") + (pathDir == null || "".equals(pathDir) ? new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS").format(new Date()) + ".txt" : pathDir);

            System.out.println(writePath);

            rowJavaRDD.coalesce(1, true).saveAsTextFile(writePath);
            System.out.println("==============================>>>WriteHDFSEnd End<<<==============================");

        }

        private FileStatus[] lsAll(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
            return fileSystem.listStatus(new Path(path));
        }

        private boolean put(InputStream is, String HDFS_OUT_PATH) throws IOException {
            FSDataOutputStream fsDataOutputStream = null;
            try {
                fsDataOutputStream = fileSystem.create(new Path(HDFS_OUT_PATH));
                IOUtils.copyBytes(is, fsDataOutputStream, 51200, true);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } finally {
                fsDataOutputStream.close();
            }
            return true;
        }

        public void del(String fileName) {
            try {
                String[] splitF = fileName.split(",");
                for (int i = 0; i < splitF.length; i++) {
                    if ("*".equals(splitF[i])) {
                        continue;
                    }
                    fileSystem.delete(new Path(hdfs_url_ac + "/dpuserdata/41736e50-883d-42e0-a484-0633759b92/" + splitF[i]), true);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void printlnEnvInformation() throws Exception {
            getHDFSSystemInformation();
        }
    }

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        String function_tag = (String) map.get("FUNCTION_TAG");
        switch (function_tag) {

            case "SEARCH_DATA":
                searchTables(map);
                break;
            case "DELETE_DATA":
                deleteData(map);
                break;
            case "ENV_INFORMATION":
                hdfsInnerClass.printlnEnvInformation();
                break;
        }
        DPSparkApp.stop();
    }

    public void searchTables(Map<String, Object> map) throws Exception {
        tableName = (String) map.get("TABLE_NAME");
        family = (String) map.get("FAMILY_NAME");
        startRowKey = (String) map.get("START_ROWKEY");
        endRowKey = (String) map.get("END_ROWKEY");
        search_sql = (String) map.get("SEARCH_SQL");
        filted_column_names = (String) map.get("FILTED_COLUMN_NAMES");
        isSalt = "true".equals((String) map.get("IS_SALT")) ? true : false;
        isStoreHDFS = "true".equals((String) map.get("IS_STORE_HDFS")) ? true : false;
        is_use_sql = "true".equals((String) map.get("IS_USE_SQL")) ? true : false;
        is_use_default_columns = "true".equals((String) map.get("IS_USE_DEFAULT_COLUMNS")) ? true : false;
        pathDir = "Log_" + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS").format(new Date()) + "_" + (String) map.get("FILE_STORE_DIR_NAME");


        System.out.println(
                "参数：》》》》》》" +
                        tableName + "\n" +
                        family + "\n" +
                        startRowKey + "\n" +
                        endRowKey + "\n" +
                        search_sql + "\n" +
                        isSalt + "\n" +
                        isStoreHDFS + "\n" +
                        is_use_sql + "\n" +
                        pathDir + "\n"
        );


        hdfsInnerClass.readHBaseDefColumnsStoreHDFS();

    }

    public void deleteData(Map<String, Object> map) throws Exception {
        hdfsInnerClass.getHDFSSystemInformation();
        String fileName = (String) map.get("FILE_NAME");

        StringBuilder sb = new StringBuilder();
        if (fileName == null || fileName.length() == 0 || "".equals(fileName)) {
            FileStatus[] fileStatuses = hdfsInnerClass.lsAll("/dpuserdata/41736e50-883d-42e0-a484-0633759b92");
            for (int i = 0; i < fileStatuses.length; i++) {
                try {

                    if (!fileStatuses[i].isFile()) {
                        //hdfs://10.124.160.20:8020/user
                        String nowPath = fileStatuses[i].getPath().toString();
                        String targetFile = nowPath.substring(nowPath.lastIndexOf("/"), nowPath.length());
                        //Log_2020_05_15_17_00_45_889_
                        if (targetFile.matches("Log_[\\d]{4}_[\\d]{2}_[\\d]{2}_[\\d]{2}_[\\d]{2}_[\\d]{2}_[\\d]{3}_")) {
                            String[] split = targetFile.split("_");
                            System.out.println(targetFile);
                            if (split.length < 4) {
                                continue;
                            }
                            if (batchGetter.dateStrCompare(batchGetter.getStDateDayAdd(-2, "-"), split[1] + "-" + split[2] + "-" + split[3], "yyyy-MM-dd", ">=")) {
                                sb.append(targetFile);
                                sb.append(",");
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("ERR PATH====>>>>" + fileStatuses[i].getPath().toString());
                }
            }
            fileName = sb.toString().substring(1, sb.length());
        }

        hdfsInnerClass.del(fileName);
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
