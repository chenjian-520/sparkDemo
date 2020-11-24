package com.foxconn.dpm.test;

import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class FtyTest extends DPSparkBase {

    /**
     * 默认启动类 spark session ： spark core , spark sql
     */
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //业务逻辑
        ftyLogicCode();
        //结束任务释放资源
        DPSparkApp.stop();
    }

    private void ftyLogicCode() throws Exception {

        Scan mbu_scan = new Scan();
        mbu_scan.withStartRow(Bytes.toBytes("0"), true);
        mbu_scan.withStopRow(Bytes.toBytes("z"), true);
        JavaRDD<Result> resultJavaRDD = DPHbase.rddRead("dpm_ods_personnel_hr_mbu_definition", mbu_scan, true);
        resultJavaRDD.foreach( a -> System.out.println(a));

    }

    /**
     * Spark streaming 启动类
     */
    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }


    public static void main(String[] args) throws ParseException {
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2020-04-27 00:00:00.000").getTime());;
    }
}
