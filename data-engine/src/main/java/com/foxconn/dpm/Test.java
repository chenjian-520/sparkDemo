package com.foxconn.dpm;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.DataTools;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.HBTableEntity;
import com.tm.dl.javasdk.dpspark.common.entity.HBcolumnEntity;
import com.tm.dl.javasdk.dpspark.common.entity.HBcolumnfamilyEntity;
import com.tm.dl.javasdk.dpspark.es.DPEs;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import javassist.NotFoundException;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple1;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HS
 * @className Test
 * @description TODO
 * @date 2019/12/27 14:13
 */
public class Test  {


    public static void main(String[] args) throws Exception {

        //System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new Date(1577721700000L)));
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2019-12-28 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2019-12-29 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2019-12-30 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2019-12-31 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-01 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-02 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-03 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-04 00:00:00.000").getTime());
//        System.out.println("------");
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-05 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-06 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-07 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-08 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-09 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-10 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-11 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse("2020-01-20 00:00:00.000").getTime());
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new Date(1578153600000L)));
//        System.out.println("00:1577808000000:WH:L5:"+UUID.randomUUID().toString());
        //System.out.println(System.currentTimeMillis());


//
//        Scan scan = new Scan();
//        scan.withStartRow("0".getBytes(), true);
//        scan.withStopRow("z".getBytes(), true);
//        JavaRDD<Result> resultJavaRDD = DPHbase.rddRead("dpm_dwd_production_output_day", scan, true);
//
//        resultJavaRDD.foreach(r->{
//            System.out.println(r);
//        });
    }



    public static void rddWrite(String tableName, JavaRDD<Put> puts) throws Exception {
        HBTableEntity hbTableEntity = DPSparkApp.getEntityByTableName(tableName);
        if (hbTableEntity == null) {
            throw new NotFoundException("没有找到相应的表");
        } else {
            final Broadcast<HBTableEntity> hbTableEntityBroadcast = DPSparkApp.getContext().broadcast(hbTableEntity);
            puts = puts.filter(new Function<Put, Boolean>() {
                public Boolean call(Put put) throws Exception {
                    Iterator var2 = put.getFamilyCellMap().values().iterator();

                    Iterator var4;
                    do {
                        if (!var2.hasNext()) {
                            return true;
                        }

                        List<Cell> cells = (List)var2.next();
                        var4 = cells.iterator();
                    } while(!var4.hasNext());

                    Cell cell = (Cell)var4.next();
                    String familyname = Bytes.toString(cell.getValueArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    HBcolumnfamilyEntity hBcolumnfamilyEntity = (HBcolumnfamilyEntity)((HBTableEntity)hbTableEntityBroadcast.value()).getColumnfamilyList().stream().filter((c) -> {
                        return familyname.equals(c.getHbcolumnfamilyName());
                    }).findFirst().orElse((HBcolumnfamilyEntity) null);
                    if (hBcolumnfamilyEntity == null) {
                        //DPHbase.log.error("===========没有找到对应的列族信息");
                        return false;
                    } else {
                        String cellname = Bytes.toString(cell.getValueArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                        if (hBcolumnfamilyEntity != null && hBcolumnfamilyEntity.getColumnList() != null && !hBcolumnfamilyEntity.getColumnList().isEmpty()) {
                            HBcolumnEntity hBcolumnEntity = (HBcolumnEntity)hBcolumnfamilyEntity.getColumnList().stream().filter((c) -> {
                                return cellname.equals(c.getHbcolumnName());
                            }).findFirst().orElse((HBcolumnEntity) null);
                            if (hBcolumnEntity != null) {
                                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                if (!DataTools.checkDataType(value, hBcolumnEntity.getHbcolumnType())) {
                                    //DPHbase.log.error("数据类型不符，需要的数据类型为：" + hBcolumnEntity.getHbcolumnType() + "传入的数据为：" + value);
                                    return false;
                                } else {
                                    return true;
                                }
                            } else {
                                //DPHbase.log.error("没有找到对应的列信息，查找的列名为：" + cellname);
                                return false;
                            }
                        } else {
                            //DPHbase.log.error("没有找到对应列族或者列的信息，查找的列族名为：" + familyname);
                            return false;
                        }
                    }
                }
            });
            Configuration configuration = DPSparkApp.getDpPermissionManager().initialHbaseSecurityContext();
            JavaHBaseContext hBaseContext = new JavaHBaseContext(DPSparkApp.getContext(), configuration);
            hBaseContext.bulkPut(puts, TableName.valueOf(hbTableEntity.getHbcurrenttablename()), new Function<Put, Put>() {
                public Put call(Put put) throws Exception {
                    return put;
                }
            });
//            String indexName = hbTableEntity.getHbcurrentindexname();
//            if (indexName != null && !indexName.isEmpty() && hbTableEntity.isHbtableIstwoLevelIndex()) {
//                DPEs.addIndexDistributed(indexName, buildJsonSourceByPutRdd(puts));
//            }

        }
    }
}
