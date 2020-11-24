package com.foxconn.dpm.sprint3.dwd_dws;

import com.foxconn.dpm.sprint3.ods_dwd.udf.GenerateRowKey;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author HS
 * @className L10OTDCommitShipMerge
 * @description TODO
 * @date 2020/5/20 10:58
 */
public class L10OTDCommitShipMerge extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        mergeHpLenovoCommit(batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(1, "-"));
    }


    public void mergeHpLenovoCommit(String startDay, String endDay) throws Exception {
        System.out.println(startDay);
        String startDayStamp = String.valueOf(yyyy_MM_dd.parse(startDay).getTime());
        String endDayStamp = String.valueOf(yyyy_MM_dd.parse(endDay).getTime());
        final JavaRDD<Result> filter = DPHbase.saltRddRead("dpm_dwd_production_scm_ship_qty", startDayStamp, endDayStamp, new Scan(), true).filter(r -> {

            return startDay.equals(batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_SCM_SHIP_QTY", "day_id"))
                    ;
        });
        try{
            for (Result result : filter.take(5)) {
                System.out.println(result);
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>filter End<<<==============================");
        JavaRDD<Row> dpm_dwd_production_scm_ship_qty = filter.mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_dwd_production_scm_ship_qty", beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_dwd_production_scm_ship_qty", "DPM_DWD_PRODUCTION_SCM_SHIP_QTY")));
            }
            return rows.iterator();
        }).cache();

        JavaRDD<Row> dpm_dwd_production_bb_ship_qty = DPHbase.saltRddRead("dpm_dwd_production_bb_ship_qty", startDayStamp, endDayStamp, new Scan(), true).filter(r -> {

            return startDay.equals(batchGetter.resultGetColumn(r, "DPM_DWD_PRODUCTION_BB_SHIP_QTY", "day_id"))
                    ;
        }).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_dwd_production_bb_ship_qty", beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_dwd_production_bb_ship_qty", "DPM_DWD_PRODUCTION_BB_SHIP_QTY")));
            }
            return rows.iterator();
        }).cache();

        sqlContext.createDataFrame(dpm_dwd_production_scm_ship_qty, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_dwd_production_scm_ship_qty")).createOrReplaceTempView("dpm_dwd_production_scm_ship_qty");
        sqlContext.createDataFrame(dpm_dwd_production_bb_ship_qty, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_dwd_production_bb_ship_qty")).createOrReplaceTempView("dpm_dwd_production_bb_ship_qty");

        try{
          /*  List<Row> collect = sqlContext.sql("select day_id,site_code ,level_code,plant_code,customer ,so_no ,so_item,po_no,po_item,dn_no,customer_pn,shiptocountry from dpm_dwd_production_scm_ship_qty group by day_id,site_code ,level_code,plant_code,customer ,so_no ,so_item,po_no,po_item,dn_no,customer_pn,shiptocountry").javaRDD().collect();
            List<Row> collect1 = sqlContext.sql("select day_id,site_code ,level_code,plant_code,customer ,so_no ,so_item,po_no,po_item,dn_no,customer_pn,shiptocountry from dpm_dwd_production_bb_ship_qty group by day_id,site_code ,level_code,plant_code,customer ,so_no ,so_item,po_no,po_item,dn_no,customer_pn,shiptocountry").javaRDD().collect();

            for (Row row : collect) {
                System.out.println(row);
            }
            for (Row row : collect1) {
                System.out.println(row);
            }*/
        }catch(Exception e){

        }
        System.out.println("==============================>>>org End<<<==============================");

        Dataset<Row> mergedRow = sqlContext.sql(
            sqlGetter.Get("sprint_three_l10_otd_commit_ship_merge.sql")
        );

        mergedRow.show();
        System.out.println("==============================>>>mergedRDD End<<<==============================");

        JavaRDD<Put> mergedRDD = mergedRow.javaRDD().mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Put> puts = new ArrayList<>();
            while (batch.hasNext()) {
                Put put = beanGetter.getPut("dpm_dws_production_otd", "DPM_DWS_PRODUCTION_OTD", batch.next());
                if (put != null) {
                    puts.add(put);
                }
            }
            return puts.iterator();
        });


        try {
            System.out.println(mergedRDD.count());
            DPHbase.rddWrite("dpm_dws_production_otd", mergedRDD);
        } catch (Exception e) {
            System.out.println("===============ERR PUT DATA================");
        }

    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
