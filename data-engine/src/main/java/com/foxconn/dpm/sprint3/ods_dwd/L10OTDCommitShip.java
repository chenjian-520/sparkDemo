package com.foxconn.dpm.sprint3.ods_dwd;

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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author HS
 * @className L10OTDCommitShip
 * @description TODO
 * @date 2020/5/20 8:10
 */
public class L10OTDCommitShip extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SQLContext sqlContext = DPSparkApp.getSession().sqlContext();
    SqlGetter sqlGetter = MetaGetter.getSql();
    SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        calculateCommitProduction(batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"));
        System.out.println("==============================>>>calculateCommitProduction End<<<==============================");
        calculateSCMCommitProduction(batchGetter.getStDateDayAdd(-1, "-"), batchGetter.getStDateDayAdd(0, "-"));
        System.out.println("==============================>>>calculateSCMCommitProduction End<<<==============================");
    }


    public void calculateCommitProduction(String startDay, String endDay) throws Exception {

        String startDayStamp = String.valueOf(yyyy_MM_dd.parse(startDay).getTime());
        String endDayStamp = String.valueOf(yyyy_MM_dd.parse(startDay).getTime());

        Scan scan = new Scan();
        scan.withStartRow("!".getBytes());
        scan.withStopRow("~".getBytes());
        System.out.println(startDay);
        JavaRDD<Row> dpm_ods_production_edidesadvmain = DPHbase.saltRddRead("dpm_ods_production_edidesadvmain", "!", "~", new Scan(), true).filter(r -> {

            return
                    batchGetter.resultGetColumn(r, "DPM_ODS_PRODUCTION_EDIDESADVMAIN", "shipdt").contains(startDay);
        }).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                ArrayList<String> rs = beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_ods_production_edidesadvmain", "DPM_ODS_PRODUCTION_EDIDESADVMAIN");
                rs.set(7, rs.get(7).split(" ")[0]);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_edidesadvmain", rs));
            }
            return rows.iterator();
        }).cache();

        try {
            for (Row row : dpm_ods_production_edidesadvmain.take(5)) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>dpm_ods_production_edidesadvmain End<<<==============================");

        JavaRDD<Row> dpm_ods_production_edidesadvdetail = DPHbase.saltRddRead("dpm_ods_production_edidesadvdetail", "!", "~", new Scan(), true).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_edidesadvdetail", beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_ods_production_edidesadvdetail", "DPM_ODS_PRODUCTION_EDIDESADVDETAIL")));
            }
            return rows.iterator();
        }).cache();
        try {
            for (Row row : dpm_ods_production_edidesadvdetail.take(5)) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>dpm_ods_production_edidesadvdetail End<<<==============================");


       /* String writePath = "hdfs://10.124.160.20:8020/dpuserdata/41736e50-883d-42e0-a484-0633759b92/".concat(String.valueOf(System.currentTimeMillis())).concat("/1/");
        dpm_ods_production_edidesadvmain.coalesce(1, true).saveAsTextFile(writePath.concat("dpm_ods_production_edidesadvmain"));
        dpm_ods_production_edidesadvdetail.coalesce(1, true).saveAsTextFile(writePath.concat("dpm_ods_production_edidesadvdetail"));*/

        JavaRDD<Row> dpm_ods_production_ediordersdetail = DPHbase.saltRddRead("dpm_ods_production_ediordersdetail", "!", "~", new Scan(), true).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_ediordersdetail", beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_ods_production_ediordersdetail", "DPM_ODS_PRODUCTION_EDIORDERSDETAIL")));
            }
            return rows.iterator();
        }).cache();

        JavaRDD<Row> dpm_ods_production_ediordersparty = DPHbase.saltRddRead("dpm_ods_production_ediordersparty", "!", "~", new Scan(), true).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_ediordersparty", beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_ods_production_ediordersparty", "DPM_ODS_PRODUCTION_EDIORDERSPARTY")));
            }
            return rows.iterator();
        }).cache();

        //==========================================================================================================================
        sqlContext.createDataFrame(dpm_ods_production_edidesadvmain, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_edidesadvmain")).createOrReplaceTempView("dpm_ods_production_edidesadvmain");
        sqlContext.createDataFrame(dpm_ods_production_edidesadvdetail, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_edidesadvdetail")).createOrReplaceTempView("dpm_ods_production_edidesadvdetail");
        sqlContext.createDataFrame(dpm_ods_production_ediordersdetail, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_ediordersdetail")).createOrReplaceTempView("dpm_ods_production_ediordersdetail");
        sqlContext.createDataFrame(dpm_ods_production_ediordersparty, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_ediordersparty")).createOrReplaceTempView("dpm_ods_production_ediordersparty");

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            sqlContext.sql("select * from dpm_ods_production_edidesadvmain,dpm_ods_production_edidesadvdetail  where dpm_ods_production_edidesadvmain.data_from = 'HPMI' and dpm_ods_production_edidesadvdetail.data_from = 'HPMI' and dpm_ods_production_edidesadvmain.shiporderno = dpm_ods_production_edidesadvdetail.shiporderno").show(3000);
            System.out.println("==============================>>>dpm_ods_production_edidesadvmain End<<<==============================");
            sqlContext.sql("select * from dpm_ods_production_edidesadvdetail  where data_from = 'HPMI'").show();
            System.out.println("==============================>>>dpm_ods_production_edidesadvdetail End<<<==============================");
            sqlContext.sql("select * from dpm_ods_production_ediordersdetail where data_from = 'HPMI'").show();
            System.out.println("==============================>>>dpm_ods_production_ediordersdetail End<<<==============================");
            sqlContext.sql("select * from dpm_ods_production_ediordersparty where data_from = 'HPMI'").show();
            System.out.println("==============================>>>dpm_ods_production_ediordersparty End<<<==============================");
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        //==========================================================================================================================

        Dataset<Row> edidesadv_output = sqlContext.sql(
                sqlGetter.Get("sprint_three_l10_otd_commit_ship_b2b.sql").replace("$startDay$", startDay)
        );

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try {
            List<Row> take = edidesadv_output.javaRDD().collect();
            for (Row row : take) {
                System.out.println(row.toString());
            }
        } catch (Exception e) {

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        JavaRDD<Put> hp_putJavaRDD = edidesadv_output.javaRDD().mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Put> puts = new ArrayList<>();
            while (batch.hasNext()) {
                Put put = beanGetter.getPut("dpm_dwd_production_bb_ship_qty", "DPM_DWD_PRODUCTION_BB_SHIP_QTY", batch.next());
                if (put != null) {
                    puts.add(put);
                }
            }
            return puts.iterator();
        });

        try {
            System.out.println(edidesadv_output.count());
            System.out.println(hp_putJavaRDD.count());
            DPHbase.rddWrite("dpm_dwd_production_bb_ship_qty", hp_putJavaRDD);
        } catch (Exception e) {
            System.out.println("===============ERR PUT DATA================");
        }
    }


    public void calculateSCMCommitProduction(String startDay, String endDay) throws Exception {

        String startDayStamp = String.valueOf(yyyy_MM_dd.parse(startDay).getTime());
        String endDayStamp = String.valueOf(yyyy_MM_dd.parse(startDay).getTime());


        JavaRDD<Row> dpm_ods_production_scm_hp_commit_ship = DPHbase.saltRddRead("dpm_ods_production_scm_hp_commit_ship", "!", "~", new Scan(), true).filter(r -> {

            return
                    batchGetter.resultGetColumn(r, "DPM_ODS_PRODUCTION_SCM_HP_COMMIT_SHIP", "actualshipdate").contains(startDay)
                    ;
        }).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                ArrayList<String> rs = beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_ods_production_scm_hp_commit_ship", "DPM_ODS_PRODUCTION_SCM_HP_COMMIT_SHIP");
                rs.set(74, rs.get(74).split(" ")[0]);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_scm_hp_commit_ship", rs));
            }
            return rows.iterator();
        }).cache();
        JavaRDD<Row> dpm_ods_production_scm_lenovo_commit_ship = DPHbase.saltRddRead("dpm_ods_production_scm_lenovo_commit_ship", "!", "~", new Scan(), true).filter(r -> {

            return batchGetter.resultGetColumn(r, "DPM_ODS_PRODUCTION_SCM_LENOVO_COMMIT_SHIP", "shipdate").contains(startDay);
        }).mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Row> rows = new ArrayList<>();
            while (batch.hasNext()) {
                ArrayList<String> rs = beanGetter.resultGetConfDeftColumnsValues(batch.next(), "dpm_ods_production_scm_lenovo_commit_ship", "DPM_ODS_PRODUCTION_SCM_LENOVO_COMMIT_SHIP");
                rs.set(1, rs.get(1).split(" ")[0]);
                rows.add(beanGetter.creDeftSchemaRow("dpm_ods_production_scm_lenovo_commit_ship", rs));
            }
            return rows.iterator();
        }).cache();

        //==========================================================================================================================
        sqlContext.createDataFrame(dpm_ods_production_scm_hp_commit_ship, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_scm_hp_commit_ship")).createOrReplaceTempView("dpm_ods_production_scm_hp_commit_ship");
        sqlContext.createDataFrame(dpm_ods_production_scm_lenovo_commit_ship, MetaGetter.getBeanGetter().getDeftSchemaStruct("dpm_ods_production_scm_lenovo_commit_ship")).createOrReplaceTempView("dpm_ods_production_scm_lenovo_commit_ship");

        sqlContext.sql("select hp_po, actualshipdate from dpm_ods_production_scm_hp_commit_ship limit 10").show();
        sqlContext.sql("select customerpo, shipdate from dpm_ods_production_scm_lenovo_commit_ship limit 10").show();
        //==========================================================================================================================

        Dataset<Row> commit_ship = sqlContext.sql(
                sqlGetter.Get("sprint_three_l10_otd_commit_ship_scm.sql").replace("$startDay$", startDay)
        );

        System.out.println("==============================>>>QA Log Start<<<==============================");
        try{
            List<Row> take = commit_ship.javaRDD().collect();
            for (Row row : take) {
                System.out.println(row.toString());
            }
        }catch(Exception e){

        }
        System.out.println("==============================>>>QA Log End<<<==============================");

        JavaRDD<Put> hp_putJavaRDD = commit_ship.javaRDD().mapPartitions(batch -> {
            BeanGetter beanGetter = MetaGetter.getBeanGetter();
            ArrayList<Put> puts = new ArrayList<>();
            while (batch.hasNext()) {
                Put put = beanGetter.getPut("dpm_dwd_production_scm_ship_qty", "DPM_DWD_PRODUCTION_SCM_SHIP_QTY", batch.next());
                if (put != null) {
                    puts.add(put);
                }
            }
            return puts.iterator();
        });

        try {
            System.out.println(commit_ship.count());
            System.out.println(hp_putJavaRDD.count());
            DPHbase.rddWrite("dpm_dwd_production_scm_ship_qty", hp_putJavaRDD);
        } catch (Exception e) {
            System.out.println("===============ERR PUT DATA================");
        }
    }


    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
