package com.foxconn.dpm.sprint1_2.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.Dpm_ads_safety_day;
import com.foxconn.dpm.sprint1_2.dwd_dws.beans.Dpm_ads_safety_detail_day;
import com.foxconn.dpm.sprint1_2.dws_ads.udf.CalculateYearWeek;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.ScanTableDto;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.Tuple7;
import scala.collection.JavaConverters;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className SafetyDwsToAds
 * @data 2020/01/20
 *
 * 安全事故统计
 * 输入表： 抽取dws表 dpm_dws_safety_dd 安全事故表
 * 输出表：放入ads表 dpm_ads_safety_day/week/month/quarter/year safety安全事件
 */
public class SafetyDwsToAds extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {

        //com.dl.spark.safety.SafetyDwsToAds
        //初始化时间
        Date today = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = simpleDateFormat.format(today);
        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyyMMdd");
        String yesterday1 = simpleDateFormat1.format(today);
        System.out.println(yesterday);
        System.out.println(yesterday1);

        String yesterdayStamp = batchGetter.getStDateDayStampAdd(-2);
        String todayStamp = batchGetter.getStDateDayStampAdd(0);
        String tomoreStamp = batchGetter.getStDateDayStampAdd(1);

        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(Long.valueOf(todayStamp));
        c.add(Calendar.YEAR, -1);
        c.add(Calendar.MONTH, -1);
        c.add(Calendar.DATE, -7);
        String oldYear = String.valueOf(c.getTime().getTime());

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(yesterdayStamp));
        scan.withStopRow(Bytes.toBytes(tomoreStamp));

        System.out.println("==============================>>>Programe Start<<<==============================");
        List<Tuple7<String, String, String, String, Long, String, String>> saftlyDwd = DPHbase.rddRead("dpm_dws_safety_dd", scan, true).map(r -> {
            return new Tuple7<>(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_day"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_count"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_status"))),
                    Bytes.toLong(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("update_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("update_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("data_from")))
            );

        }).filter(r -> r._2().equals(yesterday) && r._4().equals("立案")).sortBy(r -> r._5(), false, 1).take(1);
        JavaSparkContext context = DPSparkApp.getContext();
        SparkSession sqlContext = DPSparkApp.getSession();
        if (!saftlyDwd.isEmpty()) {

            JavaRDD<Dpm_ads_safety_day> safety_day = context.parallelize(saftlyDwd).map(r -> {
                Dpm_ads_safety_day dpm_ads_safety_day = new Dpm_ads_safety_day();
                dpm_ads_safety_day.setId(UUID.randomUUID().toString());
                dpm_ads_safety_day.setSite_code(r._1());
                dpm_ads_safety_day.setWork_date(r._2());
                dpm_ads_safety_day.setSafety_actual(Integer.valueOf(r._3()));
                dpm_ads_safety_day.setSafety_target(0);
                dpm_ads_safety_day.setEtl_time(String.valueOf(System.currentTimeMillis()));
                return dpm_ads_safety_day;
            });
            Dataset<Row> safety_day_dataFrame = sqlContext.createDataFrame(safety_day, Dpm_ads_safety_day.class);
            safety_day_dataFrame.show();

            HashMap<String, StructField> schemaList = new HashMap<>(16);
            schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
            schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
            schemaList.put("work_date", DataTypes.createStructField("work_date", DataTypes.StringType, true));
            schemaList.put("safety_actual", DataTypes.createStructField("safety_actual", DataTypes.IntegerType, true));
            schemaList.put("safety_target", DataTypes.createStructField("safety_target", DataTypes.IntegerType, true));
            schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));


            DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_safety_day", safety_day_dataFrame.toJavaRDD(), schemaList, safety_day_dataFrame.schema());

        } else {
            insterRDB("work_date", "dpm_ads_safety_day", yesterday);
        }
        // dpm_ads_safety_detail_day  安全信息明细
        String timeMillis = String.valueOf(System.currentTimeMillis());
        Scan scan1 = new Scan();
        scan1.withStartRow(Bytes.toBytes(oldYear));
        scan1.withStopRow(Bytes.toBytes(tomoreStamp));

        JavaRDD<Dpm_ads_safety_day> safetyDayJavaRDD = DPHbase.rddRead("dpm_dws_safety_dd", scan1, true).map(r -> {
            return new Dpm_ads_safety_day()
                    .setSite_code(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("site_code"))))
                    .setWork_date(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_day"))))
                    .setSafety_actual(Integer.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWS_SAFETY_DD"), Bytes.toBytes("accident_count")))));
        });
        DPSparkApp.getSession().createDataFrame(safetyDayJavaRDD, Dpm_ads_safety_day.class).createOrReplaceTempView("safetyView");

        HashMap<String, StructField> schemaList = new HashMap<>(16);
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put("week_id", DataTypes.createStructField("week_id", DataTypes.IntegerType, true));
        schemaList.put("safety_actual", DataTypes.createStructField("safety_actual", DataTypes.LongType, true));
        schemaList.put("safety_target", DataTypes.createStructField("safety_target", DataTypes.IntegerType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.LongType, true));

        System.out.println("/******************************* by week oee2 line ********************************************/");
        //注册week的 udf函数
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);
        Dataset<Row> safetyWeek = DPSparkApp.getSession().sql("select * ,uuid() id,0 safety_target,unix_timestamp() etl_time  from (select site_code,week_id,sum(safety_actual) safety_actual from (select site_code,calculateYearWeek(work_date) week_id,safety_actual from safetyView) group by site_code,week_id) where week_id = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))  ");
        safetyWeek.show();
        if (safetyWeek.count() != 0) {
            DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_safety_week", safetyWeek.toJavaRDD(), schemaList, safetyWeek.schema());
        } else {
            String dateWeek = String.valueOf(batchGetter.getDateWeek(batchGetter.getStDateWeekAdd(0, "-")._1));
            insterRDB("week_id", "dpm_ads_safety_week", dateWeek);
        }

        System.out.println("/******************************* by Last week oee2 line ********************************************/");
        String dateLastWeek = String.valueOf(batchGetter.getDateWeek(batchGetter.getStDateWeekAdd(-1, "-")._1));
        Dataset<Row> safetyLastWeek = DPSparkApp.getSession().sql("select * ,uuid() id,0 safety_target,unix_timestamp() etl_time from (select site_code,week_id,sum(safety_actual) safety_actual from (select site_code,calculateYearWeek(work_date) week_id,safety_actual from safetyView) group by site_code,week_id) where week_id = '" + dateLastWeek + "'  ");
        safetyLastWeek.show();
        if (safetyWeek.count() != 0) {
            DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_safety_week", safetyLastWeek.toJavaRDD(), schemaList, safetyLastWeek.schema());
        } else {
            insterRDB("week_id", "dpm_ads_safety_week", dateLastWeek);
        }

        System.out.println("/******************************* by month oee2 line ********************************************/");
        Dataset<Row> safetyMonth = DPSparkApp.getSession().sql("select * ,uuid() id,0 safety_target,unix_timestamp() etl_time from (select site_code,month_id,sum(safety_actual) safety_actual from (select site_code,cast(from_unixtime(to_unix_timestamp(work_date, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,safety_actual from safetyView) group by site_code,month_id) where month_id = cast(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyyMM') AS INTEGER)  ");
        safetyMonth.show();
        schemaList.remove("week_id");
        schemaList.put("month_id", DataTypes.createStructField("month_id", DataTypes.IntegerType, true));
        if (safetyMonth.count() != 0) {
            DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_safety_month", safetyMonth.toJavaRDD(), schemaList, safetyMonth.schema());
        } else {
            SimpleDateFormat simpleDateFormat12 = new SimpleDateFormat("yyyyMM");
            String month = simpleDateFormat12.format(today);
            insterRDB("month_id", "dpm_ads_safety_month", month);
        }

        System.out.println("/******************************* by quarter oee2 line ********************************************/");
        Dataset<Row> safetyQuarter = DPSparkApp.getSession().sql("select * ,uuid() id,0 safety_target,unix_timestamp() etl_time from (select site_code,quarter_id,sum(safety_actual) safety_actual from (select site_code,cast(concat(year(work_date), quarter(work_date)) AS INTEGER) quarter_id,safety_actual from safetyView) group by site_code,quarter_id) where quarter_id = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) ) AS INTEGER)  ");
        safetyQuarter.show();
        schemaList.remove("month_id");
        schemaList.put("quarter_id", DataTypes.createStructField("quarter_id", DataTypes.IntegerType, true));
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_safety_quarter", safetyQuarter.toJavaRDD(), schemaList, safetyQuarter.schema());

        System.out.println("/******************************* by year oee1 oee2 ********************************************/");
        Dataset<Row> safetyYear = DPSparkApp.getSession().sql("select * , uuid() id,0 safety_target,unix_timestamp() etl_time from (select site_code,year_id,sum(safety_actual) safety_actual from (select site_code,year(work_date) year_id,safety_actual from safetyView) group by site_code,year_id) where year_id = year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))  ");
        safetyYear.show();
        schemaList.remove("quarter_id");
        schemaList.put("year_id", DataTypes.createStructField("year_id", DataTypes.IntegerType, true));
        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_safety_year", safetyYear.toJavaRDD(), schemaList, safetyYear.schema());


//        DPMysql.commonOdbcWriteBatch("dp_ads","dpm_ads_safety_day", DetailDataFrame.toJavaRDD(), schemaList, DetailDataFrame.schema());

    }

    private void insterRDB(String work_date, String tableName, String yesterday) throws Exception {
        List<Dpm_ads_safety_day> safte = new ArrayList();
        safte.add(new Dpm_ads_safety_day().setId(UUID.randomUUID().toString()).setSite_code("WH").setWork_date(yesterday).setSafety_actual(0).setEtl_time(String.valueOf(System.currentTimeMillis())));
        safte.add(new Dpm_ads_safety_day().setId(UUID.randomUUID().toString()).setSite_code("CQ").setWork_date(yesterday).setSafety_actual(0).setEtl_time(String.valueOf(System.currentTimeMillis())));
        DPSparkApp.getSession().createDataFrame(safte, Dpm_ads_safety_day.class).createOrReplaceTempView("tableView");
        Dataset<Row> dataset = DPSparkApp.getSession().sql("select id ,site_code,work_date " + work_date + ",safety_actual,etl_time from tableView");
        dataset.show();
        HashMap<String, StructField> schemaList = new HashMap<>(16);
        schemaList.put("id", DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.put("site_code", DataTypes.createStructField("site_code", DataTypes.StringType, true));
        schemaList.put(work_date, DataTypes.createStructField(work_date, DataTypes.StringType, true));
        schemaList.put("safety_actual", DataTypes.createStructField("safety_actual", DataTypes.IntegerType, true));
        schemaList.put("etl_time", DataTypes.createStructField("etl_time", DataTypes.StringType, true));

        DPMysql.commonOdbcWriteBatch("dp_ads",tableName, dataset.toJavaRDD(), schemaList, dataset.schema());
        System.out.println("==============================>>>Programe End 00<<<==============================");
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }

}
