package com.foxconn.dpm.sprint5.dws_ads;

import com.foxconn.dpm.sprint1_2.dwd_dws.udf.CalculateYearWeek;
import com.foxconn.dpm.sprint1_2.dws_ads.beans.DpmDwsProductionOutputDD;
import com.foxconn.dpm.sprint5.dws_ads.udf.CalculteMonthday;
import com.foxconn.dpm.sprint5.dws_ads.udf.CalculteQuarterday;
import com.foxconn.dpm.sprint5.dws_ads.udf.CalculteWeekday;
import com.foxconn.dpm.sprint5.dws_ads.udf.CalculteYearday;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.collection.JavaConverters;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className
 * @data 2020-06-28
 *
 *  输入表： dpm_dws_production_output_dd
 *  输出表： dpm_ads_production_uph_adherence_day/week/month/quarter/year
 */
public class L6UphToLine extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter sqlGetter = MetaGetter.getSql();
    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //初始化时间
        String todayStamp = batchGetter.getStDateDayStampAdd(1);
        String oldYearStamp = batchGetter.getOldYear(1,0);

        JavaRDD<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDJavaRDD = DPHbase.saltRddRead("dpm_dws_production_output_dd", oldYearStamp, todayStamp, new Scan(), true)
                .filter(r -> "WH".equals(batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "site_code"))
                        && "L6".equals(batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "level_code"))
                        && "line".equals(batchGetter.resultGetColumn(r, "DPM_DWS_PRODUCTION_OUTPUT_DD", "data_granularity"))
                ).mapPartitions(batchP -> {
                    //时间范围过滤
                    BeanGetter beanGetter = MetaGetter.getBeanGetter();
                    ArrayList<DpmDwsProductionOutputDD> dpmDwsProductionOutputDDS = new ArrayList<>();
                    while (batchP.hasNext()) {
                        Result next = batchP.next();
                        ArrayList<String> r = beanGetter.resultGetConfDeftColumnsValues(next, "dpm_dws_production_output_dd", "DPM_DWS_PRODUCTION_OUTPUT_DD");
                        //process_code 4 site_code 1 data_granularity 14

                        dpmDwsProductionOutputDDS.add(batchGetter.<DpmDwsProductionOutputDD>getBeanDeftInit(new DpmDwsProductionOutputDD(), r));
                    }
                    return dpmDwsProductionOutputDDS.iterator();
                }).mapToPair(new PairFunction<DpmDwsProductionOutputDD, String, DpmDwsProductionOutputDD>() {
                    @Override
                    public Tuple2<String, DpmDwsProductionOutputDD> call(DpmDwsProductionOutputDD v) throws Exception {
                        return new Tuple2<String, DpmDwsProductionOutputDD>(batchGetter.getStrArrayOrg(",", "-",
                                v.getWork_dt(), v.getSite_code(), v.getLevel_code(), v.getFactory_code(), v.getProcess_code(), v.getArea_code(), v.getLine_code(), v.getPart_no(), v.getSku(), v.getPlatform(), v.getWorkorder_type(), v.getCustomer(), v.getWork_shift()
                        ), v);
                    }
                }).reduceByKey((v1, v2) -> {
                    return v1.getUpdate_dt() > v2.getUpdate_dt() ? v1 : v2;
                }).map(t -> {
                    return t._2;
                });
        DPSparkApp.getSession().createDataFrame(dpmDwsProductionOutputDDJavaRDD,DpmDwsProductionOutputDD.class).createOrReplaceTempView("L6UphLineView");

        //注册目标值函数
        LoadKpiTarget.loadProductionTarget();
        //注册L6sfc线体转换
        LoadKpiTarget.getLineDateset();
        //注册时间函数
        DPSparkApp.getSession().udf().register("CalculteWeekDay",new CalculteWeekday(),DataTypes.IntegerType);
        DPSparkApp.getSession().udf().register("CalculteQuarterDay",new CalculteQuarterday(),DataTypes.IntegerType);
        DPSparkApp.getSession().udf().register("CalculteMonthDay",new CalculteMonthday(),DataTypes.IntegerType);
        DPSparkApp.getSession().udf().register("CalculteYearDay",new CalculteYearday(),DataTypes.IntegerType);
        DPSparkApp.getSession().udf().register("calculateYearWeek", new CalculateYearWeek(), DataTypes.IntegerType);

        DPSparkApp.getSession().sql("select * from dpm_ods_production_target_values").show();
        System.out.println("/******************************* by day L6uphLine ********************************************/");
        Dataset<Row> l6UphLineDayView = DPSparkApp.getSession().sql(sqlGetter.Get("l6_uphLine_day_ads.sql"));
        l6UphLineDayView.show(50);
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_day", l6UphLineDayView.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(l6UphLineDayView.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by week L6uphLine ********************************************/");
        Dataset<Row> l6UphLineWeekView = DPSparkApp.getSession().sql(sqlGetter.Get("l6_uphLine_week_ads.sql"));
        l6UphLineWeekView.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_week", l6UphLineWeekView.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(l6UphLineWeekView.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by lastWeek L6uphLine ********************************************/");
        Dataset<Row> l6UphLineLastWeekView = DPSparkApp.getSession().sql(sqlGetter.Get("l6_uphLine_lastweek_ads.sql"));
        l6UphLineLastWeekView.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_week", l6UphLineLastWeekView.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(l6UphLineLastWeekView.schema().toSeq()).asJava(), SaveMode.Append);


        System.out.println("/******************************* by month L6uphLine ********************************************/");
        Dataset<Row> l6UphLineMonthView = DPSparkApp.getSession().sql(sqlGetter.Get("l6_uphLine_month_ads.sql"));
        l6UphLineMonthView.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_month", l6UphLineMonthView.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(l6UphLineMonthView.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by quarter L6uphLine ********************************************/");
        Dataset<Row> l6UphLineQuarterView = DPSparkApp.getSession().sql(sqlGetter.Get("l6_uphLine_quarter_ads.sql"));
        l6UphLineQuarterView.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_quarter", l6UphLineQuarterView.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(l6UphLineQuarterView.schema().toSeq()).asJava(), SaveMode.Append);

        System.out.println("/******************************* by year L6uphLine ********************************************/");
        Dataset<Row> l6UphLineYearView = DPSparkApp.getSession().sql(sqlGetter.Get("l6_uphLine_year_ads.sql"));
        l6UphLineYearView.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_production_uph_adherence_year", l6UphLineYearView.toJavaRDD(), (List<StructField>) JavaConverters.seqAsJavaListConverter(l6UphLineYearView.schema().toSeq()).asJava(), SaveMode.Append);

    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
