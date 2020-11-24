package com.foxconn.dpm.sprint3.dws_ads;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.ScanTableDto;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.mysql.DPMysql;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类
 *
 * @author cj
 * @version 1.0.0
 * @className L6SafetyDetail
 * @data 2020-05-27
 * 安全自定义查询
 * dpm_dwd_safety_detail To dpm_ads_safety_detail_day
 */
public class L6SafetyDetail extends DPSparkBase {

    BatchGetter batchGetter = MetaGetter.getBatchGetter();
    SqlGetter metaGetter = MetaGetter.getSql();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;

        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1,"-");
            today = batchGetter.getStDateDayAdd(0,"-");
            todayStamp = batchGetter.getStDateDayStampAdd(0);
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }

        ScanTableDto empTurnover = new ScanTableDto();
        empTurnover.setBySalt(false);
        empTurnover.setTableName("dpm_dwd_safety_detail");
        empTurnover.setViewTabelName("safetyDetailView");
        empTurnover.setStartRowKey("0");
        empTurnover.setEndRowKey("z");
        empTurnover.setScan(new Scan());
        DPHbase.loadDatasets(new ArrayList() {{
            add(empTurnover);
        }});
        DPSparkApp.getSession().sql("select * from safetyDetailView sort by accident_date desc").show();

        Dataset<Row> dataset = DPSparkApp.getSession().sql("select uuid() id,site_code,accident_classification,accident_date,accident_location,accident_owner,accident_desc,accident_status,unix_timestamp() etl_time from safetyDetailView where accident_date = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd') ");
        dataset.show();
        DPMysql.commonDatasetWriteBatch("dp_ads", "dpm_ads_safety_detail_day",dataset.toJavaRDD(),(List<StructField>) JavaConverters.seqAsJavaListConverter(dataset.schema().toSeq()).asJava(),SaveMode.Append);
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
