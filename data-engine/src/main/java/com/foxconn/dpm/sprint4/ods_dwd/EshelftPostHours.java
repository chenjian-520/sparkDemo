package com.foxconn.dpm.sprint4.ods_dwd;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.DlFunction;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.entity.ScanTableDto;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;

import lombok.SneakyThrows;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.InternalAccumulator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据处理业务类 
 *
 * @author cj
 * @version 1.0.0
 * @Description
 * @data
 */
public class EshelftPostHours extends DPSparkBase {
    BatchGetter batchGetter = MetaGetter.getBatchGetter();

    @Override
    public void scheduling(Map<String, Object> map) throws Exception {
        //业务逻辑
        //获取传入的时间
        String yesterday = null;
        String today = null;
        String yesterdayStamp = null;
        String todayStamp = null;

        if (map.get("workDate") == null) {
            //初始化时间
            yesterday = batchGetter.getStDateDayAdd(-1);
            today = batchGetter.getStDateDayAdd(0);
            //两天更新数据
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
            todayStamp = batchGetter.getStDateDayStampAdd(1);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }

        ScanTableDto empTurnover = new ScanTableDto();
        empTurnover.setBySalt(true);
        empTurnover.setTableName("dpm_ods_production_post_hours");
        empTurnover.setViewTabelName("postHoursView");
        empTurnover.setStartRowKey(yesterdayStamp);
        empTurnover.setEndRowKey(todayStamp);
        empTurnover.setScan(new Scan());
        DPHbase.loadDatasets(Collections.singletonList(empTurnover));

        Dataset<Row> sql = DPSparkApp.getSession().sql("select position,update_by,ot_workhours,BU_code,l4,l5,data_from,act_attendance_workhours,humresource_type,level_code,emp_id,l3,update_dt,work_shift,process_code,skill,line_code,area_code,work_dt,mfg,factory_code,site_code from postHoursView");
        sql.show();

        ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
        SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");


        DPHbase.rddWrite("dpm_dwd_production_post_hours", sql, new DlFunction<GenericRowWithSchema, String>() {
            @SneakyThrows
            @Override
            public String apply(GenericRowWithSchema genericRowWithSchema) {
                //addsalt+work_dt+site_code+level_code+emp_id
                String rowkey = String.valueOf(formatWorkDt.parse(genericRowWithSchema.getAs("work_dt").toString()).getTime()) + ":" + genericRowWithSchema.getAs("site_code").toString() + ":" + genericRowWithSchema.getAs("level_code").toString() + ":" + genericRowWithSchema.getAs("emp_id").toString();
                return consistentHashLoadBalance.selectNode(rowkey) + ":" + rowkey;
            }
        });
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }
}
