package com.foxconn.dpm.sprint5.dwd_dws;

import com.foxconn.dpm.sprint1_2.dwd_dws.beans.UphDwdCt;
import com.foxconn.dpm.sprint5.bean.DpmDwdProductionOutput;
import com.foxconn.dpm.target_const.LoadKpiTarget;
import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.batchData.BatchGetter;
import com.foxconn.dpm.util.beanstruct.BeanGetter;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.tm.dl.javasdk.dpspark.DPSparkApp;
import com.tm.dl.javasdk.dpspark.common.DlFunction;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import com.tm.dl.javasdk.dpspark.hbase.DPHbase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.storage.StorageLevel;
import org.springframework.util.StringUtils;
import scala.*;

import java.lang.Long;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.foxconn.dpm.sprint1_2.dwd_dws.L6Oee1DwdToDws.notNull;

/**
 * @author cj
 * @date 2020/06/28
 * L10uph
 *
 * 3.其中HP測試工站分別抓取"PRETEST"和"POST RUNIN"兩個工站的數據；聯想為抓取"Testing "工站的數據
 * UPH= 實際output產量 * ct/生產時間       by line
 *
 * 输入表： 抽取dwd表 dpm_dwd_production_output (线体或者机台的单日产量 事实数据 标准化表 高表 每日数据镜像,保留一天的真实数据)
 * 输出表：dpm_dws_production_partno_dd  料号平台生产状态统计(L6 OEE因子數據 L10 UPH因子數據)
 */
public class L10UphDwdToDws extends DPSparkBase {
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
            yesterday = batchGetter.getStDateDayAdd(-1);
            today = batchGetter.getStDateDayAdd(0);
            yesterdayStamp = batchGetter.getStDateDayStampAdd(-1);
            todayStamp = batchGetter.getStDateDayStampAdd(0);
        } else {
            yesterday = map.get("workDate").toString();
            today = map.get("workDate").toString();
            yesterdayStamp = String.valueOf(batchGetter.formatTimestampMilis(yesterday, "yyyy-MM-dd"));
            todayStamp = String.valueOf(batchGetter.formatTimestampMilis(today, "yyyy-MM-dd") + 1);
        }
        String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.valueOf(yesterdayStamp)));
        JavaRDD<DpmDwdProductionOutput> DpmDwdProductionOutputRdd = DPHbase.saltRddRead("dpm_dwd_production_output", yesterdayStamp, todayStamp, new Scan(), true).map(r -> {
            return new DpmDwdProductionOutput(
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("site_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("level_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("factory_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("process_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("area_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("line_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("machine_id"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("part_no"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sku"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("platform"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("customer"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("wo"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("workorder_type"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_dt"))),
                    notNull(Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("work_shift")))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("sn"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_code"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("station_name"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("is_fail"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_by"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("scan_dt"))),
                    Bytes.toString(r.getValue(Bytes.toBytes("DPM_DWD_PRODUCTION_OUTPUT"), Bytes.toBytes("output_qty")))
            );
        }).filter(r -> "0".equals(r.getIs_fail()) && "WH".equals(r.getSite_code()) && "L10".equals(r.getLevel_code()) && format.equals(r.getWork_dt()) && ( ("PRETEST".equals(r.getStation_code().trim()) || "POST RUNIN".equals(r.getStation_code().trim()) && "HP".equals(r.getCustomer()))  || ("Testing".equals(r.getStation_code().trim()) && "LENOVO".equals(r.getCustomer())) ))
          .keyBy(r -> {
            // 全量去重
            return r.toString();
        }).coalesce(10, false).reduceByKey((v1, v2) -> {
            //取最新的数据
            return v1;
        }).map(r -> {
            //还原RDD
            return r._2();
        }).persist(StorageLevel.MEMORY_AND_DISK());

       DPSparkApp.getSession().createDataFrame(DpmDwdProductionOutputRdd, DpmDwdProductionOutput.class).createOrReplaceTempView("OutputView");

       Dataset<Row> rowDataset = DPSparkApp.getSession().sql("select site_code,level_code,factory_code,line_code,platform,station_code process_code,customer,work_dt,count(*) output_qty from OutputView group by site_code,level_code,factory_code,process_code,line_code,platform,customer,station_code,work_dt ");
       rowDataset.show();
        /**
         * 取得dwd中的表数据
         * dpm_dwd_production_standary_ct 不加盐表，ct表
         * rowkey格式 site_code+level_code+ plantform+part_no
         * 取全表数据
         */
        JavaRDD<Result> ctData = DPHbase.rddRead("dpm_dwd_production_standary_ct", new Scan(), true).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<UphDwdCt> ctFormatData = ctData.map(new Function<Result, UphDwdCt>() {
            @Override
            public UphDwdCt call(Result result) throws Exception {
                final String family = "DPM_DWD_PRODUCTION_STANDARY_CT";
                String site_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("site_code")));
                String level_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("level_code")));
                String factory_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("factory_code")));
                String process_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("process_code")));
                String line_code = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("line_code")));
                String platform = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("platform")));
                String part_no = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("part_no")));
                String cycle_time = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("cycle_time")));
                Long update_dt = null;
                try {
                    update_dt = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_dt"))));
                } catch (Exception e) {
                }
                String update_by = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("update_by")));
                String data_from = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("data_from")));
                return new UphDwdCt(site_code, level_code, factory_code, process_code, line_code, platform, part_no, cycle_time, update_dt, update_by, data_from);
            }
        })
                .filter(r -> r.getUpdate_dt() != null && "L10".equals(r.getLevel_code()))
                .keyBy(r -> batchGetter.getStrArrayOrg(",", "-", r.getSite_code(), r.getLevel_code(), r.getFactory_code(), r.getProcess_code(), r.getLine_code(), r.getPlatform(), r.getPart_no()/*, r.getCycle_time()*/))
                .reduceByKey((v1, v2) -> v1.getUpdate_dt() >= v2.getUpdate_dt() ? v1 : v2)
                .map(t -> t._2());

        DPSparkApp.getSession().createDataFrame(ctFormatData, UphDwdCt.class).createOrReplaceTempView("uphCt");
        DPSparkApp.getSession().sql("select * from uphCt").show();
        Dataset<Row> sql = DPSparkApp.getSession().sql(metaGetter.Get("L10UphtoPartnodd.sql"));
        sql.show(60);

        System.out.println(sql.count());


        ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
        SimpleDateFormat formatWorkDt = new SimpleDateFormat("yyyy-MM-dd");

//        JavaRDD<Put> putJavaRDD = sql.toJavaRDD().mapPartitions(batch -> {
//
//            ArrayList<Put> puts = new ArrayList<>();
//            BeanGetter beanGetter = MetaGetter.getBeanGetter();
//            while (batch.hasNext()) {
//                Put put = beanGetter.getPut("dpm_dws_production_output_dd", "DPM_DWS_PRODUCTION_OUTPUT_DD", batch.next());
//                if (put != null) {
//                    puts.add(put);
//                }
//            }
//            return puts.iterator();
//        });


        JavaRDD<Put> putJavaRdd = sql.toJavaRDD().mapPartitions(r -> {
            ArrayList<Put> puts = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            while (r.hasNext()) {
                Row next = r.next();
                //salt+work_dt+site_code+level_code+line_code+platform
                String string = next.getString(8);
                String baseRowKey = sb.append(String.valueOf(formatWorkDt.parse(string).getTime())).append(":").append(next.get(0)).append(":").append(next.get(1)).append(":").append(next.get(4)).append(":").append(next.get(5)).toString();
                String salt = consistentHashLoadBalance.selectNode(baseRowKey);
                Put put = new Put(Bytes.toBytes(salt + ":" + baseRowKey));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("site_code"), Bytes.toBytes(next.getString(0)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("level_code"), Bytes.toBytes(next.getString(1)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("factory_code"), Bytes.toBytes(next.getString(2)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("process_code"), Bytes.toBytes(next.getString(3)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("area_code"), Bytes.toBytes(next.getString(11)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("line_code"), Bytes.toBytes(next.getString(4)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("platform"), Bytes.toBytes(next.getString(5)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("customer"), Bytes.toBytes(next.getString(7)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("work_dt"), Bytes.toBytes(next.getString(8)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("output_qty"), Bytes.toBytes(String.valueOf(next.getLong(9))));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("ct"), Bytes.toBytes(next.getString(10)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("part_no"), Bytes.toBytes(next.getString(6)));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("data_granularity"), Bytes.toBytes("line"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_dt"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("update_by"), Bytes.toBytes("cj"));
                put.addColumn(Bytes.toBytes("DPM_DWS_PRODUCTION_PARTNO_DD"), Bytes.toBytes("data_from"), Bytes.toBytes("dwd_production_ou"));
                puts.add(put);
                sb.delete(0, sb.length());
            }
            return puts.iterator();
        });
        putJavaRdd.take(5).forEach(r -> System.out.println(r));

        DPHbase.rddWrite("dpm_dws_production_partno_dd",putJavaRdd);
        System.out.println("==========end============");

    }




    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }


}
