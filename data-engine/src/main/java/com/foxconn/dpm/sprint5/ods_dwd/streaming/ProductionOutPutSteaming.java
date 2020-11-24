package com.foxconn.dpm.sprint5.ods_dwd.streaming;

import com.foxconn.dpm.core.base.BaseStreaming;
import com.foxconn.dpm.core.base.BaseStreamingService;
import com.foxconn.dpm.sprint5.ods_dwd.service.ProductionOutPutDwdToDwsService;
import com.foxconn.dpm.sprint5.ods_dwd.service.ProductionOutPutDwsToAdsService;
import com.foxconn.dpm.sprint5.ods_dwd.service.ProductionOutPutKafkaToOdsService;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * <H2>描述: 小时产量streaming流程总控制器 </H2>
 * @date 2020/7/15
 */
@Slf4j
public class ProductionOutPutSteaming extends BaseStreaming {

    @Override
    protected List<BaseStreamingService> autoWiredService() {
       return Arrays.asList(
                new ProductionOutPutKafkaToOdsService(),
                new ProductionOutPutDwdToDwsService(),
                new ProductionOutPutDwsToAdsService()
        );
    }

    public static void main(String[] args) throws Exception {
        new ProductionOutPutSteaming().streaming(null, null);
    }
}
