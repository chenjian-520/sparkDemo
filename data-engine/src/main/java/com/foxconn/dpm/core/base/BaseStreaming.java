package com.foxconn.dpm.core.base;

import cn.hutool.core.collection.CollUtil;
import com.foxconn.dpm.core.exception.BusinessException;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.log.DPLog;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.Map;

/**
 * <H2>描述: stream控制器 </H2>
 * @date 2020/7/15
 */
@Slf4j
public abstract class BaseStreaming extends DPSparkBase {


    /**
     * 需要注入的service实例 .
     * @author ws
     * @date 2020/7/2 14:08
     * @return com.foxconn.dpm.core.enums.BusinessTypeEnum
     **/
    protected abstract List<BaseStreamingService> autoWiredService();


    /**
     * <H2>描述: 循环执行调度streaming </H2>
     * @date 2020/7/15
     */
    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

        List<BaseStreamingService> serviceList = this.autoWiredService();
        DPLog.esLog(true, "--------- {"+serviceList+"} start ------- 参数：{"+map+"}" );
        if (CollUtil.isEmpty(serviceList)) {
            DPLog.esLog(false, "未发现对应的service-streaming执行集合，参数为：{"+map+"}" );
            throw new BusinessException("未发现对应的service-streaming执行集合");
        }
        serviceList.forEach(service -> {
            try {
                service.execute(map, dpStreaming);
            } catch (Exception e) {
                log.error("任务执行异常", e);
            }
        });
        DPLog.esLog(true, "--------- {{"+serviceList+"}} end -------" );
    }


    @Override
    public void scheduling(Map<String, Object> parameterMap) throws Exception {

    }
}
