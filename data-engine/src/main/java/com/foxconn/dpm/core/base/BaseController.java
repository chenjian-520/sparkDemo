package com.foxconn.dpm.core.base;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Singleton;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ClassUtil;
import cn.hutool.core.util.StrUtil;
import com.foxconn.dpm.core.consts.StrategyConst;
import com.foxconn.dpm.core.enums.BusinessTypeEnum;
import com.foxconn.dpm.core.exception.BusinessException;
import com.google.common.collect.Lists;
import com.tm.dl.javasdk.dpspark.common.dpinterface.DPSparkBase;
import com.tm.dl.javasdk.dpspark.streaming.DPStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 控制器 基类 .
 *      需配合入参（strategy）使用，规则：site_level_line 多个组合用逗号隔开
 *
 * @className: BaseController
 * @author: ws
 * @date: 2020/7/2 11:28
 * @version: 1.0.0
 */
public abstract class BaseController extends DPSparkBase {

    /**
     * service 扫描路径
     */
    private static final String SERVICE_SCAN_PATH = "com.foxconn.dpm";

    /**
     * 日志
     */
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * 绑定业务类型 .
     * @author ws
     * @date 2020/7/2 14:08
     * @return com.foxconn.dpm.core.enums.BusinessTypeEnum
     **/
    protected abstract BusinessTypeEnum bindBusinessType();

    /**
     * 构造方法 .
     * @author ws
     * @date 2020/7/2 15:49
     * @return
     **/
    public BaseController() {
        // 注册 Service
        registryService(SERVICE_SCAN_PATH);
    }

    /**
     * 调度任务 .
     * @param parameterMap   参数
     * @author ws
     * @date 2020/7/2 11:45
     * @return void
     **/
    @Override
    public void scheduling(Map<String, Object> parameterMap) throws Exception {
        BusinessTypeEnum businessTypeEnum = bindBusinessType();
        log.info("--------- {} start ------- 参数：{}", businessTypeEnum, parameterMap);
        List<String> strategyKeys = parseParameterMap2StrategyEnum(parameterMap);
        if (CollUtil.isEmpty(strategyKeys)) {
            log.warn("根据参数未找到策略枚举，参数为：{}", parameterMap);
            throw new BusinessException("根据参数未找到策略枚举");
        }
        List<BaseService> services = BaseService.listServiceByStrategy(businessTypeEnum, strategyKeys);
        if (CollUtil.isEmpty(services)) {
            log.error("根据业务类型和策略未找到业务类，业务类型：{}，策略：{}", businessTypeEnum, strategyKeys);
            throw new BusinessException("绑定的业务类型和策略已存在，请检查");
        }
        services.forEach(service -> {
            try {
                service.doTask(parameterMap);
            } catch (Exception e) {
                log.error("任务执行异常", e);
            }
        });
        log.info("--------- {} end -------", businessTypeEnum);
    }

    @Override
    public void streaming(Map<String, Object> map, DPStreaming dpStreaming) throws Exception {

    }

    /**
     * 注册 Service .
     * @param packagePath
     * @author ws
     * @date 2020/7/2 15:43
     * @return void
     **/
    private void registryService(String packagePath) {
        ClassUtil.scanPackage(SERVICE_SCAN_PATH).stream()
                .filter(item -> BaseService.class != item
                        && BaseService.class.isAssignableFrom(item))
                .forEach(item -> {
                    Singleton.get(item);
                });
    }

    /**
     * 解析参数 parameterMap 为枚举集合 .
     * @param parameterMap
     * @author ws
     * @date 2020/7/2 14:01
     * @return java.util.List<java.lang.Object>
     **/
    private List<String> parseParameterMap2StrategyEnum(Map<String, Object> parameterMap) {
        List<String> strategyKeys = Lists.newArrayList();
        if (MapUtil.isEmpty(parameterMap)) {
            return strategyKeys;
        }
        String strategyStr = MapUtil.getStr(parameterMap, StrategyConst.KEY_STRATEGY);
        if (StrUtil.isBlank(strategyStr)) {
            return strategyKeys;
        }
        return Arrays.asList(strategyStr.split(StrategyConst.STRATEGY_SPLIT_SYMBOL));
    }
}
