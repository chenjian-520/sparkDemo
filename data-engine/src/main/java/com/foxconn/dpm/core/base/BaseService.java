package com.foxconn.dpm.core.base;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ReflectUtil;
import com.foxconn.dpm.core.consts.StrategyConst;
import com.foxconn.dpm.core.enums.BusinessTypeEnum;
import com.foxconn.dpm.core.enums.Strategy;
import com.foxconn.dpm.core.exception.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 业务类 基类 .
 *
 * @className: BaseService
 * @author: ws
 * @date: 2020/7/2 11:32
 * @version: 1.0.0
 */
public abstract class BaseService implements Serializable {

    /**
     * 日志
     */
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * 策略业务类集合
     */
    private static final ConcurrentHashMap<String, BaseService> STRATEGY_SERVICES
            = new ConcurrentHashMap<>();

    /**
     * 构造方法 .
     * @author ws
     * @date 2020/7/2 13:22
     * @return
     **/
    public BaseService() {
        BusinessTypeEnum businessTypeEnum = bindBusinessType();
        if (businessTypeEnum == null) {
            throw new BusinessException("请绑定业务类型");
        }
        List<List<Strategy>> strategies = bindStrategy();
        if (CollUtil.isEmpty(strategies)) {
            throw new BusinessException("请绑定策略");
        }
        List<String> keys = buildKey(businessTypeEnum, strategies);
        keys.forEach(key -> {
            if (STRATEGY_SERVICES.get(key) != null) {
                log.error("绑定的业务类型和策略已存在，key：{}", key);
                throw new BusinessException("绑定的业务类型和策略已存在，请检查");
            }
            STRATEGY_SERVICES.put(key, this);
        });
    }

    /**
     * 绑定业务类型 .
     * @author ws
     * @date 2020/7/2 14:08
     * @return com.foxconn.dpm.core.enums.BusinessTypeEnum
     **/
    protected abstract BusinessTypeEnum bindBusinessType();

    /**
     * 绑定策略 .
     * @author ws
     * @date 2020/7/2 17:15
     * @return java.util.List<java.util.List<com.foxconn.dpm.core.enums.Strategy>>
     **/
    protected abstract List<List<Strategy>> bindStrategy();

    /**
     * 执行任务 .
     * @param parameterMap
     * @author ws
     * @date 2020/7/2 15:32
     * @return void
     **/
    public abstract void doTask(Map<String, Object> parameterMap) throws Exception;

    /**
     * 根据策略获取业务类集合 .
     * @param businessTypeEnum
     * @param strategyKeys
     * @author ws
     * @date 2020/7/2 15:23
     * @return java.util.List<com.foxconn.dpm.core.base.BaseService>
     **/
    public static List<BaseService> listServiceByStrategy(BusinessTypeEnum businessTypeEnum
            , List<String> strategyKeys) {
        return strategyKeys.stream()
                .map(strategyKey -> {
                    String key = businessTypeEnum.name()
                            + StrategyConst.STRATEGY_CONCAT_SYMBOL + strategyKey;
                    return STRATEGY_SERVICES.get(key);
                })
                .filter(item -> item != null)
                .collect(Collectors.toList());
    };

    /**
     * 构建 key .
     * @param businessTypeEnum
     * @param strategies
     * @author ws
     * @date 2020/7/2 17:18
     * @return java.util.List<java.lang.String>
     **/
    private static List<String> buildKey(BusinessTypeEnum businessTypeEnum, List<List<Strategy>> strategies) {
        return strategies.stream().map(list ->
            businessTypeEnum.name() + StrategyConst.STRATEGY_CONCAT_SYMBOL +
                    list.stream()
                    .map(item -> getEnumName(item))
                    .collect(Collectors.joining(StrategyConst.STRATEGY_CONCAT_SYMBOL))
        ).collect(Collectors.toList());
    }

    /**
     * 获取 枚举名 .
     * @param strategy
     * @author ws
     * @date 2020/7/2 14:36
     * @return java.lang.String
     **/
    private static String getEnumName(Strategy strategy) {
        Method nameMethod = ReflectUtil.getMethod(strategy.getClass(), "name");
        String name = null;
        try {
            name = (String) nameMethod.invoke(strategy);
        } catch (Exception e) {
            throw new BusinessException("获取 枚举名异常");
        }
        return name;
    }


}
