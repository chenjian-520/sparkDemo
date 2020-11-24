package com.foxconn.dpm.common.enums;

/**
 * @Author HY
 * @Date 2020/6/28 13:53
 * @Description TODO
 */
public enum  FilterEnums {
    FILTER_EQUALS("FilterEqualsExecutor"),

    FILTER_NOT_NULL("FilterNotNullExecutor");


    private static final String basePackage = "com.foxconn.dpm.util.beanutils.executor.";

    private String classPath;
    FilterEnums(String classPath) {
        this.classPath = classPath;
    }

    public String getClassPath() {
        return basePackage + this.classPath;
    }
}
