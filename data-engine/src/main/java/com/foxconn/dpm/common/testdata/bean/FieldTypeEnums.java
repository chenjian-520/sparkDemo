package com.foxconn.dpm.common.testdata.bean;

/**
 * @Author HY
 * @Date 2020/7/7 14:04
 * @Description TODO
 */
public enum FieldTypeEnums {

    STRING(String.class, "STRING"),

    LONG(Long.class, "LONG"),

    INTEGER(Integer.class, "INTEGER");
    ;

    private Class type;
    private String typeName;
    FieldTypeEnums (Class type, String typeName) {
        this.typeName = typeName;
        this.type = type;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public Class getType() {
        return type;
    }

}
