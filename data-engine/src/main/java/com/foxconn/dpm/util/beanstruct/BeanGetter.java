package com.foxconn.dpm.util.beanstruct;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;
import com.tm.dl.javasdk.dpspark.common.hashsalt.ConsistentHashLoadBalance;
import javassist.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.ho.yaml.Yaml;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.JavaConverters.*;

/* ********************************************************************************************
 * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRI             <<<<<<<<<<<<<<<<<<<<<<<<<<<<
 * ********************************************************************************************
 * 注意：
 *     这个类的类池不能序列化，所以不要全局引用，要用就在Partition级别用，看数据处理收益
 *                                                                             **   **
 *
 *                                                                           ************
 ********************************************************************************************** */
public class BeanGetter implements Serializable, MetaGetterRegistry {
    //TODO 此处存储在性能问题，之后可以通过组合模式进行分级处理类信息存储组件
    private ClassPool classPool;
    private Loader loader;
    private HashMap<String, HashMap<String, ArrayList<String>>> hTableInfoMeta;
    private HashMap<String, CtClass> hTableInstanceMeta;
    private HashMap<String, HashMap<String, ArrayList<String>>> tableInfoMeta;
    private HashMap<String, CtClass> tableInstanceMeta;
    private Stack<CtClass> ctClassStack;
    private String BLANK = " ";
    private String SEMICOLON = ";";

    /**
     * ====================================================================
     * 描述:
     * 注意：传入Row的时候：数据Rowkey前面加上预分区的个数：并且
     * 所有字段全部转成String：因为代码中使用getString
     * ====================================================================
     */
    public Put getPut(String tableName, String family, Object abstColumnValues, boolean... isSubSufferFix) {
        if ((isNull(abstColumnValues)) || !existsTableFamily(tableName, family)) {
            return null;
        }
        boolean isSubFamily = false;
        if (isSubSufferFix != null && isSubSufferFix.length == 1) {
            isSubFamily = isSubSufferFix[0];
        }
        if (abstColumnValues instanceof String[] && hTableInfoMeta.get(tableName).get(family).size() == ((String[]) abstColumnValues).length) {
            return creArrPut(isSubFamily, tableName, family, (String[]) abstColumnValues);
        } else if (abstColumnValues instanceof HashMap && ((HashMap) abstColumnValues).size() >= 1) {
            return creMapPut(isSubFamily, tableName, family, (HashMap<String, String>) abstColumnValues);
        } else if (abstColumnValues instanceof Row && ((Row) (abstColumnValues)).size() == hTableInfoMeta.get(tableName).get(family).size()) {
            return creRowPut(isSubFamily, tableName, family, (Row) abstColumnValues);
        } else {
            return null;
        }
    }

    public Put[] getPuts(String tableName, HashMap<String, Object> familyCellEntries) {
        if (!existsHTable(tableName)) {
            return null;
        }
        Put[] puts = new Put[familyCellEntries.values().size()];
        int putCounter = 0;
        Put put = null;
        for (String family : familyCellEntries.keySet()) {
            if ((put = getPut(tableName, family, familyCellEntries.get(family))) != null) {
                puts[putCounter++] = put;
            } else {
                return null;
            }
        }
        return putCounter != familyCellEntries.values().size() ? null : puts;
    }

    public static void main(String[] args) {
    }

    public HashMap<String, StructField> creDeftSchemaMap(String tableName) {
        HashMap<String, StructField> databaseMapping = new HashMap<>();
        creSchema(tableName).forEach(new Consumer<StructField>() {
            @Override
            public void accept(StructField confField) {
                databaseMapping.put(confField.name(), confField);
            }
        });
        return databaseMapping;
    }

    //unsuguesst method
    public HashMap<String, StructField> creDeftSchemaMap(String tableName, String... columns) {

        if (columns == null || columns.length == 0) {
            return creDeftSchemaMap(tableName);
        } else {
            HashMap<String, StructField> databaseMapping = new HashMap<>();
            creSchema(tableName, columns).forEach(new Consumer<StructField>() {
                @Override
                public void accept(StructField confField) {
                    databaseMapping.put(confField.name(), confField);
                }
            });
            return databaseMapping;
        }

    }

    public HashMap<String, StructField> creDeftSchemaMap(String tableName, StructField... addStructFields) {
        HashMap<String, StructField> databaseMapping = new HashMap<>();
        creSchema(tableName).forEach(new Consumer<StructField>() {
            @Override
            public void accept(StructField confField) {
                databaseMapping.put(confField.name(), confField);
            }
        });
        for (StructField addStructField : addStructFields) {
            databaseMapping.put(addStructField.name(), addStructField);
        }
        return databaseMapping;
    }


    public StructType getDeftSchemaStruct(String tableName, Tuple2<Integer, StructField>... addStructFields) {
        List<StructField> confStructSchema = creSchema(tableName);
        ArrayList<StructField> newStructFieldTemp = new ArrayList<>();
        for (int i = 0; i < confStructSchema.size() + addStructFields.length; i++) {
            newStructFieldTemp.add(null);

        }
        for (int i = 0; i < addStructFields.length; i++) {
            newStructFieldTemp.set(addStructFields[i]._1, addStructFields[i]._2);
        }
        int i = 0;
        for (StructField structField : confStructSchema) {
            while (newStructFieldTemp.get(i) != null) {
                i++;
                if (i >= newStructFieldTemp.size()) {
                    i = -1;
                    break;
                }
            }
            if (i == -1) {
                return null;
            }
            newStructFieldTemp.set(i, structField);
        }
        return new StructType(newStructFieldTemp.toArray(new StructField[0]));
    }


    public boolean checkSqlSchema(String tableName, List<StructField> structFields, boolean isCheckOrder) {
        try {
            if (isNull(structFields) || structFields.size() == 0 || !existsTable(tableName)) {
                return false;
            }
            if (isCheckOrder) {
                ArrayList<String> metaColumnInfo = this.tableInfoMeta.get(tableName).get(tableName);
                for (int i = 0; i < metaColumnInfo.size(); i++) {
                    String[] splitMetaInfo = metaColumnInfo.get(i).split("=");
                    if (!splitMetaInfo[0].equals(structFields.get(i).name()) || !this.tableInstanceMeta.get(tableName).getField(structFields.get(i).name()).getType().getSimpleName().equals(getTypeSimpleName(getBaseTypeFullName(upFirst(structFields.get(i).dataType().typeName()))))) {
                        return false;
                    }
                }
            } else {
                for (StructField field : structFields) {
                    if (!this.tableInstanceMeta.get(tableName).getField(field.name()).getType().getSimpleName().equals(getTypeSimpleName(getBaseTypeFullName(upFirst(field.dataType().typeName()))))) {
                        return false;
                    }
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean checkSqlSchema(String tableName, String family, List<StructField> structFields, boolean isCheckOrder) {
        try {
            if (isNull(structFields) || structFields.size() == 0 || !existsTableFamily(tableName, family)) {
                return false;
            }
            if (isCheckOrder) {
                ArrayList<String> metaColumnInfo = this.hTableInfoMeta.get(tableName).get(family);
                for (int i = 0; i < metaColumnInfo.size(); i++) {
                    String[] splitMetaInfo = metaColumnInfo.get(i).split("=");
                    if (!splitMetaInfo[0].equals(structFields.get(i).name()) || !this.hTableInstanceMeta.get(tableName).getField(family).getType().getField(structFields.get(i).name()).getType().getSimpleName().equals(getTypeSimpleName(getBaseTypeFullName(upFirst(structFields.get(i).dataType().typeName()))))) {
                        return false;
                    }
                }
            } else {
                for (StructField field : structFields) {
                    if (!this.hTableInstanceMeta.get(tableName).getField(family).getType().getField(field.name()).getType().getSimpleName().equals(getTypeSimpleName(getBaseTypeFullName(upFirst(field.dataType().typeName()))))) {
                        return false;
                    }
                }
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public HashMap<String, String[]> getDefaultOrderColumnNames(String tableName, String... familys) {
        HashMap<String, String[]> familiesColumnNames = new HashMap<>();
        for (String family : familys) {
            String[] familyColumnNames = this.hTableInfoMeta.get(tableName).get(family).toArray(new String[0]);
            if (familyColumnNames != null && familyColumnNames.length != 0) {
                familiesColumnNames.put(family, familyColumnNames);
            }
        }
        return familiesColumnNames.size() != 0 ? familiesColumnNames : null;
    }

    public String[] getDefaultOrderColumnNames(String tableName, String family) {
        return this.hTableInfoMeta.get(tableName).get(family).toArray(new String[0]);
    }

    public Row creDeftSchemaRow(String tableName, ArrayList values) {
        List<StructField> structFields = creSchema(tableName);
        for (int i = 0; i < structFields.size(); i++) {
            values.set(i, getStringTargetTypeObj(structFields.get(i).dataType().getClass().getSimpleName().replace("Type$", ""), String.valueOf(values.get(i))));
        }

        return new GenericRowWithSchema(values.toArray(), creDeftSchemaStructType(tableName));
    }

    public Row creSchemaRow(StructType structType, ArrayList values) {
        List<StructField> structFields = (List<StructField>) JavaConverters.seqAsJavaListConverter(structType.seq()).asJava();
        for (int i = 0; i < structFields.size(); i++) {
            values.set(i, getStringTargetTypeObj(structFields.get(i).dataType().getClass().getSimpleName().replace("Type$", ""), String.valueOf(values.get(i))));
        }

        return new GenericRowWithSchema(values.toArray(), structType);
    }

    public StructType getDeftSchemaStruct(String tableName) {
        return creDeftSchemaStructType(tableName);
    }

    //该方法默认使用空串代替值
    public ArrayList<String> resultGetConfDeftColumnsValues(Result result, String tableName, String family) {
        String[] columnNames = hTableInfoMeta.get(tableName).get(family).toArray(new String[0]);
        try {
            ArrayList<String> columnValues = new ArrayList<>();
            for (String columnName : columnNames) {
                columnName = columnName.split("=")[0];
                String columnValue = null;
                if (columnName.toLowerCase().equals("rowkey")) {
                    columnValue = Bytes.toString(result.getRow());
                } else {
                    columnValue = Bytes.toString(result.getValue(family.getBytes(), columnName.getBytes()));
                }
                columnValues.add(columnValue != null ? columnValue : "");
            }
            return columnValues.size() == columnNames.length ? columnValues : null;
        } catch (Exception e) {
            return null;
        }
    }

    /* ********************************************************************************************
     * >>>>>>>>>>>>>>>>>>>>             CODE BLOCK DESCRI             <<<<<<<<<<<<<<<<<<<<<<<<<<<<
     * ********************************************************************************************
     *   初始化
     *
     *
     ********************************************************************************************** */

    private BeanGetter() {
        if (null != StaticNestedInstance.instance) {
            throw new RuntimeException();
        } else {
            this.classPool = ClassPool.getDefault();
            this.hTableInfoMeta = new HashMap<>();
            this.hTableInstanceMeta = new HashMap<>();
            this.tableInfoMeta = new HashMap<>();
            this.tableInstanceMeta = new HashMap<>();
            this.ctClassStack = new Stack<>();
            this.loader = new Loader(this.classPool);
            initClassPool();
            initYamlObject();
        }
    }

    private void initClassPool() {
        this.classPool.importPackage("java.math.BigDecimal");
        this.classPool.importPackage("java.sql.Timestamp");
    }

    private List<StructField> creSchema(String tableName) {
        try {
            List<StructField> schema = new ArrayList<>();
            for (String fieldInfo : this.tableInfoMeta.get(tableName).get(tableName)) {
                String[] splitFieldInfo = fieldInfo.split("=");
                StructField structField = null;
                if ((structField = getSchemaField(splitFieldInfo[0], splitFieldInfo[1])) != null) {
                    schema.add(structField);
                } else {
                    return null;
                }
            }
            return schema;
        } catch (Exception e) {
            return null;
        }
    }

    private List<StructField> creSchema(String tableName, String... columnNames) {
        try {
            return removeUnNeedColumn(creSchema(tableName), columnNames);
        } catch (Exception e) {
            return null;
        }
    }

    private StructType creDeftSchemaStructType(String tableName) {
        return new StructType(creSchema(tableName).toArray(new StructField[0]));
    }

    private List<StructField> creHSchema(String tableName, String family) {

        if (isNulls(tableName, family)) {
            return null;
        }
        try {
            List<StructField> schema = new ArrayList<>();
            for (String fieldInfo : this.hTableInfoMeta.get(tableName).get(family)) {
                String[] splitFieldInfo = fieldInfo.split("=");
                StructField structField = null;
                if ((structField = getSchemaField(splitFieldInfo[0], splitFieldInfo[1])) != null) {
                    schema.add(structField);
                } else {
                    return null;
                }
            }
            return schema;
        } catch (Exception e) {
            return null;
        }
    }

    private List<StructField> creHSchema(String tableName, String family, String... columnNames) {
        try {
            return removeUnNeedColumn(creSchema(tableName, family), columnNames);
        } catch (Exception e) {
            return null;
        }
    }

    private List<StructField> removeUnNeedColumn(List<StructField> structFields, String... columnNames) {
        try {
            ArrayList<String> columnNamesList = new ArrayList<>();
            for (String columnName : columnNames) {
                columnNamesList.add(columnName);
            }
            structFields.forEach(new Consumer<StructField>() {
                @Override
                public void accept(StructField structField) {
                    if (!columnNamesList.contains(structField.name())) {
                        structFields.remove(structField);
                    }
                }
            });
            return structFields.size() == 0 ? null : structFields;
        } catch (Exception e) {
            return null;
        }
    }

    private Put creArrPut(boolean isSubSufferFix, String tableName, String family, String... columnValues) {
        if (isNull(columnValues) || columnValues.length == 0) {
            return null;
        }
        String usefulFamily = isSubSufferFix ? family.substring(0, family.lastIndexOf("_")).trim() : family;
        Put put = new Put(columnValues[0].getBytes());
        ArrayList<String> columnNames = null;
        for (int i = 1; i < (columnNames = hTableInfoMeta.get(tableName).get(family)).size(); i++) {
            try {
                String[] columnNames_split = columnNames.get(i).split("=");
                if (checkBaseType(hTableInstanceMeta.get(tableName).getField(family).getType().getField(columnNames_split[0]).getType().getSimpleName(), columnValues[i])) {
                    if (usefulFamily.equals("")) {
                        return null;
                    }
                    put.addColumn(Bytes.toBytes(usefulFamily), columnNames_split[0].getBytes(), columnValues[i].getBytes());
                } else {
                    return null;
                }
            } catch (Exception e) {
                return null;
            }
        }
        return put;
    }

    private Put creRowPut(boolean isSubSufferFix, String tableName, String family, Row columnValues) {
        if (isNull(columnValues) || columnValues.size() == 0) {
            return null;
        }
        String usefulFamily = isSubSufferFix ? family.substring(0, family.lastIndexOf("_")) : family;
        List<StructField> rowStructFields = ScalaBeanGetter.getRowStructFiles(columnValues);

        /////////////////////////////////////////////////////////////////////////////////////////////
        String baseRowKeyInfo = columnValues.getString(0);
        Integer region = -1;
        try {
            region = Integer.parseInt(baseRowKeyInfo.substring(0, baseRowKeyInfo.indexOf(":")));
        } catch (Exception e) {
            return null;
        }
        //基本的rowkey信息
        String baseRowKey = baseRowKeyInfo.substring(baseRowKeyInfo.indexOf(":") + 1, baseRowKeyInfo.length());
        String rowKey = "";
        if (region == -1) {
            //此处不做处理数据无Rgion分区
            rowKey = baseRowKey;
        } else {
            if (region == null || region == 0) {
                return null;
            } else {
                //这一句代码很成问题：资源和侵入式开发都不是好方法
                ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance(region);
                rowKey = consistentHashLoadBalance.selectNode(baseRowKey) + ":" + baseRowKey;
            }
        }
        try {
            if ("".equals(rowKey) || rowKey == null) {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
        /////////////////////////////////////////////////////////////////////////////////////////////

        Put put = new Put(rowKey.getBytes());

        ArrayList<String> columnNames = null;
        for (int i = 1; i < (columnNames = hTableInfoMeta.get(tableName).get(family)).size(); i++) {
            try {
                String[] columnNames_split = columnNames.get(i).split("=");
                CtField field = hTableInstanceMeta.get(tableName).getField(family).getType().getField(columnNames_split[0]);
                if (field.getType().getSimpleName().equals(upFirst(rowStructFields.get(i).dataType().simpleString())) && field.getName().equals(rowStructFields.get(i).name())) {
                    put.addColumn(Bytes.toBytes(usefulFamily), columnNames_split[0].getBytes(), columnValues.getString(i).getBytes());
                } else {
                    return null;
                }
            } catch (Exception e) {
                return null;
            }
        }
        return put;
    }

    private Put creMapPut(boolean isSubSufferFix, String tableName, String family, HashMap<String, String> columnValues) {
        String rowKeyName = null;
        String usefulFamily = isSubSufferFix ? family.substring(0, family.lastIndexOf("_")) : family;
        for (String columnName : columnValues.keySet()) {
            if (checkNameIsRowKey(columnName)) {
                rowKeyName = columnName;
            }
        }
        if (rowKeyName == null) {
            return null;
        }
        Put put = new Put(columnValues.get(rowKeyName).getBytes());
        for (String columnName : columnValues.keySet()) {
            try {
                if (checkBaseType(hTableInstanceMeta.get(tableName).getField(family).getType().getField(columnName).getType().getSimpleName(), columnValues.get(columnName))) {
                    put.addColumn(Bytes.toBytes(usefulFamily), columnName.getBytes(), columnValues.get(columnName).getBytes());
                } else {
                    return null;
                }
            } catch (Exception e) {
                return null;
            }
        }
        return put;
    }

    private void initYamlObject() {
        MetaGetter.properties.forEach(new BiConsumer<Object, Object>() {
            @Override
            public void accept(Object o, Object o2) {
                if (!(((String) o).startsWith("metafile.beans"))) {
                    return;
                }
                String[] beanDirs = ((String) o).split("\\.");
                String beanDir = beanDirs[0] + "/" + beanDirs[1] + "/" + beanDirs[2];
                try {


                    if (beanDir.startsWith("metafile/beans")) {
                        Map<String, Object> load = (Map<String, Object>) new Yaml().load(BeanGetter.class.getClassLoader().getResourceAsStream(beanDir + "/" + ((String) o2)));
                        if (load == null) {
                            return;
                        }
                        load.forEach(new BiConsumer<String, Object>() {
                            @Override
                            public void accept(String tableName, Object node) {
                                switch (checkYamlType(node)) {
                                    case 1:
                                        hTableInfoMeta.put(tableName, (HashMap<String, ArrayList<String>>) node);
                                        try {
                                            initHBeanMeta();
                                        } catch (CannotCompileException e) {
                                            MetaGetter.getFtpLog().info("===================>>>" + tableName + "文件内容或格式错误<<<===================");
                                        }
                                        break;
                                    case 2:
                                        HashMap<String, ArrayList<String>> tableNode = new HashMap<>();
                                        tableNode.put(tableName, (ArrayList<String>) node);
                                        tableInfoMeta.put(tableName, tableNode);
                                        try {
                                            initBeanMeta();
                                        } catch (CannotCompileException e) {
                                        }
                                        break;
                                    case -1:
                                        break;
                                }
                            }
                        });
                    }
                } catch (Exception e) {
                }

            }
        });
    }

    private void initHBeanMeta() throws CannotCompileException {
        if (this.hTableInfoMeta.isEmpty()) {
            return;
        }
        for (String tableName : hTableInfoMeta.keySet()) {
            CtClass tableCtClazz = classPool.makeClass(upFirst(tableName));
            this.ctClassStack.push(tableCtClazz);
            hTableInfoMeta.get(tableName).forEach(new BiConsumer<String, ArrayList<String>>() {
                @Override
                public void accept(String familyName, ArrayList<String> columns) {
                    CtClass familyCtClazz = classPool.makeClass(upFirst(familyName));
                    ctClassStack.push(familyCtClazz);
                    clazzMakeField(tableCtClazz, upFirst(familyName), familyName);
                    clazzMakeField(familyCtClazz, "String", "rowKey");
                    columns.forEach(new Consumer<String>() {
                        @Override
                        public void accept(String column) {
                            String[] split = column.replace(" ", "").split("=");
                            clazzMakeField(familyCtClazz, split[1], split[0]);
                        }
                    });
                }
            });
            makeCtClassStack();
            hTableInstanceMeta.put(tableName, tableCtClazz);
        }
    }

    private void initBeanMeta() throws CannotCompileException {
        if (this.tableInfoMeta.isEmpty()) {
            return;
        }
        for (String tableName : tableInfoMeta.keySet()) {
            CtClass tableCtClazz = classPool.makeClass(upFirst(tableName));
            this.ctClassStack.push(tableCtClazz);
            tableInfoMeta.get(tableName).get(tableName).forEach(new Consumer<String>() {
                @Override
                public void accept(String column) {
                    String[] split = column.replace(" ", "").split("=");
                    clazzMakeField(tableCtClazz, split[1], split[0]);
                }
            });
            makeCtClassStack();
            tableInstanceMeta.put(tableName, tableCtClazz);
        }
    }

    private String upFirst(String word) {
        return word.substring(0, 1).toUpperCase() + word.substring(1, word.length());
    }

    private boolean clazzMakeField(CtClass clazz, String type, String name) {
        return clazzMakeField(clazz, "public", type, name);
    }

    private boolean clazzMakeField(CtClass clazz, String permission, String type, String name) {
        try {
            type = getBaseTypeFullName(type);
        } catch (Exception e) {
            return false;
        }
        try {
            clazz.addField(CtField.make(new StringBuilder(BLANK).append(permission).append(BLANK).append(type).append(BLANK).append(name).append(SEMICOLON).toString(), clazz));
            return true;
        } catch (CannotCompileException e) {
        }
        return false;
    }

    private String getBaseTypeFullName(String type) throws Exception {
        if (type == null) {
            throw new Exception("##################################################\n\nYml File Is Error!\n\n##################################################");
        }
        type = subFromEndCh(type, ".", "(", false, true);
        switch (type) {
            case "Integer":
                return Integer.class.getName();
            case "Long":
                return Long.class.getName();
            case "Float":
                return Float.class.getName();
            case "Double":
                return Double.class.getName();
            case "Date":
                return Date.class.getName();
            case "Boolean":
                return Boolean.class.getName();
            case "Decimal":
                return BigDecimal.class.getName();
            case "Timestamp":
                return Timestamp.class.getName();
            default:
                return type;
        }
    }

    private String getTypeSimpleName(String type) {
        try {
            return subFromEndCh(type, ".", "(", false, true);
        } catch (Exception e) {
            return null;
        }
    }

    private String subFromEndCh(String word, String startWord, String endWord, boolean isStartFirst, boolean isEndFirst) {
        int startIdx = isStartFirst ? word.indexOf(startWord) : word.lastIndexOf(startWord);
        int endIdx = isEndFirst ? word.indexOf(endWord) : word.lastIndexOf(endWord);

        startIdx = startIdx == -1 ? 0 : startIdx + 1;
        endIdx = endIdx == -1 ? word.length() : endIdx;

        if (startIdx <= endIdx) {
            return word.substring(startIdx, endIdx);
        } else {
            return word;
        }
    }

    private boolean makeCtClassStack() {
        while (this.ctClassStack.isEmpty()) {
            try {
                CtClass clazz = this.ctClassStack.pop();
                clazz.makeClassInitializer();
                this.loader.loadClass(clazz.getName());
            } catch (Exception e) {
                this.ctClassStack.clear();
                return false;
            }
        }
        this.ctClassStack.clear();
        return true;
    }

    private int checkYamlType(Object object) {
        try {
            //HashMap<String, HashMap<String, ArrayList>> load1 = (HashMap<String, HashMap<String, ArrayList>>) object;
            HashMap<String, ArrayList> load1 = (HashMap<String, ArrayList>) object;
            return 1;
        } catch (Exception e) {
        }
        try {
            ArrayList<String> load1 = (ArrayList<String>) object;
            return 2;
        } catch (Exception e) {
        }
        return -1;
    }

    private boolean checkBaseType(String baseTypeName, String value) {
        if (value == null || value == "null") {
            return false;
        }
        try {
            switch (baseTypeName) {
                case "String":
                    break;
                case "Integer":
                    Integer.valueOf(value);
                    break;
                case "Long":
                    Long.valueOf(value);
                    break;
                case "Float":
                    Float.valueOf(value);
                    break;
                case "Double":
                    Double.valueOf(value);
                    break;
                case "Date":
                    return value.matches("((^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(10|12|0?[13578])([-\\/\\._])(3[01]|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(11|0?[469])([-\\/\\._])(30|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(0?2)([-\\/\\._])(2[0-8]|1[0-9]|0?[1-9])$)|(^([2468][048]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([3579][26]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$))")
                            ?
                            DateFormat.getDateInstance().parse(value) != null
                            : false;
                case "Boolean":
                    Boolean.valueOf(value);
                case "BigDecimal":
                    new BigDecimal(value);
                    break;
                case "Timestamp":
                    Timestamp.valueOf(value);
                    break;
                default:
                    return false;
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public Object getStringTargetTypeObj(String typeSimpleName, String value) {
        try {
            switch (typeSimpleName) {
                case "String":
                    return value;
                case "Integer":
                    return Integer.valueOf(value);
                case "Long":
                    return Long.valueOf(value);
                case "Float":
                    return Float.valueOf(value);
                case "Double":
                    return Double.valueOf(value);
                case "Date":
                    return value.matches("((^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(10|12|0?[13578])([-\\/\\._])(3[01]|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(11|0?[469])([-\\/\\._])(30|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(0?2)([-\\/\\._])(2[0-8]|1[0-9]|0?[1-9])$)|(^([2468][048]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([3579][26]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$))")
                            ?
                            DateFormat.getDateInstance().parse(value) != null
                            : false;
                case "Boolean":
                    return Boolean.valueOf(value);
                case "BigDecimal":
                    return new BigDecimal(value);
                case "Timestamp":
                    return Timestamp.valueOf(value);
            }
        } catch (Exception e) {
        }
        return null;
    }

    private StructField getSchemaField(String fieldName, String fieldType) {
        try {
            if (isNulls(fieldName, fieldType)) {
                return null;
            }
            DataType dataType = null;
            if (!isNull(dataType = getSchemaFieldType(fieldType))) {
                return DataTypes.createStructField(fieldName, dataType, true);
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    public DataType getSchemaFieldType(String fieldType) {
        String typeSimpleName = getTypeSimpleName(fieldType);
        try {
            switch (typeSimpleName) {
                case "String":
                    return DataTypes.StringType;
                case "Integer":
                    return DataTypes.IntegerType;
                case "Long":
                    return DataTypes.LongType;
                case "Float":
                    return DataTypes.FloatType;
                case "Double":
                    return DataTypes.DoubleType;
                case "Date":
                    return DataTypes.DateType;
                case "Boolean":
                    return DataTypes.BooleanType;
                case "Decimal":
                    Matcher matcher = Pattern.compile("[\\d]+").matcher(fieldType);
                    int precision = matcher.find() ? Integer.valueOf(matcher.group()) : 0;
                    int scale = matcher.find() ? Integer.valueOf(matcher.group()) : 0;
                    return DataTypes.createDecimalType(precision, scale);
                case "Timestamp":
                    return DataTypes.TimestampType;
                default:
                    return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    private boolean checkNameIsRowKey(String... columnName) {
        if (columnName.length == 0) {
            return false;
        }
        if (columnName.length == 1) {
            return columnName[0].toLowerCase().equals("rowkey");
        } else {
            for (String c : columnName) {
                if (c.toLowerCase().equals("rowkey")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean existsHTable(String tableName) {
        try {
            if (hTableInfoMeta.get(tableName) != null) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    private boolean existsTable(String tableName) {
        try {
            if (tableInfoMeta.get(tableName) != null) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    private boolean existsTableFamily(String tableName, String family) {
        if ((!existsHTable(tableName)) || hTableInfoMeta.get(tableName).get(family) == null) {
            return false;
        }
        return true;
    }

    private boolean isNull(Object... obj) {
        return obj == null ? true : false;
    }

    private boolean isNulls(Object... obj) {
        try {
            for (Object o : obj) {
                if (isNull(o)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private static final class StaticNestedInstance {
        private static final BeanGetter instance = new BeanGetter();
    }

    public static BeanGetter getInstance() {
        return StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return StaticNestedInstance.instance;
    }
}
