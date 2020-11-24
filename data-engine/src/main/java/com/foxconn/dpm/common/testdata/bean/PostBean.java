package com.foxconn.dpm.common.testdata.bean;

import com.foxconn.dpm.common.annotation.HBaseColumn;
import lombok.Data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author HY
 * @Date 2020/7/7 13:54
 * @Description TODO
 */
@Data
public class PostBean{


    private String businessId;

    private List<ColumnKvList> rowKvList;


    public static <T> PostBean buildPostBean(String businessId, List<T> list, Class<T> classTyp) {
        List<ColumnKvList> rowKvLists = PostBean.convertData(list, classTyp);
        PostBean bean = new PostBean();
        bean.setBusinessId(businessId);
        bean.setRowKvList(rowKvLists);
        return  bean;
    }




    public static <T> List<ColumnKvList> convertData(List<T> list, Class<T> classType) {


        List<ColumnKvList> rowKvList = new ArrayList<>();
        Field[] fields = classType.getDeclaredFields();
        list.forEach(data -> {

            List<RowKvList> result = new ArrayList<>();
            for (Field field : fields) {
                HBaseColumn columnAnnotation = field.getAnnotation(HBaseColumn.class);
                if(columnAnnotation == null) {
                    continue;
                }

                field.setAccessible(true);
                Class<?> type = field.getType();
                String typeName = getTypeName(type);
                try {
                    Object o = field.get(data);
                    RowKvList row = new RowKvList();
                    row.setKey(columnAnnotation.column());
                    row.setValue(String.valueOf(o));
                    row.setType(typeName);
                    result.add(row);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }


            ColumnKvList columns = new ColumnKvList();
            columns.setColumnKvList(result);
            rowKvList.add(columns);
        });

       return rowKvList;
    }



    public static String getTypeName(Class<?> type) {

        FieldTypeEnums[] values = FieldTypeEnums.values();

        for(FieldTypeEnums value : values) {

            if(value.getType().equals(type)) {
                return value.getTypeName();
            }
        }

        throw new RuntimeException("获取不到数据类型");
    }
}
