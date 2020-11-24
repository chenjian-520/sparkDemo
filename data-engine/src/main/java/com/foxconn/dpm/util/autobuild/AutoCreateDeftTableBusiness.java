package com.foxconn.dpm.util.autobuild;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;
import com.google.gson.Gson;
import com.squareup.okhttp.*;
import javassist.*;
import lombok.Data;
import org.ho.yaml.Yaml;
import scala.Tuple2;
import scala.collection.mutable.ArrayStack;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author HS
 * @className AutoCreateDeftTableBusiness
 * @description TODO
 * @date 2020/6/5 14:33
 */
public class AutoCreateDeftTableBusiness implements Serializable, MetaGetterRegistry {
    private static HashMap<String, HashMap<String, ArrayList<String>>> hTableInfoMeta = new HashMap<>();
    private static HashMap<String, HashMap<String, HashMap<String, String>>> hTableConfigMeta = new HashMap<>();
    private static HashMap<String, ArrayList<String>> validFoldId = new HashMap<>();
    private static OkHttpClient okHttpClient = new OkHttpClient();
    private static Gson gson = new Gson();

    /**
     * ====================================================================
     * 描述:
     * {
     * "columnfamilyConfigDtos": [
     * {
     * "hbcolumnfamilyName": "TEST",
     * "columnConfigDtos": [
     * {
     * "hbcolumnName": "id",
     * "hbcolumnType": "STRING",
     * "hbcolumnIsindex": false,
     * "hbcolumnDesc": "",
     * "status": "add"
     * },
     * {
     * "hbcolumnName": "aaa",
     * "hbcolumnType": "LONG",
     * "hbcolumnIsindex": false,
     * "hbcolumnDesc": "",
     * "status": "add"
     * }
     * ],
     * "status": "add"
     * }
     * ],
     * "hbtableSplitinfo": "01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19",
     * "hbtableIscompression": true,
     * "hbtableIstablesegment": false,
     * "hbtableIstwoLevelIndex": false,
     * "hbtableName": "test",
     * "hbtableDesc": "test",
     * "hbtableCompressionname": "SNAPPY",
     * "isFlag": true,
     * "folderId": "4369863e-6f12-4b94-99af-a976ea6cc6ef"
     * }
     * ====================================================================
     */
    private static String AUTO_CREATE_HTABLES = "http://10.124.160.3:9102/api/manage/v1/hbaseconfig";
    private static String AUTO_CREATE_BUSINESS_HTABLES = "http://10.124.160.3:9102/api/manage/v1/business";
    private static String SEARCH_TABLE_INFO = "http://10.124.160.30:8089/api/dlapiservice/v1/hbaseconfig/tables/select/";
    private static String DELETE_TABLE = "http://10.124.160.3:9102/api/manage/v1/hbaseconfig/";
    private static String INITVALID_FOLDID_BUSINESS = "http://10.124.160.3:9102/api/manage/v1/user/datagrouptree/41736e50-883d-42e0-a484-0633759b92/01d87190-bb63-4eb9-9e21-7a6828fad65c1/";
    private static String INITVALID_FOLDID_INSTANCE = "http://10.124.160.3:9102/api/manage/v1/user/datagrouptree/41736e50-883d-42e0-a484-0633759b92/01d87190-bb63-4eb9-9e21-7a6828fad65c2/";
    private static String GET_TABLE_BUSINESSID = "http://10.124.160.3:9102/api/common/v1/id/business_id";

    public String auto_create_htables(String tableName, String foldId) throws IOException {
        SubmitHbaseTableParams submitHbaseTableParams = initTableSchema(tableName, foldId);

        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, gson.toJson(submitHbaseTableParams));
        Request build = preparePublicHeader(new Request.Builder())
                .url(AUTO_CREATE_HTABLES)
                .post(postBody)
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);
        ResponseStruct responseStruct = gson.fromJson(result, ResponseStruct.class);
        return responseStruct.data;
    }

    public boolean auto_create_business_htables(String tableName, String createdTableID, String foldId) throws IOException {

        if (validFoldId.get("businessValidFoldId").isEmpty()){
            initValidFoldId();
        }

        if (createdTableID == null) {
            return false;
        }

        SearchTableInfo searchTableInfo = searchTableInfo(createdTableID);
        if (searchTableInfo == null) {
            return false;
        }
        BusinessMappingSchema businessMappingSchema = new BusinessMappingSchema();
        businessMappingSchema.businessId = getTableBusinessID();
        businessMappingSchema.businessName = tableName;
        businessMappingSchema.businessDesc = "API CREATED";
        businessMappingSchema.folderId = hTableConfigMeta.get(tableName).get("$$_TABLESCHE_CONFIG_$$").get(foldId);
        if (!businessMappingSchema.folderId.matches("^.{8}-.{4}-.{4}-.{4}-.{12}$") || !validFoldId.get("businessValidFoldId").contains(businessMappingSchema.folderId)) {
            return false;
        }

        ArrayList<BusinessColumnMapping> businessColumnMappings = new ArrayList<>();
        HashMap<String, ResponseTableFamiliesSchema> hbColumnFamilys = searchTableInfo.data.hbColumnFamilys;
        for (String familyName : hbColumnFamilys.keySet()) {
            ResponseTableFamiliesSchema responseTableFamiliesSchema = hbColumnFamilys.get(familyName);
            for (ResponseFamilyColumnsSchema columnConfigDto : responseTableFamiliesSchema.columnConfigDtos) {
                BusinessColumnMapping businessColumnMapping = new BusinessColumnMapping();
                businessColumnMapping.hbtableId = createdTableID;
                businessColumnMapping.hbtableName = tableName;
                businessColumnMapping.hbcolumnfamilyId = responseTableFamiliesSchema.hbcolumnfamilyId;
                businessColumnMapping.hbcolumnfamilyName = responseTableFamiliesSchema.hbcolumnfamilyName;
                businessColumnMapping.hbcolumnId = columnConfigDto.hbcolumnId;
                businessColumnMapping.hbcolumnName = columnConfigDto.hbcolumnName;
                businessColumnMapping.msgkey = columnConfigDto.hbcolumnName;
                businessColumnMappings.add(businessColumnMapping);
            }
        }
        businessMappingSchema.relationtableConfigs = businessColumnMappings;

        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, gson.toJson(businessMappingSchema));
        Request build = preparePublicHeader(new Request.Builder())
                .url(AUTO_CREATE_BUSINESS_HTABLES)
                .post(postBody)
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);
        ResponseStruct responseStruct = gson.fromJson(result, ResponseStruct.class);

        return false;
    }

    public void auto_create_default_conf_all_tables() throws IOException {
        for (String tableName : hTableInfoMeta.keySet()) {
            try {
                String createdTableID = auto_create_htables(tableName, "instance_fold_id");
                auto_create_business_htables(tableName, createdTableID, "business_fold_id");
            } catch (Exception e) {
                System.out.println("Create Table Err :  ".concat(tableName));
            }
        }
    }


    public static void main(String[] args) throws IOException {
        AutoCreateDeftTableBusiness instance = AutoCreateDeftTableBusiness.getInstance();
        //System.out.println(instance.getTableBusinessID());
        //String htable_instance_id = instance.auto_create_htables("dpm_dim_test", "instance_fold_id");
        //instance.auto_create_business_htables("dpm_dim_test", htable_instance_id, "business__fold_id");
        // System.out.println(gson.toJson(instance.searchTableInfo("f163be7d-5f50-4911-b72e-89b3450a5ddf")));
        instance.auto_create_default_conf_all_tables();
        //instance.initValidFoldId();
    }

    private SearchTableInfo searchTableInfo(String tableInstanceID) throws IOException {

        Request build = preparePublicHeader(new Request.Builder())
                .header("Host", "10.124.160.30:8089")
                .header("X-Authorization", "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJGMjBETCIsInNjb3BlcyI6WyJTWVNfREFUQV9NQU5BR0VSIl0sInVzZXJJZCI6IjQxNzM2ZTUwLTg4M2QtNDJlMC1hNDg0LTA2MzM3NTliOTIiLCJhY2NvdW50bmFtZSI6IkZveGNvbm4yLjAgREwiLCJhY2NvdW50IjoiRjIwREwiLCJlbmFibGVkIjpmYWxzZSwiY3JlYXRlZCI6MTU5MTU4OTgzMzY1OSwidXNlckdyb3VwIjoiZjIwZ3JvdXAiLCJpc3MiOiJtYXhpb3QuaW8iLCJpYXQiOjE1OTE1ODk4MzMsImV4cCI6MTYyMzEyNTgzM30.sVaKEH6_t4xZAoV6ghqZ_qIIyierJYo23Z8hce2Ya2jKN-yOkOS1OMk6dQZfhMJKAiZdK7_ODpBTp3A1srHW4g")
                .url(SEARCH_TABLE_INFO.concat(tableInstanceID))
                .get()
                .build();
        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        SearchTableInfo searchTableInfo = gson.fromJson(result, SearchTableInfo.class);
        return searchTableInfo;
    }

    private String getTableBusinessID() throws IOException {
        MediaType contextType = MediaType.parse("application/json;charset=UTF-8");
        RequestBody postBody = RequestBody.create(contextType, "{}");
        Request build = preparePublicHeader(new Request.Builder())
                .url(GET_TABLE_BUSINESSID)
                .post(postBody)
                .build();
        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);

        return gson.fromJson(result, ResponseStruct.class).data;

    }

    private String deleteTable(String tableInstance_id, String tableName) throws IOException {
        MediaType contextType = MediaType.parse("application/json;charset=UTF-8");
        RequestBody postBody = RequestBody.create(contextType, "{}");
        Request build = preparePublicHeader(new Request.Builder())
                .url(DELETE_TABLE.concat(tableInstance_id).concat("/").concat(tableName).concat("/"))
                .method("DELETE", postBody)
                .build();
        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);

        return gson.fromJson(result, ResponseStruct.class).data;

    }

    private Request.Builder preparePublicHeader(Request.Builder builder) {
        return builder.addHeader("Accept", "*/*")
                .addHeader("Accept-Encoding", "gzip, deflate")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
                .addHeader("Content-Type", "application/json;charset=UTF-8")
                .addHeader("DNT", "1")
                .addHeader("Host", "10.124.160.3:9102")
                .addHeader("Origin", "http://tm.iisd.efoxconn.com:803")
                .addHeader("Referer", "http://tm.iisd.efoxconn.com:803/")
                .addHeader("tm-locale", "zh-cn")
                .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36")
                .addHeader("X-Authorization", "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJGMjBETCIsInNjb3BlcyI6WyJTWVNfREFUQV9NQU5BR0VSIl0sInVzZXJJZCI6IjQxNzM2ZTUwLTg4M2QtNDJlMC1hNDg0LTA2MzM3NTliOTIiLCJhY2NvdW50bmFtZSI6IkZveGNvbm4yLjAgREwiLCJhY2NvdW50IjoiRjIwREwiLCJlbmFibGVkIjpmYWxzZSwiY3JlYXRlZCI6MTU5MTI0MTA3NjUwNywidXNlckdyb3VwIjoiZjIwZ3JvdXAiLCJpc3MiOiJtYXhpb3QuaW8iLCJpYXQiOjE1OTEyNDEwNzYsImV4cCI6MTYyMjc3NzA3Nn0.e0uDx0P2HadibPO4zhuedmokt_yX3fV8dYK26rbc1bYCnKW3BNM5W0LrqmwEBvZWfTSAE3W2UQMrQKQ81IVWnw");

    }

    private AutoCreateDeftTableBusiness() {
        if (null != AutoCreateDeftTableBusiness.StaticNestedInstance.instance) {
            throw new RuntimeException();
        } else {
            //validFoldId
            validFoldId.put("businessValidFoldId", new ArrayList<>());
            validFoldId.put("instanceValidFoldId", new ArrayList<>());
            if (!MetaGetter.properties.isEmpty()) {
                MetaGetter.properties.forEach(new BiConsumer<Object, Object>() {
                    @Override
                    public void accept(Object o, Object o2) {
                        if (!(((String) o).startsWith("metafile.autocreate.htables"))) {
                            return;
                        }
                        String[] projectFileDirs = ((String) o).split("\\.");
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < projectFileDirs.length - 2; i++) {
                            sb.append(projectFileDirs[i]);
                            if (i < projectFileDirs.length - 3) {
                                sb.append("/");
                            }
                        }
                        String htablesParamsDir = sb.toString();
                        try {
                            if (((String) o2).endsWith(".yml")) {
                                initYamlObject(htablesParamsDir, (String) o, ((String) o2));
                            }
                        } catch (Exception e) {
                        }

                    }
                });
            }

        }
    }

    private void initYamlObject(String htablesParamsDir, String o, String o2) {
        Map<String, Object> load = (Map<String, Object>) new Yaml().load(AutoCreateDeftTableBusiness.class.getClassLoader().getResourceAsStream(htablesParamsDir + "/" + ((String) o2)));
        if (load == null) {
            return;
        }
        load.forEach(new BiConsumer<String, Object>() {
            @Override
            public void accept(String tableName, Object node) {
                switch (checkYamlType(node)) {
                    case 1:
                        HashMap<String, ArrayList<String>> listNode = (HashMap<String, ArrayList<String>>) node;
                        HashMap<String, HashMap<String, String>> temp = new HashMap<String, HashMap<String, String>>();
                        ArrayList<String> removeKeys = new ArrayList<>();
                        for (String key : listNode.keySet()) {
                            if (key.matches("\\$\\$_.+_\\$\\$")) {
                                HashMap<String, String> confs = new HashMap<>();
                                for (String v : listNode.get(key)) {
                                    if (v.matches("^.+=.+$")) {
                                        confs.put(v.substring(0, v.indexOf("=")), v.substring(v.indexOf("=") + 1, v.length()));
                                    }
                                }
                                temp.put(key, confs);
                                removeKeys.add(key);
                            }
                        }
                        for (String removeKey : removeKeys) {
                            listNode.remove(removeKey);
                        }
                        hTableInfoMeta.put(tableName, listNode);
                        hTableConfigMeta.put(tableName, temp);
                        break;
                    case -1:
                        break;
                }
            }
        });
    }

    private int checkYamlType(Object object) {
        try {
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

    private ArrayList<FamilySchema> initFamiliesSchema(String tableName) {
        ArrayList<FamilySchema> familiesSchemas = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> kv : hTableInfoMeta.get(tableName).entrySet()) {
            FamilySchema familySchema = new FamilySchema();
            familySchema.hbcolumnfamilyName = kv.getKey();
            // state default = add

            if (kv.getKey().matches("\\$\\$_.+\\$\\$")) {
                continue;
            }

            for (String field : kv.getValue()) {
                FieldSchema fieldSchema = new FieldSchema();
                String[] split = field.split("=");
                if (split == null || split.length < 2) {
                    continue;
                }
                if (split[0].matches("\\$\\$_.+\\$\\$")) {
                    if (split[0].equals("$$_FAMILY_STATUE_$$")) {
                        familySchema.status = split[1];
                    }
                    continue;
                }

                for (int i = 0; i < split.length; i++) {
                    switch (i) {
                        case 0:
                            fieldSchema.hbcolumnName = split[i];
                            break;
                        case 1:
                            fieldSchema.hbcolumnType = split[i];
                            break;
                        case 2:
                            try {
                                fieldSchema.hbcolumnIsindex = Boolean.valueOf(split[i]);
                            } catch (Exception e) {
                                fieldSchema.hbcolumnIsindex = false;
                            }
                            break;
                        case 3:
                            fieldSchema.hbcolumnDesc = split[i];
                            break;
                        case 4:
                            fieldSchema.status = split[i];
                            break;
                    }
                    if (i > 4) {
                        break;
                    }
                }
                familySchema.columnConfigDtos.add(fieldSchema);
            }
            familiesSchemas.add(familySchema);
        }

        return familiesSchemas;
    }

    private SubmitHbaseTableParams initTableSchema(String tableName, String foldId) throws IOException {
        if (validFoldId.get("instanceValidFoldId").isEmpty()){
            initValidFoldId();
        }

        SubmitHbaseTableParams submitHbaseTableParams = new SubmitHbaseTableParams();
        Field[] declaredFields = SubmitHbaseTableParams.class.getDeclaredFields();
        for (int i = 0; i < declaredFields.length; i++) {
            declaredFields[i].setAccessible(true);
            String $$_tablesche_config_$$ = hTableConfigMeta.get(tableName).get("$$_TABLESCHE_CONFIG_$$").get(declaredFields[i].getName());
            if ($$_tablesche_config_$$ != null) {
                try {
                    declaredFields[i].set(submitHbaseTableParams, $$_tablesche_config_$$);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        submitHbaseTableParams.folderId = hTableConfigMeta.get(tableName).get("$$_TABLESCHE_CONFIG_$$").get(foldId);
        if (!submitHbaseTableParams.folderId.matches("^.{8}-.{4}-.{4}-.{4}-.{12}$") || !validFoldId.get("instanceValidFoldId").contains(submitHbaseTableParams.folderId)) {
            return null;
        }
        submitHbaseTableParams.columnfamilyConfigDtos = initFamiliesSchema(tableName);
        return submitHbaseTableParams;
    }


    private void initValidFoldId() throws IOException {

        //http://10.124.160.3:9102/api/manage/v1/user/datagrouptree/41736e50-883d-42e0-a484-0633759b92/01d87190-bb63-4eb9-9e21-7a6828fad65c1/
        Request build = preparePublicHeader(new Request.Builder())
                .header("Host", "10.124.160.3:9102")
                .header("X-Authorization", "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJGMjBETCIsInNjb3BlcyI6WyJTWVNfREFUQV9NQU5BR0VSIl0sInVzZXJJZCI6IjQxNzM2ZTUwLTg4M2QtNDJlMC1hNDg0LTA2MzM3NTliOTIiLCJhY2NvdW50bmFtZSI6IkZveGNvbm4yLjAgREwiLCJhY2NvdW50IjoiRjIwREwiLCJlbmFibGVkIjpmYWxzZSwiY3JlYXRlZCI6MTU5MTY2MjEyNDQzNCwidXNlckdyb3VwIjoiZjIwZ3JvdXAiLCJpc3MiOiJtYXhpb3QuaW8iLCJpYXQiOjE1OTE2NjIxMjQsImV4cCI6MTYyMzE5ODEyNH0.ZI_FejXHGY_-gCDOKIYHusxIEvH0mIPjg5SrGWxHO4d0UcZWuMbj2VPUfLsUvSsDv4VtYQknXlzvK7ejkLTZow")
                .url(INITVALID_FOLDID_BUSINESS)
                .get()
                .build();
        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);
        ResponseFoldInfo responseFoldInfo = gson.fromJson(result, ResponseFoldInfo.class);

        ArrayStack<FoldSchema> schemaArrayStack = new ArrayStack<>();

        for (FoldSchema datum : responseFoldInfo.data) {
            schemaArrayStack.push(datum);
        }

        while (!schemaArrayStack.isEmpty()){
            FoldSchema pop = schemaArrayStack.pop();

            validFoldId.get("businessValidFoldId").add(pop.id);

            if (pop.childNode != null && pop.childNode.length > 0){
                for (int i = 0; i < pop.childNode.length; i++) {
                    schemaArrayStack.push(pop.childNode[i]);
                }
            }
        }

        schemaArrayStack.clear();


        Request instance_build = preparePublicHeader(new Request.Builder())
                .header("Host", "10.124.160.3:9102")
                .header("X-Authorization", "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJGMjBETCIsInNjb3BlcyI6WyJTWVNfREFUQV9NQU5BR0VSIl0sInVzZXJJZCI6IjQxNzM2ZTUwLTg4M2QtNDJlMC1hNDg0LTA2MzM3NTliOTIiLCJhY2NvdW50bmFtZSI6IkZveGNvbm4yLjAgREwiLCJhY2NvdW50IjoiRjIwREwiLCJlbmFibGVkIjpmYWxzZSwiY3JlYXRlZCI6MTU5MTU5ODE1NTAwNCwidXNlckdyb3VwIjoiZjIwZ3JvdXAiLCJpc3MiOiJtYXhpb3QuaW8iLCJpYXQiOjE1OTE1OTgxNTUsImV4cCI6MTYyMzEzNDE1NX0.b2DKdPG8yykLakhSmCgN3LG9X10Rkyi73ZOmrPIPiJxDTY6PKMzMf0WH6MBpIwIbmkuj6iohWXAVlkaROWYaDQ")
                .url(INITVALID_FOLDID_INSTANCE)
                .get()
                .build();
        Response instance_execute = okHttpClient.newCall(instance_build).execute();
        String instance_result = instance_execute.body().string();
        System.out.println(instance_result);
        ResponseFoldInfo instance_responseFoldInfo = gson.fromJson(instance_result, ResponseFoldInfo.class);

        for (FoldSchema datum : instance_responseFoldInfo.data) {
            schemaArrayStack.push(datum);
        }

        while (!schemaArrayStack.isEmpty()){
            FoldSchema pop = schemaArrayStack.pop();

            validFoldId.get("instanceValidFoldId").add(pop.id);

            if (pop.childNode != null && pop.childNode.length > 0){
                for (int i = 0; i < pop.childNode.length; i++) {
                    schemaArrayStack.push(pop.childNode[i]);
                }
            }
        }

        System.out.println("==============================>>>initValidFoldId End<<<==============================");
    }

    /////////////////////////////////////////////////////////////
    private static final class StaticNestedInstance {
        private static final AutoCreateDeftTableBusiness instance = new AutoCreateDeftTableBusiness();
    }

    public static AutoCreateDeftTableBusiness getInstance() {
        return AutoCreateDeftTableBusiness.StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return AutoCreateDeftTableBusiness.StaticNestedInstance.instance;
    }


}

////////////////////////////////////////////////////////////
//          业务表映射
////////////////////////////////////////////////////////////
@Data
class BusinessMappingSchema {

    public String businessId;
    public String businessName;
    public String businessDesc;
    public String folderId;

    public ArrayList<BusinessColumnMapping> relationtableConfigs = new ArrayList<>();

}

@Data
class BusinessColumnMapping {
    public String id = UUID.randomUUID().toString();
    public String msgkey;
    public Boolean isEdit = false;
    public String hbtableId;
    public String hbtableName;
    public String hbcolumnfamilyId;
    public String hbcolumnfamilyName;
    public String hbcolumnId;
    public String hbcolumnName;
    public String status = "add";
}

////////////////////////////////////////////////////////////
//          实体表结构
////////////////////////////////////////////////////////////
@Data
class FamilySchema {
    public String hbcolumnfamilyName;
    public String status = "add";
    public ArrayList<FieldSchema> columnConfigDtos = new ArrayList<>();
}

@Data
class FieldSchema {
    public String hbcolumnName;
    public String hbcolumnType;
    public Boolean hbcolumnIsindex = false;
    public String hbcolumnDesc = "";
    public String status = "add";
}

@Data
class SubmitHbaseTableParams {
    public String hbtableSplitinfo = "01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19";
    public Boolean hbtableIscompression = true;
    public Boolean hbtableIstablesegment = false;
    public Boolean hbtableIstwoLevelIndex = false;
    public String hbtableName;
    public String hbtableDesc;
    public String hbtableCompressionname = "SNAPPY";
    public Boolean isFlag = true;
    public String folderId;
    public ArrayList<FamilySchema> columnfamilyConfigDtos = new ArrayList<>();


}
////////////////////////////////////////////////////////////
//搜索表
////////////////////////////////////////////////////////////

/**
 * {
 * "msg": "selected",
 * "data": {
 * "hbtableId": "f163be7d-5f50-4911-b72e-89b3450a5ddf",
 * "hbtableName": "testaaa",
 * "hbtableLogName": {
 * "5eeaa0dd-ba4f-4f91-bacd-7bf82ef0e9ca": {
 * "tablelogId": "5eeaa0dd-ba4f-4f91-bacd-7bf82ef0e9ca",
 * "hbtableId": "f163be7d-5f50-4911-b72e-89b3450a5ddf",
 * "tablelogName": "testaaa20200608121703",
 * "tablelogCreatetime": 1591589826000
 * }
 * },
 * "hbtableIndexName": {
 * "99855474-408a-47fc-9deb-4f4f89096483": {
 * "indexlogId": "99855474-408a-47fc-9deb-4f4f89096483",
 * "hbtableId": "f163be7d-5f50-4911-b72e-89b3450a5ddf",
 * "indexlogName": "testaaa20200608121706",
 * "indexlogCreatetime": 1591589827000
 * }
 * },
 * "hbColumnFamilys": {
 * "TESTAAA": {
 * "hbcolumnfamilyId": "44b9e980-a2da-4c84-ad88-5b9068f2869c",
 * "hbtableId": "f163be7d-5f50-4911-b72e-89b3450a5ddf",
 * "hbcolumnfamilyName": "TESTAAA",
 * "hbcolumnfamilyIsenable": true,
 * "hbcolumnfamilyModifiedby": "F20DL",
 * "hbcolumnfamilyModifieddt": 1591589824000,
 * "hbcolumnfamilyCreateby": "F20DL",
 * "hbcolumnfamilyCreatedt": 1591589824000,
 * "status": null,
 * "columnConfigDtos": [
 * {
 * "hbcolumnId": "9811704e-4342-46fe-9769-ed0be83e781a",
 * "hbcolumnfamilyId": "44b9e980-a2da-4c84-ad88-5b9068f2869c",
 * "hbcolumnName": "asa",
 * "hbcolumnType": "STRING",
 * "hbcolumnIsenable": true,
 * "hbcolumnIsindex": false,
 * "hbcolumnModifiedby": "F20DL",
 * "hbcolumnModifieddt": 1591589824000,
 * "hbcolumnCreateby": "F20DL",
 * "hbcolumnCreatedt": 1591589824000,
 * "hbcolumnDesc": "",
 * "status": null
 * }
 * ]
 * }
 * }
 * }
 * }
 */
@Data
class SearchTableInfo {
    public String msg;
    public ResponseTableSchema data;

}

@Data
class ResponseTableSchema {
    public String hbtableId;
    public String hbtableName;
    public HashMap<String, HashMap<String, String>> hbtableLogName;
    public HashMap<String, HashMap<String, String>> hbtableIndexName;
    public HashMap<String, ResponseTableFamiliesSchema> hbColumnFamilys;
}

@Data
class ResponseTableFamiliesSchema {
    public String hbcolumnfamilyId;
    public String hbtableId;
    public String hbcolumnfamilyName;
    public Boolean hbcolumnfamilyIsenable;
    public String hbcolumnfamilyModifiedby;
    public Long hbcolumnfamilyModifieddt;
    public String hbcolumnfamilyCreateby;
    public Long hbcolumnfamilyCreatedt;
    public String status;
    public ArrayList<ResponseFamilyColumnsSchema> columnConfigDtos = new ArrayList<>();
}

@Data
class ResponseFamilyColumnsSchema {
    public String hbcolumnId;
    public String hbcolumnfamilyId;
    public String hbcolumnName;
    public String hbcolumnType;
    public Boolean hbcolumnIsenable;
    public Boolean hbcolumnIsindex;
    public String hbcolumnModifiedby;
    public Long hbcolumnModifieddt;
    public String hbcolumnCreateby;
    public Long hbcolumnCreatedt;
    public String hbcolumnDesc;
    public Boolean status;
}
////////////////////////////////////////////////////////////

@Data
class ResponseStruct {
    public Boolean result;
    public String msg;
    public String resultcode;
    public String data;
    public String stacktrace;
}

//////////////////////////////////////////////////////////
@Data
class ResponseFoldInfo {

    public String result;
    public String msg;
    public String resultcode;
    public String stacktrace;
    public ArrayList<FoldSchema> data;
}

@Data
class FoldSchema {
    public String id;
    public String functionId;
    public String objectId;
    public String parentId;
    public String itemName;
    public String groupType;
    public String queryGroupType;
    public String queryName;
    public FoldSchema[] childNode;
    public String tasks;
}