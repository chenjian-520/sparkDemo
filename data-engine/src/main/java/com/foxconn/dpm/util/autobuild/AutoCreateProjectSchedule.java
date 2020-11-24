package com.foxconn.dpm.util.autobuild;

import com.foxconn.dpm.util.MetaGetter;
import com.foxconn.dpm.util.MetaGetterRegistry;
import com.foxconn.dpm.util.sql.SqlGetter;
import com.google.gson.Gson;
import com.squareup.okhttp.*;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import lombok.Data;
import okio.BufferedSink;
import org.ho.yaml.Yaml;
import scala.Tuple3;
import scala.collection.mutable.ArrayStack;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * @author HS
 * @className AutoCreateProjectSchedule
 * @description TODO
 * @date 2020/6/4 9:24
 */
public class AutoCreateProjectSchedule implements Serializable, MetaGetterRegistry {

    private static OkHttpClient okHttpClient = new OkHttpClient();
    private static Gson gson = new Gson();
    private static HashMap<String, HashMap<String, Object>> projectDefParams = new HashMap<>();
    private static ResponseProjectParamsBean remote_project_params;
    private static HashMap<String, Integer> queueInfo = new HashMap<>();
    private static HashMap<String, ArrayList<String>> validFoldId = new HashMap<>();

    private AutoCreateProjectSchedule() {
        if (null != AutoCreateProjectSchedule.StaticNestedInstance.instance) {
            throw new RuntimeException();
        } else {
            loadQueueInformation();
            //validFoldId
            validFoldId.put("scheduleValidFoldId", new ArrayList<>());
            validFoldId.put("taskValidFoldId", new ArrayList<>());
            if (!MetaGetter.properties.isEmpty()) {
                MetaGetter.properties.forEach(new BiConsumer<Object, Object>() {
                    @Override
                    public void accept(Object o, Object o2) {
                        if (!(((String) o).startsWith("metafile.autocreate.projects"))) {
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
                        String projectsParamsDir = sb.toString();
                        try {
                            if (((String) o2).endsWith(".yml")) {
                                initYmlFile(projectsParamsDir, (String) o, ((String) o2));
                            }
                        } catch (Exception e) {
                        }

                    }
                });
            }
        }
    }

    private void initYmlFile(String projectsFileDir, String o, String o2) {
        try {
            Map<String, ArrayList<String>> load = (Map<String, ArrayList<String>>) new Yaml().load(AutoCreateProjectSchedule.class.getClassLoader().getResourceAsStream(projectsFileDir + "/" + ((String) o2)));
            if (load == null) {
                return;
            } else {
                load.forEach(new BiConsumer<String, ArrayList<String>>() {
                    @Override
                    public void accept(String projectName, ArrayList<String> params) {
                        if (!projectDefParams.keySet().contains(projectName)) {
                            projectDefParams.put(projectName, new HashMap<>());
                        }
                        for (String param : params) {
                            if (param.matches(".+=.+")) {
                                String[] split = param.split("=");
                                projectDefParams.get(projectName).put(split[0], split[1]);
                            }
                        }
                    }
                });
            }

        } catch (Exception e) {
            return;
        }
    }

    /*
     * ====================================================================
     * 描述:
     *      TASk_SUBMIT_URL =>> http://10.124.160.3:9102/api/manage/v1/task/spark/  Method => PUT
     *      SCHEDULE_SUBMIT_URL =>> http://10.124.160.3:9102/api/manage/v1/schedule  Method => PUT
     *
     *  get_remote_project_params
     *
     *  registerSparkID => uploadJARToHDFS => auto_create_project_schedule
     * 返回值:
     * ====================================================================
     */

    private static String WEB_FUNC_IP_PORT = "10.124.160.3:9102";
    private static String HDFS_FUNC_IP_PORT = "10.124.160.30:8089";
    private static String UNSAFE_WEB_PROTOCOL = "http://";

    private static String REGISTER_SPARK_ID = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/common/v1/id/spark_id");
    private static String REGISTER_SPARK_SCHEDULE_ID = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/common/v1/id/schedule_id");
    private static String GET_SCHEDULE_GROUP_CONFIG = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/config/code/job_group");
    private static String DELETE_SPARK_TASK = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/task/spark/");
    private static String DELETE_SPARK_TASK_SCHEDULE = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/schedule/");
    private static String UPLOAD_JAR_TO_HDFS = UNSAFE_WEB_PROTOCOL.concat(HDFS_FUNC_IP_PORT).concat("/api/dlapiservice/v1/file/base");
    private static String AUTO_CREATE_PROJECT = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/task/spark/");
    private static String AUTO_CREATE_TASK_SCHEDULE = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/schedule");
    private static String GET_REMOTE_PROJECT_PARAMS = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/task/spark/6fbaa726-88fb-4d00-9acc-f2d86bcadf7c/All/");
    private static String INIT_VALID_FOLD_ID_SCHEDULE = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/user/datagrouptree/41736e50-883d-42e0-a484-0633759b92/01d87190-bb63-4eb9-9e21-7a6828fad6651/");
    private static String INIT_VALID_FOLD_ID_TASK = UNSAFE_WEB_PROTOCOL.concat(WEB_FUNC_IP_PORT).concat("/api/manage/v1/user/datagrouptree/41736e50-883d-42e0-a484-0633759b92/6b36aa72-68e3-11ea-9f20-28c13c8f89a2/");

    public static String DP_USER_ID = "41736e50-883d-42e0-a484-0633759b92";

    private ResponseProjectRegisterID registerSparkID() throws IOException {
        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, "{}");
        Request build = preparePublicHeader(new Request.Builder())
                .url(REGISTER_SPARK_ID)
                .post(postBody)
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();

        return gson.fromJson(result, ResponseProjectRegisterID.class);
    }

    private ResponseProjectRegisterID registerSparkScheduleID() throws IOException {
        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, "{}");
        Request build = preparePublicHeader(new Request.Builder())
                .url(REGISTER_SPARK_SCHEDULE_ID)
                .post(postBody)
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();

        return gson.fromJson(result, ResponseProjectRegisterID.class);
    }

    private ResponseGroupConfig getScheduleGroupConfig() throws IOException {
        Request build = preparePublicHeader(new Request.Builder())
                .url(GET_SCHEDULE_GROUP_CONFIG)
                .get()
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();

        return gson.fromJson(result, ResponseGroupConfig.class);
    }

    private void deleteSparkTask(String taskId) throws IOException {
        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, "{}");
        Request build = preparePublicHeader(new Request.Builder())
                .url(DELETE_SPARK_TASK.concat(taskId))
                .method("DELETE", postBody)
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);

    }

    private void deleteSparkTaskSchedule(String taskScheduleId) throws IOException {
        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, "{}");
        Request build = preparePublicHeader(new Request.Builder())
                .url(DELETE_SPARK_TASK_SCHEDULE.concat(taskScheduleId))
                .method("DELETE", postBody)
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();
        System.out.println(result);

    }

    private String uploadJARToHDFS(String jarAbsPath) throws IOException {
        String hdfsPath = "/spark_jar/F20DL/" + UUID.randomUUID().toString() + "/" + jarAbsPath.substring(jarAbsPath.lastIndexOf("/") + 1, jarAbsPath.length());

        RequestBody requestBody = new MultipartBuilder()
                .addFormDataPart("file", "blob", new RequestBody() {
                    @Override
                    public MediaType contentType() {
                        return MediaType.parse("application/octet-stream");
                    }

                    @Override
                    public void writeTo(BufferedSink bufferedSink) throws IOException {
                        ByteOutputStream byteOutputStream = new ByteOutputStream();
                        FileInputStream fileInputStream = new FileInputStream(new File(jarAbsPath));
                        byte[] buf = new byte[1024];
                        int len = -1;
                        while ((len = fileInputStream.read(buf, 0, buf.length)) > 0) {
                            byteOutputStream.write(buf, 0, len);
                        }
                        byteOutputStream.flush();
                        bufferedSink.write(byteOutputStream.getBytes());
                        byteOutputStream.close();
                    }
                })
                .addFormDataPart("filepath", hdfsPath)
                .addFormDataPart("action", "create")
                .build();

        Request build = preparePublicHeader(new Request.Builder())
                .header("Content-Type", "multipart/form-data; boundary=----WebKitFormBoundaryzLiNNdiuUUbEDnQZ")
                .header("Host", "10.124.160.30:8089")
                .url(UPLOAD_JAR_TO_HDFS)
                .post(requestBody)
                .build();

        Response execute = okHttpClient.newCall(build).execute();
        String result = execute.body().string();

        System.out.println(result);
        System.out.println(hdfsPath);
        return hdfsPath;
    }

    /**
     * DEFAULT_PARAM =>>
     * {
     * "id": "SP202006050008",
     * "name": "test",
     * "businessid": "test",
     * "description": "",
     * "editDate": "",
     * "editor": "",
     * "executoridleTimeout": 10,
     * "folderId": "6fbaa726-88fb-4d00-9acc-f2d86bcadf7c",
     * "isActive": true,
     * "maxExecutors": 3,
     * class Params {
     * public String dprunclass;
     * public String dpmaster = "yarn";
     * public String dpappName;
     * public String pmClass = "com.tm.dl.javasdk.dpspark.common.ProdPermissionManager";
     * public String dpType = "scheduling";
     * public String dpuserid = "41736e50-883d-42e0-a484-0633759b92";
     * }
     * "queue": "dp_queue",
     * "schedulerbacklogTimeout": 30,
     * "mainClass": "com.tm.dl.javasdk.dpspark.DPSparkApp",
     * "fileIsUpload": true,
     * "filename": "data_engine.jar",
     * "hdfsPath": "/spark_jar/F20DL/6ba473c9-a19a-4b07-abae-f909312b2076",
     * "driverMemory": 1,
     * "executorMemory": 1,
     * "dependItem": "",
     * "dependUser": ""
     * }
     * ====================================================================
     */
    public Tuple3<String, String, String> auto_create_project(String taskConfName, String task_fold_id, String taskLevel) throws IOException {

        if (validFoldId.get("taskValidFoldId").isEmpty()) {
            initValidFoldId();
        }

        String executeClassName = String.valueOf(projectDefParams.get(taskConfName).get("dprunclass"));
        if (isExecuteClassDefined(executeClassName)) {
            System.out.println("Class Is Defined!");
            return null;
        }
        //prepare Params
        String execSimpleClassName = executeClassName.substring(executeClassName.lastIndexOf(".") + 1, executeClassName.length());
        SubmitSparkTaskParams submitSparkTaskParams = new SubmitSparkTaskParams();
        String taskId = registerSparkID().data;
        submitSparkTaskParams.id = taskId;
        String markTaskName = taskLevel + " " + execSimpleClassName;
        submitSparkTaskParams.name = markTaskName;
        submitSparkTaskParams.businessid = markTaskName;
        submitSparkTaskParams.editDate = String.valueOf(System.currentTimeMillis());
        submitSparkTaskParams.editor = "API";
        submitSparkTaskParams.folderId = String.valueOf(projectDefParams.get(taskConfName).get(task_fold_id));
        //767ae0da-5435-489b-973a-1b6ba6895862
        if (!submitSparkTaskParams.folderId.matches("^.{8}-.{4}-.{4}-.{4}-.{12}$") || !validFoldId.get("taskValidFoldId").contains(submitSparkTaskParams.folderId)) {
            return null;
        }
        submitSparkTaskParams.queue = chooseBalanceQueue();
        Params params = new Params();
        params.dprunclass = executeClassName;
        params.dpappName = execSimpleClassName;
        String executeParams = gson.toJson(params);
        submitSparkTaskParams.params = executeParams;
        String absHdfsFilePath = uploadJARToHDFS(String.valueOf(projectDefParams.get(taskConfName).get("jarAbsPath")));
        submitSparkTaskParams.filename = absHdfsFilePath.substring(absHdfsFilePath.lastIndexOf("/") + 1, absHdfsFilePath.length());
        submitSparkTaskParams.hdfsPath = absHdfsFilePath.substring(0, absHdfsFilePath.lastIndexOf("/"));

        String submitParams = gson.toJson(submitSparkTaskParams);

        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, submitParams);
        Request build = preparePublicHeader(new Request.Builder())
                .url(AUTO_CREATE_PROJECT)
                .post(postBody)
                .build();
        Response execute = okHttpClient.newCall(build).execute();
        ResponseProjectRegisterID responseProjectRegisterTaskID = gson.fromJson(execute.body().string(), ResponseProjectRegisterID.class);
        System.out.println(responseProjectRegisterTaskID.data);

        if (responseProjectRegisterTaskID == null || responseProjectRegisterTaskID.data == null) {
            return null;
        }
        return new Tuple3<>(responseProjectRegisterTaskID.data, executeParams, markTaskName);

    }

    public void auto_create_task_schedule(String taskId, String executeParams, String taskConfName, String markTaskName, String schedule_fold_id) throws IOException {

        if (validFoldId.get("scheduleValidFoldId").isEmpty()) {
            initValidFoldId();
        }


        String taskScheduleId = registerSparkScheduleID().data;
        String schedule_fold_id_v = String.valueOf(projectDefParams.get(taskConfName).get(schedule_fold_id));
        String retriesNumber = String.valueOf(projectDefParams.get(taskConfName).get("retriesNumber"));
        String taskTimeout = String.valueOf(projectDefParams.get(taskConfName).get("taskTimeout"));
        String cron = String.valueOf(projectDefParams.get(taskConfName).get("cron"));
        String sc_description = String.valueOf(projectDefParams.get(taskConfName).get("sc_description"));
        String jobGroup = String.valueOf(getScheduleGroupConfig().data.get("configValue"));

        SubmitBusinessSparkTaskParams submitBusinessSparkTaskParams = new SubmitBusinessSparkTaskParams();
        submitBusinessSparkTaskParams.taskId = taskId;
        submitBusinessSparkTaskParams.folderId = schedule_fold_id_v;
        if (!submitBusinessSparkTaskParams.folderId.matches("^.{8}-.{4}-.{4}-.{4}-.{12}$") || !validFoldId.get("scheduleValidFoldId").contains(submitBusinessSparkTaskParams.folderId)) {
            return;
        }
        submitBusinessSparkTaskParams.jobGroup = jobGroup;
        submitBusinessSparkTaskParams.id = taskScheduleId;
        submitBusinessSparkTaskParams.scheduleParams = executeParams;
        submitBusinessSparkTaskParams.name = markTaskName;
        submitBusinessSparkTaskParams.taskName = markTaskName;
        submitBusinessSparkTaskParams.retriesNumber = retriesNumber;
        submitBusinessSparkTaskParams.taskTimeout = taskTimeout;
        submitBusinessSparkTaskParams.cron = cron;
        submitBusinessSparkTaskParams.description = sc_description;

        String submitScheduleParams = gson.toJson(submitBusinessSparkTaskParams);

        MediaType contextType = MediaType.parse("application/json; charset=utf-8");
        RequestBody postBody = RequestBody.create(contextType, submitScheduleParams);
        Request build = preparePublicHeader(new Request.Builder())
                .url(AUTO_CREATE_TASK_SCHEDULE)
                .post(postBody)
                .build();
        Response execute = okHttpClient.newCall(build).execute();
        System.out.println(execute.body().string());

    }

    public void auto_create_all() throws IOException {
        for (String taskConfName : projectDefParams.keySet()) {
            String taskLevel = String.valueOf(projectDefParams.get(taskConfName).get("taskLevel"));
            Tuple3<String, String, String> createdTaskParams = auto_create_project(taskConfName, "task_fold_id", taskLevel);
            auto_create_task_schedule(createdTaskParams._1(), createdTaskParams._2(), taskConfName, createdTaskParams._3(), "schedule_fold_id");
        }


    }

    public void auto_create_all(String targetTask) throws IOException {
        for (String taskConfName : projectDefParams.keySet()) {
            if (!taskConfName.equals(targetTask)) {
                continue;
            }
            String taskLevel = String.valueOf(projectDefParams.get(taskConfName).get("taskLevel"));
            //String taskId, String executeParams, String markTaskName
            Tuple3<String, String, String> createdTaskParams = auto_create_project(taskConfName, "task_fold_id", taskLevel);
            auto_create_task_schedule(createdTaskParams._1(), createdTaskParams._2(), taskConfName, createdTaskParams._3(), "schedule_fold_id");
        }


    }


    public static void main(String[] args) throws IOException {
        //get_remote_project_params();
        //      System.out.println(registerSparkID().data);
        //  uploadJARToHDFS();
        //   System.out.println( chooseBalanceQueue());
        AutoCreateProjectSchedule.getInstance().auto_create_all("L6_UPH_Line_SPRINT_5");
        //MetaGetter.getAutoCrePrjSc().deleteSparkTask("SP202006080001");
        //MetaGetter.getAutoCrePrjSc().deleteSparkTaskSchedule("SC202006080015");
    }

    private void get_remote_project_params() {

        if (remote_project_params == null || remote_project_params.data.size() == 0) {

            Request build = preparePublicHeader(new Request.Builder())
                    .url(GET_REMOTE_PROJECT_PARAMS)
                    .get()
                    .build();

            Response execute = null;
            String result = null;
            try {
                execute = okHttpClient.newCall(build).execute();
                result = execute.body().string();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //System.out.println(result.replace("\\", "").replace("\"{\"dprunclass", "{\"dprunclass").replace("}\",\"queue", "},\"queue"));
            ResponseProjectParamsBean responseProjectParamsBean = gson.fromJson(result.replace("\\", "").replace("\"{\"dprunclass", "{\"dprunclass").replace("}\",\"queue", "},\"queue"), ResponseProjectParamsBean.class);

            this.remote_project_params = responseProjectParamsBean;
        }
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

    private boolean isExecuteClassDefined(String executeClassName) {
        get_remote_project_params();
        for (ResponseProjectParamsBean.Project datum : remote_project_params.data) {
            if (datum.params.dprunclass.equals(executeClassName)) {
                return true;
            }
        }
        return false;
    }

    private void loadQueueInformation() {
        get_remote_project_params();
        for (ResponseProjectParamsBean.Project datum : remote_project_params.data) {
            if (!queueInfo.keySet().contains(datum.queue)) {
                queueInfo.put(datum.queue, 1);
            } else {
                queueInfo.put(datum.queue, queueInfo.get(datum.queue) + 1);
            }
        }
    }

    private String chooseBalanceQueue() {
        String bestQueue = null;
        int bestQueueTaskCount = Integer.MAX_VALUE;
        for (String queue : queueInfo.keySet()) {
            Integer count = queueInfo.get(queue);
            if (count < bestQueueTaskCount) {
                bestQueue = queue;
                bestQueueTaskCount = count;
            }
        }
        return bestQueue;
    }

    private void initValidFoldId() throws IOException {

        Request build = preparePublicHeader(new Request.Builder())
                .header("Host", "10.124.160.3:9102")
                .header("X-Authorization", "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJGMjBETCIsInNjb3BlcyI6WyJTWVNfREFUQV9NQU5BR0VSIl0sInVzZXJJZCI6IjQxNzM2ZTUwLTg4M2QtNDJlMC1hNDg0LTA2MzM3NTliOTIiLCJhY2NvdW50bmFtZSI6IkZveGNvbm4yLjAgREwiLCJhY2NvdW50IjoiRjIwREwiLCJlbmFibGVkIjpmYWxzZSwiY3JlYXRlZCI6MTU5MTY2MzkzNDc5OCwidXNlckdyb3VwIjoiZjIwZ3JvdXAiLCJpc3MiOiJtYXhpb3QuaW8iLCJpYXQiOjE1OTE2NjM5MzQsImV4cCI6MTYyMzE5OTkzNH0.KJlUr-KVcxfr1iXmDC_l_5fxdTfMb9WZ4lBBKls8b6x0cbpqC-5R69jTgwgxHe107jmn_JUjxk51q8WWN2FHaw")
                .url(INIT_VALID_FOLD_ID_SCHEDULE)
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

        while (!schemaArrayStack.isEmpty()) {
            FoldSchema pop = schemaArrayStack.pop();

            validFoldId.get("scheduleValidFoldId").add(pop.id);

            if (pop.childNode != null && pop.childNode.length > 0) {
                for (int i = 0; i < pop.childNode.length; i++) {
                    schemaArrayStack.push(pop.childNode[i]);
                }
            }
        }

        schemaArrayStack.clear();


        Request instance_build = preparePublicHeader(new Request.Builder())
                .header("Host", "10.124.160.3:9102")
                .header("X-Authorization", "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJGMjBETCIsInNjb3BlcyI6WyJTWVNfREFUQV9NQU5BR0VSIl0sInVzZXJJZCI6IjQxNzM2ZTUwLTg4M2QtNDJlMC1hNDg0LTA2MzM3NTliOTIiLCJhY2NvdW50bmFtZSI6IkZveGNvbm4yLjAgREwiLCJhY2NvdW50IjoiRjIwREwiLCJlbmFibGVkIjpmYWxzZSwiY3JlYXRlZCI6MTU5MTY2MzkzNDc5OCwidXNlckdyb3VwIjoiZjIwZ3JvdXAiLCJpc3MiOiJtYXhpb3QuaW8iLCJpYXQiOjE1OTE2NjM5MzQsImV4cCI6MTYyMzE5OTkzNH0.KJlUr-KVcxfr1iXmDC_l_5fxdTfMb9WZ4lBBKls8b6x0cbpqC-5R69jTgwgxHe107jmn_JUjxk51q8WWN2FHaw")
                .url(INIT_VALID_FOLD_ID_TASK)
                .get()
                .build();
        Response instance_execute = okHttpClient.newCall(instance_build).execute();
        String instance_result = instance_execute.body().string();
        System.out.println(instance_result);
        ResponseFoldInfo instance_responseFoldInfo = gson.fromJson(instance_result, ResponseFoldInfo.class);

        for (FoldSchema datum : instance_responseFoldInfo.data) {
            schemaArrayStack.push(datum);
        }

        while (!schemaArrayStack.isEmpty()) {
            FoldSchema pop = schemaArrayStack.pop();

            validFoldId.get("taskValidFoldId").add(pop.id);

            if (pop.childNode != null && pop.childNode.length > 0) {
                for (int i = 0; i < pop.childNode.length; i++) {
                    schemaArrayStack.push(pop.childNode[i]);
                }
            }
        }

        System.out.println("==============================>>>initValidFoldId End<<<==============================");
    }

    /////////////////////////////////////////////////////////////
    private static final class StaticNestedInstance {
        private static final AutoCreateProjectSchedule instance = new AutoCreateProjectSchedule();
    }

    public static AutoCreateProjectSchedule getInstance() {
        return AutoCreateProjectSchedule.StaticNestedInstance.instance;
    }

    private Object readResolve() throws ObjectStreamException {
        return AutoCreateProjectSchedule.StaticNestedInstance.instance;
    }


}

/**
 * ====================================================================
 * 描述:
 * <p>
 * {
 * "result": true,
 * "msg": null,
 * "resultcode": null,
 * "data": [
 * {
 * "id": "SP202006040003",
 * "name": "! 3-4 L10ANDL5ProductionUpphSprintFour",
 * "params": {
 * "dprunclass": "com.foxconn.dpm.sprint4.dws_ads.L10ANDL5ProductionUpphSprintFour",
 * "dpmaster": "yarn",
 * "dpappName": "L10ANDL5ProductionUpphSprintFour",
 * "pmClass": "com.tm.dl.javasdk.dpspark.common.ProdPermissionManager",
 * "dpType": "scheduling",
 * "dpuserid": "41736e50-883d-42e0-a484-0633759b92"
 * },
 * "queue": "dp_queue2",
 * "maxExecutors": 3,
 * "minExecutors": 1,
 * "executoridleTimeout": 10,
 * "schedulerbacklogTimeout": 30,
 * "folderId": "6fbaa726-88fb-4d00-9acc-f2d86bcadf7c",
 * "businessid": "! 3-4 L10ANDL5ProductionUpphSprintFour",
 * "isActive": true,
 * "description": "",
 * "editor": "F20DL",
 * "editDate": 1591260436000,
 * "driverMemory": "1",
 * "executorMemory": "1",
 * "dependUser": "",
 * "dependItem": ""
 * }
 * ],
 * "stacktrace": null
 * }
 * ====================================================================
 */
@Data
class ResponseProjectParamsBean extends PublicResponseHeader implements Serializable {
    public ArrayList<Project> data;

    class Project {
        public String id;
        public String name;
        public Params params;
        public String queue;
        public Integer maxExecutors;
        public Integer minExecutors;
        public Integer executoridleTimeout;
        public Integer schedulerbacklogTimeout;
        public String folderId;
        public String businessid;
        public Boolean isActive;
        public String description;
        public String editor;
        public Long editDate;
        public String driverMemory;
        public String executorMemory;
        public String dependUser;
        public String dependItem;

    }
}

@Data
class Params {
    public String dprunclass;
    public String dpmaster = "yarn";
    public String dpappName;
    public String pmClass = "com.tm.dl.javasdk.dpspark.common.ProdPermissionManager";
    public String dpType = "scheduling";
    public String dpuserid = AutoCreateProjectSchedule.DP_USER_ID;
}

@Data
class ResponseProjectRegisterID extends PublicResponseHeader implements Serializable {
    public String data;
}

abstract class PublicResponseHeader {
    public String result;
    public String msg;
    public String resultcode;
    public String stacktrace;
}

@Data
class SubmitSparkTaskParams {
    public String id;
    public String name;
    public String businessid;
    public String description = "API AUTO CREATE TASK.";
    public String editDate;
    public String editor;
    public Integer executoridleTimeout = 10;
    public String folderId;
    public Boolean isActive = true;
    public Integer maxExecutors = 3;
    public String params;
    public String queue = "dp_queue";
    public Integer schedulerbacklogTimeout = 30;
    public String mainClass = "com.tm.dl.javasdk.dpspark.DPSparkApp";
    public Boolean fileIsUpload = true;
    public String filename;
    public String hdfsPath;
    public Integer driverMemory = 1;
    public Integer executorMemory = 1;
    public String dependItem;
    public String dependUser;
}

@Data
class SubmitBusinessSparkTaskParams {
    public String taskId;
    public String folderId;
    public String jobGroup;
    public String id;
    public String scheduleParams;
    public ArrayList<HashMap<String, String>> childSchedule;
    public String name;
    public String taskModel = "data_collection";
    public String taskModelText = "数据采集";
    public String taskType = "spark";
    public String taskName;
    public String taskTypeText = "Spark 任务";
    public String jobHandler = "InvokeThirdPartyHandler";
    public String executorBlockStrategy = "DISCARD_LATER";
    public String routingStrategy = "FIRST";
    public String retriesNumber = "2";
    public String taskTimeout = "600";
    public String cron;
    public String description;
    public String childScheduleId;

}

@Data
class ResponseGroupConfig extends PublicResponseHeader {
    public HashMap<String, String> data = new HashMap<>();
}