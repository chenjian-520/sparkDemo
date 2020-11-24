package com.foxconn.dpm.common.testdata;

import com.alibaba.fastjson.JSONObject;
import com.foxconn.dpm.common.testdata.bean.PostBean;
import com.foxconn.dpm.common.testdata.http.HttpClientUtil;
import com.foxconn.dpm.sprint5.ods_dwd.bean.ODSProductionSfcTpmLineMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author HY
 * @Date 2020/7/7 13:41
 * @Description TODO
 */
public class CreateTestDataTools {


    public static void main(String[] args) {
        dpm_ods_production_tpm_machine_maintenance();
    }

    private static String url = "http://10.60.136.156:8089/api/dlapiservice/v1/";


    public static void dpm_ods_production_tpm_machine_maintenance() {
        String businessId = "BC202007070001";


        ODSProductionSfcTpmLineMapping data = new ODSProductionSfcTpmLineMapping();
        data.setTpmLineCode("Aline");
        data.setAreaCode("AAAAA");
        data.setLineCode("BBBB");
        data.setRowKey("Aline");
        List<ODSProductionSfcTpmLineMapping> list = new ArrayList<>();
        list.add(data);


        PostBean postBean = PostBean.buildPostBean(businessId, list, ODSProductionSfcTpmLineMapping.class);

        String s = JSONObject.toJSONString(postBean);
        s = s.replace("null", "22222");

        System.out.println(s);
        String s1 = HttpClientUtil.sendPostByJson(url + businessId, s, 0);

        System.out.println(s1);

    }

}
