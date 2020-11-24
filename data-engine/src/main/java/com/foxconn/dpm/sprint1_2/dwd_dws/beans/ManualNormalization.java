package com.foxconn.dpm.sprint1_2.dwd_dws.beans;

import lombok.Data;

/**
 * @author HS
 * @className ManualNormalization
 * @description TODO
 * @date 2019/12/27 18:33
 */
@Data
public class ManualNormalization {
    String site_code;
    String Key;
    String Level;
    Double Normalization;
    Double Normalization_BTO;
    Double Normalization_CTO;

    public ManualNormalization() {
    }

    public ManualNormalization(String key, String level, Double normalization, Double normalization_BTO, Double normalization_CTO, String site_code) {
        this.site_code = site_code;
        this.Key = key;
        this.Level = level;
        this.Normalization = normalization;
        this.Normalization_BTO = normalization_BTO;
        this.Normalization_CTO = normalization_CTO;
    }

}
