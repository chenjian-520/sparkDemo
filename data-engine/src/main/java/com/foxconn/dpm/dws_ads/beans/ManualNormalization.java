package com.foxconn.dpm.dws_ads.beans;

/**
 * @author HS
 * @className ManualNormalization
 * @description TODO
 * @date 2019/12/27 18:33
 */
public class ManualNormalization {
    String Key;
    String Level;
    Double Normalization;
    Double Normalization_BTO;
    Double Normalization_CTO;

    public ManualNormalization() {
    }

    public ManualNormalization(String key, String level, Double normalization, Double normalization_BTO, Double normalization_CTO) {
        Key = key;
        Level = level;
        Normalization = normalization;
        Normalization_BTO = normalization_BTO;
        Normalization_CTO = normalization_CTO;
    }

    public String getKey() {
        return Key;
    }

    public void setKey(String key) {
        Key = key;
    }

    public String getLevel() {
        return Level;
    }

    public void setLevel(String level) {
        Level = level;
    }

    public Double getNormalization() {
        return Normalization;
    }

    public void setNormalization(Double normalization) {
        Normalization = normalization;
    }

    public Double getNormalization_BTO() {
        return Normalization_BTO;
    }

    public void setNormalization_BTO(Double normalization_BTO) {
        Normalization_BTO = normalization_BTO;
    }

    public Double getNormalization_CTO() {
        return Normalization_CTO;
    }

    public void setNormalization_CTO(Double normalization_CTO) {
        Normalization_CTO = normalization_CTO;
    }
}
