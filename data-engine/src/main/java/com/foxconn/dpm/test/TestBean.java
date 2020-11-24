package com.foxconn.dpm.test;

import com.foxconn.dpm.util.inject.BeanStruct;
import lombok.Data;

/**
 * @author HS
 * @className TestBean
 * @description TODO
 * @date 2020/5/22 17:20
 */
@Data
public class TestBean extends BeanStruct<TestBean> implements TestBeanStructInterface {
    private Float number_1;
    private Float number_2;


    public static void main(String[] args) {
        TestBean struct = new TestBean().<TestBeanStructInterface>getProxyInstance(TestBeanStructInterface.class).BEAN("0=1");
        System.out.println(struct);
        System.out.println(struct.getNumber_1());
    }

    public TestBean BEAN(String number_1) {
        this.number_1 = Float.valueOf(number_1);
        return this;
    }
}




