package com.foxconn.dpm.util.inject;

import org.springframework.core.DefaultParameterNameDiscoverer;
import sun.misc.ProxyGenerator;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.UUID;

/**
 * @author HS
 * @className BeanStruct
 * @description 通过继承该类获得方法参数列表对类属性的默认值处理
 * <p>
 * <p>
 * public class TestBean extends BeanStruct<TestBean> implements BeanStructInterface {
 * private Float number_1;
 * @Override public TestBean BEAN(String number_1) {
 * this.number_1 = Float.valueOf(number_1);
 * return this;
 * }
 * }
 * interface BeanStructInterface {
 * public BeanStructInterface BEAN(String number_1);
 * }
 * <p>
 * BeanStructInterface struct = BeanStruct.<BeanStructInterface>getInstance(new TestBean()).BEAN("0=1");
 * @date 2020/5/22 16:41
 */
public abstract class BeanStruct<T> implements InvocationHandler {
    private DefaultParameterNameDiscoverer defaultParameterNameDiscoverer = new DefaultParameterNameDiscoverer();
    private HashMap<String, Field> beansFieldMeta = new HashMap<>();
    private HashMap<String, Method> beansMethodMeta = new HashMap<>();


    protected BeanStruct() {
        for (Field field : this.getClass().getDeclaredFields()) {
            beansFieldMeta.put(field.getName(), field);
        }
        for (Method method : this.getClass().getDeclaredMethods()) {
            beansMethodMeta.put(method.getName(), method);
        }
        nullFieldInitDeftValue();
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        method.setAccessible(true);
        String invokeMethodName = method.getName();

        if (invokeMethodName.matches("^set[A-Z$_]*$")) {
            if (beansFieldMeta.keySet().contains(upperFirstChar(invokeMethodName.substring(3)))) {
                catchExceptionDeftValue(defaultParameterNameDiscoverer.getParameterNames(beansMethodMeta.get(invokeMethodName)), method.getParameters(), args);
                method.invoke((T) this, args);
            }
        } else if (invokeMethodName.equals("BEAN")) {
            catchExceptionDeftValue(defaultParameterNameDiscoverer.getParameterNames(beansMethodMeta.get(invokeMethodName)), method.getParameters(), args);
            method.invoke((T) this, args);
        }
        return this;
    }

    public void nullFieldInitDeftValue() {
        try {

            for (Field field : this.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                if (field.get(this) == null) {
                    field.set(this, getTypeDeftValue(field.getType().getSimpleName()));
                }
            }
        } catch (Exception e) {

        }
    }

    public synchronized <T> T getProxyInstance(Class<?>... interfaces) {
        return (T) udCProxyInstance(ClassLoader.getSystemClassLoader(), interfaces, (InvocationHandler) this);

    }

    /*
     * ====================================================================
     * 描述:
     *
     *     【注意】：该方法判别参数列表的默认属性类型凭据为
     *              该方法对应的参数名和类的属性名称一致
     * ====================================================================
     */
    private void catchExceptionDeftValue(String[] parametersNames, Parameter[] parameters, Object[] args) throws ParseException {
        for (int i = 0; i < args.length; i++) {
            Field field = beansFieldMeta.get(parametersNames[i]);
            if (field == null) {
                continue;
            }
            Object stringTargetTypeObj = getStringTargetTypeObj(field.getType().getSimpleName(), String.valueOf(args[i]));
            if (stringTargetTypeObj == null) {
                args[i] = getTypeDeftValue(this.getClass().getDeclaredFields()[i].getType().getSimpleName());
            } else {
                args[i] = stringTargetTypeObj;
            }
            args[i] = getStringTargetTypeObj(parameters[i].getType().getSimpleName(), String.valueOf(args[i]));
        }
    }

    private Object getTypeDeftValue(String typeName) throws ParseException {
        switch (typeName) {
            case "Float":
                return 0.0f;
            case "Double":
                return 0.0d;
            case "Long":
                return 0L;
            case "Integer":
                return 0;
            case "Boolean":
                return false;
            case "BigDecimal":
                return new BigDecimal("0");
            case "Timestamp":
                return Timestamp.valueOf("0");
            case "Date":
                return new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
            default:
                return null;
        }
    }

    private String upperFirstChar(String word) {
        return word.substring(0, 1).toUpperCase() + word.substring(1, word.length());
    }

    private Object getStringTargetTypeObj(String typeSimpleName, String value) {

        //Pre Clean Number
        /*try {
            switch (typeSimpleName) {
                case "Integer":
                case "Long":
                    value = value.replaceAll("[^\\d]+", "");
                    break;
                case "Float":
                case "Double":
                    value = value.replaceAll("[^\\d.]+", "");
                    break;
            }
        } catch (Exception e) {
        }*/
        //Change Number Instance
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

    private Object udCProxyInstance(ClassLoader loader, Class<?>[] interfaces, InvocationHandler h) {
        String proxyClassName = "com.sun.proxy.$Proxy" + UUID.randomUUID().toString().replace("-", "");
        byte[] bytes = ProxyGenerator.generateProxyClass(proxyClassName, interfaces.clone(), 17);
        Method[] declaredMethods = Proxy.class.getDeclaredMethods();
        int i = 0;
        for (i = 0; i < declaredMethods.length; i++) {
            if (declaredMethods[i].getName().equals("defineClass0")) {
                break;
            }
        }
        if (i == declaredMethods.length) {
            return null;
        }
        declaredMethods[i].setAccessible(true);
        try {
            Constructor<?> constructor = ((Class<?>) declaredMethods[i].invoke(null, ClassLoader.getSystemClassLoader(), proxyClassName, bytes, 0, bytes.length)).getConstructor(new Class[]{InvocationHandler.class});
            constructor.setAccessible(true);
            return constructor.newInstance(new Object[]{h});
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }

}
