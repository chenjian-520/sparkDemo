package com.foxconn.dpm.util.inject;

public interface BeanStructInterface<T> {
    public T BEAN(Object... arguments);
    public T init();
}

