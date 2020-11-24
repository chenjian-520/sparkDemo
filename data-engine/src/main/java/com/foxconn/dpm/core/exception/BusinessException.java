package com.foxconn.dpm.core.exception;

import lombok.extern.slf4j.Slf4j;

/**
 * 业务异常 .
 *
 * @className: BusinessException
 * @author: ws
 * @date: 2020/7/2 14:20
 * @version: 1.0.0
 */
@Slf4j
public class BusinessException extends RuntimeException {

    /**
     * Constructs a new runtime exception with {@code null} as its
     * detail message.  The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     */
    public BusinessException() {
    }

    /**
     * Constructs a new runtime exception with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public BusinessException(String message) {
        super(message);
    }

    /**
     * 构造方法，todo 后续提取完整堆栈信息 .
     * @param e
     * @author ws
     * @date 2020/7/14 16:34
     * @return 
     **/
    public BusinessException(Exception e) {
        this(e.getMessage());
        log.error("系统异常", e);
    }
}
