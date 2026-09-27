package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception;

/**
 * 编排相关异常的基类，异常类型即可区分拒绝原因。
 */
public class OrchestrationException extends RuntimeException {

    public OrchestrationException(String message) {
        super(message);
    }

    public OrchestrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
