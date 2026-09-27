package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception;

/**
 * 触发或查询的任务不存在。
 */
public class UnknownTaskException extends OrchestrationException {
    public UnknownTaskException(String message) {
        super(message);
    }
}
