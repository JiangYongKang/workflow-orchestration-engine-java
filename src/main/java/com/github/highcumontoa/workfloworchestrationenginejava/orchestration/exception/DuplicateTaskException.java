package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception;

/**
 * 重复注册：同一 taskId 被注册多次。
 */
public class DuplicateTaskException extends OrchestrationException {
    public DuplicateTaskException(String message) {
        super(message);
    }
}
