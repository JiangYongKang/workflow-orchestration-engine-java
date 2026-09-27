package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception;

/**
 * 重复触发：任务正在执行，且策略为 REJECT。
 */
public class DuplicateTriggerException extends OrchestrationException {
    public DuplicateTriggerException(String message) {
        super(message);
    }
}
