package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception;

/**
 * 任务当前状态不允许触发（例如 FAILED / BLOCKED，需走恢复流程）。
 */
public class TaskNotRunnableException extends OrchestrationException {
    public TaskNotRunnableException(String message) {
        super(message);
    }
}
