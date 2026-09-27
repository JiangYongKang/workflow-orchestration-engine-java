package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 任务的本地执行体，由调用方提供，引擎负责调度。
 */
@FunctionalInterface
public interface TaskExecution {
    void execute(TaskExecutionContext context) throws Exception;
}
