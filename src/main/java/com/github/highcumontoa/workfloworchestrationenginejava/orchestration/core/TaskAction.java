package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core;

/**
 * 任务的本地执行动作，纯本地模拟，不依赖外部服务。
 */
@FunctionalInterface
public interface TaskAction {
    void execute(TaskContext context) throws Exception;
}
