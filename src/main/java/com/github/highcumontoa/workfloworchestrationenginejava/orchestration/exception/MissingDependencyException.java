package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception;

/**
 * 依赖缺失：任务声明的依赖在工作流中不存在。
 */
public class MissingDependencyException extends OrchestrationException {
    public MissingDependencyException(String message) {
        super(message);
    }
}
