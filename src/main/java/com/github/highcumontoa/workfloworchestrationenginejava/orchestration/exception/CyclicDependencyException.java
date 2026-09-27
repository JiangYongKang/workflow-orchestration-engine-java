package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception;

/**
 * 循环依赖：依赖图中存在环。
 */
public class CyclicDependencyException extends OrchestrationException {
    public CyclicDependencyException(String message) {
        super(message);
    }
}
