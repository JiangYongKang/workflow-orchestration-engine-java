package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 任务在编排引擎中的生命周期状态。
 */
public enum TaskStatus {
    PENDING,
    WAITING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    BLOCKED
}
