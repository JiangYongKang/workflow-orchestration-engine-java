package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 任务在一次编排运行中的生命周期状态。
 */
public enum TaskState {
    /** 已注册/等待调度，或等待前置依赖完成 */
    PENDING,
    /** 正在执行（含本轮重试的全部尝试） */
    RUNNING,
    /** 执行成功（终态，恢复重放时不会再次执行） */
    SUCCEEDED,
    /** 重试次数用尽后的失败终态 */
    FAILED,
    /** 因上游失败而被阻断、未执行 */
    BLOCKED
}
