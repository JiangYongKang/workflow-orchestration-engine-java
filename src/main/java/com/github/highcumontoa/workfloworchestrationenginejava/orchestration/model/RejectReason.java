package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 编排引擎拒绝一次注册/触发/恢复请求的可区分原因。
 */
public enum RejectReason {
    /** 同一任务 ID 被重复注册 */
    DUPLICATE_REGISTRATION,
    /** 任务声明了工作流中不存在的依赖 */
    MISSING_DEPENDENCY,
    /** 依赖关系中存在环 */
    CYCLIC_DEPENDENCY,
    /** 已有同工作流的运行在执行中，且触发策略为 REJECT */
    TRIGGER_REJECTED_ALREADY_RUNNING,
    /** 运行尚未结束，不能执行恢复 */
    RUN_NOT_TERMINAL,
    /** 运行不存在失败任务，无需恢复 */
    RECOVERY_NOT_APPLICABLE
}
