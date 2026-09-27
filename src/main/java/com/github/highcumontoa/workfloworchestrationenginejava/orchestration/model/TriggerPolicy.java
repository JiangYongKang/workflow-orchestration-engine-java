package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 同一工作流存在执行中的运行时，再次触发所采用的策略。
 */
public enum TriggerPolicy {
    /** 合并：等待并复用正在执行的同一次运行，不重复执行任务 */
    MERGE,
    /** 拒绝：直接以 {@code TRIGGER_REJECTED_ALREADY_RUNNING} 原因拒绝本次触发 */
    REJECT
}
