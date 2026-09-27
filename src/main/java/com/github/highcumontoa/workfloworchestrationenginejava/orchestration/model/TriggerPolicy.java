package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 同一任务被并发重复触发时的合并策略。
 */
public enum TriggerPolicy {
    /** 合并到正在进行的执行上（返回同一个结果）。 */
    MERGE,
    /** 明确拒绝重复触发。 */
    REJECT
}
