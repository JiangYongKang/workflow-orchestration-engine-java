package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 任务最终失败 / 被阻断时的可区分原因码。
 */
public enum FailureReason {
    NONE,
    RETRY_EXHAUSTED,
    UPSTREAM_FAILED,
    UPSTREAM_BLOCKED,
    RESUME_STILL_FAILING
}
