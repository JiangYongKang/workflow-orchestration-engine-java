package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 一次可追溯的运行事件（状态流转、重试、阻断、恢复等）。
 */
public record RunEvent(
        long timestampMillis,
        String taskId,
        TaskState fromState,
        TaskState toState,
        int attempt,
        String message
) {
}
