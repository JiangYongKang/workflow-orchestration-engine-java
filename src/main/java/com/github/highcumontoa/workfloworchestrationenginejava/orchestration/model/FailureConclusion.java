package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

import java.util.Set;

/**
 * 重试耗尽后保留的可查询失败结论。
 */
public record FailureConclusion(
        String failedTaskId,
        String errorMessage,
        int attempts,
        Set<String> blockedTasks
) {
}
