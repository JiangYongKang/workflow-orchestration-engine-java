package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

import java.time.Instant;

/**
 * 任务状态流转事件，用于审计与可追溯。
 */
public record TaskEvent(String taskId,
                        int generation,
                        TaskStatus fromStatus,
                        TaskStatus toStatus,
                        String detail,
                        Instant timestamp) {
}
