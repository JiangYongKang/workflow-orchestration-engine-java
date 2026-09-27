package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

import java.time.Instant;

/**
 * 某个任务在一次尝试代次（generation）中的最终/当前状态记录。
 */
public final class TaskRecord {

    private final String taskId;
    private final int generation;
    private final TaskStatus status;
    private final FailureReason failureReason;
    private final int attempts;
    private final String message;
    private final Instant timestamp;

    public TaskRecord(String taskId,
                      int generation,
                      TaskStatus status,
                      FailureReason failureReason,
                      int attempts,
                      String message,
                      Instant timestamp) {
        this.taskId = taskId;
        this.generation = generation;
        this.status = status;
        this.failureReason = failureReason;
        this.attempts = attempts;
        this.message = message;
        this.timestamp = timestamp;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getGeneration() {
        return generation;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public FailureReason getFailureReason() {
        return failureReason;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getMessage() {
        return message;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "TaskRecord{" +
                "taskId='" + taskId + '\'' +
                ", generation=" + generation +
                ", status=" + status +
                ", failureReason=" + failureReason +
                ", attempts=" + attempts +
                ", message='" + message + '\'' +
                '}';
    }
}
