package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

import java.util.List;
import java.util.Objects;

/**
 * 任务定义：标识、依赖、重试策略与本地执行体。
 */
public final class TaskDefinition {

    private final String taskId;
    private final List<String> dependencies;
    private final RetryPolicy retryPolicy;
    private final TaskExecution execution;

    public TaskDefinition(String taskId,
                          List<String> dependencies,
                          RetryPolicy retryPolicy,
                          TaskExecution execution) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId 不能为空");
        }
        this.taskId = taskId;
        this.dependencies = List.copyOf(dependencies == null ? List.of() : dependencies);
        this.retryPolicy = retryPolicy == null ? RetryPolicy.noRetry() : retryPolicy;
        this.execution = Objects.requireNonNull(execution, "任务执行体 execution 不能为空");
    }

    public String getTaskId() {
        return taskId;
    }

    public List<String> getDependencies() {
        return dependencies;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public TaskExecution getExecution() {
        return execution;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TaskDefinition that)) {
            return false;
        }
        return Objects.equals(taskId, that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(taskId);
    }
}
