package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 任务定义：标识、依赖、重试策略与本地执行动作。
 */
public final class TaskDefinition {

    /** 默认不重试：只执行一次。 */
    public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.of(1);

    private final String id;
    private final Set<String> dependencies;
    private final RetryPolicy retryPolicy;
    private final TaskAction action;

    public TaskDefinition(String id, Set<String> dependencies, RetryPolicy retryPolicy, TaskAction action) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("任务 ID 不能为空");
        }
        this.id = id;
        this.dependencies = dependencies == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new HashSet<>(dependencies));
        this.retryPolicy = retryPolicy == null ? DEFAULT_RETRY_POLICY : retryPolicy;
        this.action = Objects.requireNonNull(action, "任务执行动作不能为空: " + id);
    }

    public TaskDefinition(String id, TaskAction action) {
        this(id, Collections.emptySet(), DEFAULT_RETRY_POLICY, action);
    }

    public TaskDefinition(String id, RetryPolicy retryPolicy, TaskAction action) {
        this(id, Collections.emptySet(), retryPolicy, action);
    }

    public String getId() {
        return id;
    }

    public Set<String> getDependencies() {
        return dependencies;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public TaskAction getAction() {
        return action;
    }
}
