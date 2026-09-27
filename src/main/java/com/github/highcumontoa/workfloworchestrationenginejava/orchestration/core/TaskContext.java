package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core;

import java.util.Map;

/**
 * 任务单次执行尝试的上下文。
 */
public final class TaskContext {

    private final String runId;
    private final int attempt;
    private final Map<String, Object> sharedData;

    public TaskContext(String runId, int attempt, Map<String, Object> sharedData) {
        this.runId = runId;
        this.attempt = attempt;
        this.sharedData = sharedData;
    }

    public String getRunId() {
        return runId;
    }

    /** 当前尝试序号，从 1 开始。 */
    public int getAttempt() {
        return attempt;
    }

    /** 同一次运行内任务间共享的数据区。 */
    public Map<String, Object> getSharedData() {
        return sharedData;
    }
}
