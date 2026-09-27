package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 工作流定义：一组带依赖关系的任务。
 */
public final class WorkflowDefinition {

    private final String workflowId;
    private final List<TaskDefinition> tasks;

    public WorkflowDefinition(String workflowId, List<TaskDefinition> tasks) {
        this.workflowId = workflowId;
        this.tasks = new ArrayList<>(tasks);
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public List<TaskDefinition> getTasks() {
        return Collections.unmodifiableList(tasks);
    }
}
