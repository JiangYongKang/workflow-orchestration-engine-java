package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RejectReason;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 工作流定义：任务注册表。
 *
 * <p>注册与触发时会执行三类非法编排的显式拒绝，原因互不相同：
 * <ul>
 *     <li>{@link RejectReason#DUPLICATE_REGISTRATION}：同一任务 ID 重复注册；</li>
 *     <li>{@link RejectReason#MISSING_DEPENDENCY}：依赖了工作流中不存在的任务；</li>
 *     <li>{@link RejectReason#CYCLIC_DEPENDENCY}：依赖图中存在环（含自依赖）。</li>
 * </ul>
 */
public final class Workflow {

    private final String id;
    private final Map<String, TaskDefinition> tasks = new LinkedHashMap<>();

    public Workflow(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("工作流 ID 不能为空");
        }
        this.id = id;
    }

    public String getId() {
        return id;
    }

    /**
     * 注册任务。任务 ID 重复时立即以 {@code DUPLICATE_REGISTRATION} 拒绝，
     * 避免静默覆盖既有定义。
     */
    public Workflow register(TaskDefinition definition) {
        if (definition == null) {
            throw new IllegalArgumentException("任务定义不能为空");
        }
        String taskId = definition.getId();
        if (tasks.containsKey(taskId)) {
            throw new OrchestrationException(RejectReason.DUPLICATE_REGISTRATION,
                    "任务重复注册被拒绝: workflow=" + id + ", task=" + taskId);
        }
        tasks.put(taskId, definition);
        return this;
    }

    /**
     * 校验依赖完整性与无环性。任何一项不合法都抛出 {@link OrchestrationException}，
     * 调用方不得据此执行任何任务（引擎在调度前调用，保证不发生部分执行）。
     */
    public void validate() {
        // 1. 缺失依赖：被依赖方必须在同一工作流中已注册
        for (TaskDefinition task : tasks.values()) {
            for (String dep : task.getDependencies()) {
                if (!tasks.containsKey(dep)) {
                    throw new OrchestrationException(RejectReason.MISSING_DEPENDENCY,
                            "依赖缺失被拒绝: workflow=" + id + ", task=" + task.getId()
                                    + " 依赖了未注册的任务 " + dep);
                }
            }
        }
        // 2. 循环依赖：DFS 三色标记（0=未访问, 1=在当前递归栈, 2=已完成）
        Map<String, Integer> color = new LinkedHashMap<>();
        for (String taskId : tasks.keySet()) {
            color.put(taskId, 0);
        }
        for (String taskId : tasks.keySet()) {
            if (color.get(taskId) == 0) {
                Deque<String> stack = new ArrayDeque<>();
                dfsCycleCheck(taskId, color, stack);
            }
        }
    }

    private void dfsCycleCheck(String node, Map<String, Integer> color, Deque<String> stack) {
        color.put(node, 1);
        stack.push(node);
        for (String dep : tasks.get(node).getDependencies()) {
            int depColor = color.get(dep);
            if (depColor == 1) {
                // 从栈底到当前节点的路径，定位环
                List<String> path = new ArrayList<>(stack);
                Collections.reverse(path);
                int start = path.indexOf(dep);
                List<String> cycle = new ArrayList<>(path.subList(start, path.size()));
                cycle.add(dep);
                throw new OrchestrationException(RejectReason.CYCLIC_DEPENDENCY,
                        "循环依赖被拒绝: workflow=" + id + ", 检测到环 " + String.join(" -> ", cycle));
            }
            if (depColor == 0) {
                dfsCycleCheck(dep, color, stack);
            }
        }
        stack.pop();
        color.put(node, 2);
    }

    /**
     * 返回依赖优先的拓扑执行顺序（被依赖者先执行）。先做完整校验，
     * 不合法时抛出异常而不是返回部分顺序。
     */
    public List<String> topologicalOrder() {
        validate();
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> downstream = new LinkedHashMap<>();
        for (String taskId : tasks.keySet()) {
            indegree.put(taskId, 0);
            downstream.put(taskId, new ArrayList<>());
        }
        for (TaskDefinition task : tasks.values()) {
            indegree.put(task.getId(), task.getDependencies().size());
            for (String dep : task.getDependencies()) {
                downstream.get(dep).add(task.getId());
            }
        }
        // 按注册顺序消费入度为 0 的节点，保证顺序稳定可预期
        Deque<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> e : indegree.entrySet()) {
            if (e.getValue() == 0) {
                queue.add(e.getKey());
            }
        }
        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            order.add(current);
            for (String next : downstream.get(current)) {
                int left = indegree.merge(next, -1, Integer::sum);
                if (left == 0) {
                    queue.add(next);
                }
            }
        }
        if (order.size() != tasks.size()) {
            // 理论上 validate 已拦截环，此处为防御性检查
            throw new OrchestrationException(RejectReason.CYCLIC_DEPENDENCY,
                    "循环依赖被拒绝: workflow=" + id + ", 拓扑排序无法覆盖全部任务");
        }
        return List.copyOf(order);
    }

    public TaskDefinition getTask(String taskId) {
        return tasks.get(taskId);
    }

    public Map<String, TaskDefinition> getTasks() {
        return tasks;
    }
}
