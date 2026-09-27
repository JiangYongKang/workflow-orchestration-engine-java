package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.validation;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.CyclicDependencyException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.DuplicateTaskException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.MissingDependencyException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.WorkflowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 工作流定义校验：重复注册、依赖缺失、循环依赖，并产出拓扑顺序。
 *
 * <p>三类非法情况使用三种可区分异常，调用方可按异常类型分别处理：
 * <ul>
 *     <li>{@link DuplicateTaskException}：同一 taskId 注册超过一次；</li>
 *     <li>{@link MissingDependencyException}：依赖指向了不存在的任务；</li>
 *     <li>{@link CyclicDependencyException}：依赖图中存在环。</li>
 * </ul>
 */
@Component
public class WorkflowValidator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidator.class);

    /**
     * 校验工作流定义，返回 taskId -&gt; 任务定义 的有序映射。
     * 校验失败立即抛出对应异常，绝不返回部分结果。
     */
    public Map<String, TaskDefinition> validate(WorkflowDefinition definition) {
        Map<String, TaskDefinition> tasks = new LinkedHashMap<>();
        // 1. 重复注册检测
        for (TaskDefinition task : definition.getTasks()) {
            if (tasks.containsKey(task.getTaskId())) {
                log.warn("[校验] 拒绝注册工作流 workflowId={}：任务 taskId={} 重复注册",
                        definition.getWorkflowId(), task.getTaskId());
                throw new DuplicateTaskException(
                        "任务重复注册: taskId=" + task.getTaskId());
            }
            tasks.put(task.getTaskId(), task);
        }
        // 2. 依赖缺失检测（同时对单个任务声明中的重复依赖也明确拒绝）
        for (TaskDefinition task : definition.getTasks()) {
            Set<String> seen = new HashSet<>();
            for (String dep : task.getDependencies()) {
                if (!seen.add(dep)) {
                    log.warn("[校验] 拒绝注册：任务 taskId={} 重复声明依赖 dep={}",
                            task.getTaskId(), dep);
                    throw new DuplicateTaskException(
                            "任务 " + task.getTaskId() + " 重复声明依赖: " + dep);
                }
                if (!tasks.containsKey(dep)) {
                    log.warn("[校验] 拒绝注册工作流 workflowId={}：任务 taskId={} 的依赖 dep={} 不存在",
                            definition.getWorkflowId(), task.getTaskId(), dep);
                    throw new MissingDependencyException(
                            "任务 " + task.getTaskId() + " 依赖缺失: " + dep);
                }
            }
        }
        // 3. 循环依赖检测（DFS 三色标记）
        detectCycle(definition.getWorkflowId(), tasks);
        log.info("[校验] 工作流 workflowId={} 校验通过，共 {} 个任务",
                definition.getWorkflowId(), tasks.size());
        return tasks;
    }

    private void detectCycle(String workflowId, Map<String, TaskDefinition> tasks) {
        // 0=未访问 1=在当前递归栈中 2=已完成
        Map<String, Integer> color = new HashMap<>();
        for (String id : tasks.keySet()) {
            color.put(id, 0);
        }
        Deque<String> stack = new ArrayDeque<>();
        for (String id : tasks.keySet()) {
            if (color.get(id) == 0) {
                dfs(workflowId, id, tasks, color, stack);
            }
        }
    }

    private void dfs(String workflowId,
                     String node,
                     Map<String, TaskDefinition> tasks,
                     Map<String, Integer> color,
                     Deque<String> stack) {
        color.put(node, 1);
        stack.push(node);
        for (String dep : tasks.get(node).getDependencies()) {
            int depColor = color.get(dep);
            if (depColor == 1) {
                // 从栈中还原环路径
                List<String> cycle = new ArrayList<>();
                cycle.add(dep);
                for (String n : stack) {
                    cycle.add(n);
                    if (n.equals(dep)) {
                        break;
                    }
                }
                java.util.Collections.reverse(cycle);
                log.warn("[校验] 拒绝注册工作流 workflowId={}：检测到循环依赖 path={}",
                        workflowId, String.join(" -> ", cycle));
                throw new CyclicDependencyException(
                        "检测到循环依赖: " + String.join(" -> ", cycle));
            }
            if (depColor == 0) {
                dfs(workflowId, dep, tasks, color, stack);
            }
        }
        color.put(node, 2);
        stack.pop();
    }

    /**
     * 基于入度的 Kahn 拓扑排序，返回满足依赖顺序的任务 id 序列。
     * 调用前必须已通过 {@link #validate} 校验（无环、无缺失依赖）。
     */
    public List<String> topoSort(WorkflowDefinition definition, Map<String, TaskDefinition> tasks) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, Set<String>> dependents = new HashMap<>();
        for (String id : tasks.keySet()) {
            indegree.put(id, 0);
            dependents.put(id, new LinkedHashSet<>());
        }
        for (TaskDefinition task : tasks.values()) {
            for (String dep : task.getDependencies()) {
                indegree.merge(task.getTaskId(), 1, Integer::sum);
                dependents.get(dep).add(task.getTaskId());
            }
        }
        Deque<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> e : indegree.entrySet()) {
            if (e.getValue() == 0) {
                queue.add(e.getKey());
            }
        }
        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            order.add(id);
            for (String down : dependents.get(id)) {
                int left = indegree.merge(down, -1, Integer::sum);
                if (left == 0) {
                    queue.add(down);
                }
            }
        }
        if (order.size() != tasks.size()) {
            // 理论不可达：validate 已保证无环；保留作为防御性检查
            throw new CyclicDependencyException("拓扑排序失败：依赖图中存在环");
        }
        return order;
    }
}
