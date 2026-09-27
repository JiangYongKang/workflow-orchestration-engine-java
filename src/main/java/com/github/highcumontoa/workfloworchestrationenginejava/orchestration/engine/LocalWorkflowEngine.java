package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.engine;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.OrchestrationException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskContext;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.Workflow;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.WorkflowRun;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.FailureConclusion;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskState;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 纯本地任务编排引擎（不依赖任何外部服务）。
 *
 * <p>三大保证：
 * <ol>
 *     <li><b>非法编排显式拒绝</b>：触发前做重复注册/缺失依赖/循环依赖校验，
 *     原因见 {@link RejectReason}，绝不部分执行或静默跳过；</li>
 *     <li><b>并发触发不重复执行</b>：同一工作流同时刻只有一个执行中的
 *     {@link WorkflowRun}；{@link TriggerPolicy#MERGE} 合并到在途运行（等其结束后复用其结论），
 *     {@link TriggerPolicy#REJECT} 以 {@code TRIGGER_REJECTED_ALREADY_RUNNING} 拒绝；</li>
 *     <li><b>失败重试、阻断与可恢复</b>：任务按 {@code maxAttempts} 重试；
 *     重试耗尽则阻断全部下游、固化 {@link FailureConclusion}；
 *     {@link #recover(WorkflowRun)} 从失败点重放，已成功任务跳过不重复产生副作用。</li>
 * </ol>
 */
public final class LocalWorkflowEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LocalWorkflowEngine.class);

    /** 工作流 ID -> 当前执行中的运行；存在映射即代表该工作流在途，天然保证单飞。 */
    private final ConcurrentHashMap<String, WorkflowRun> inFlight = new ConcurrentHashMap<>();
    /** 每个工作流一把发布锁：把“检查在途 -> 决策 -> 发布新运行”收敛为原子区。等待动作在锁外完成。 */
    private final ConcurrentHashMap<String, Object> workflowLocks = new ConcurrentHashMap<>();
    private final AtomicLong runSequence = new AtomicLong();

    public LocalWorkflowEngine() {
    }

    /**
     * 触发一次工作流运行。
     *
     * <p>非法编排（循环依赖/依赖缺失/重复注册）在任何任务执行之前抛出异常。
     * 并发触发同一工作流时按 {@code policy} 合并或拒绝。
     */
    public WorkflowRun trigger(Workflow workflow, TriggerPolicy policy) {
        if (workflow == null) {
            throw new IllegalArgumentException("工作流不能为空");
        }
        TriggerPolicy effective = policy == null ? TriggerPolicy.MERGE : policy;
        String workflowId = workflow.getId();

        // 校验在发布到在途表之前完成：非法编排绝不占用执行槽、绝不执行任何任务
        List<String> order = workflow.topologicalOrder();

        Object lock = workflowLocks.computeIfAbsent(workflowId, k -> new Object());
        WorkflowRun run = null;
        WorkflowRun toAwait = null;
        synchronized (lock) {
            WorkflowRun existing = inFlight.get(workflowId);
            if (existing != null) {
                if (effective == TriggerPolicy.REJECT) {
                    log.warn("[拒绝] workflow={} 在途运行 run={} 正在执行，按 REJECT 策略拒绝本次触发（判定依据：inFlight 表存在该工作流映射）",
                            workflowId, existing.getRunId());
                    throw new OrchestrationException(RejectReason.TRIGGER_REJECTED_ALREADY_RUNNING,
                            "触发被拒绝: workflow=" + workflowId + " 已有执行中的运行 " + existing.getRunId());
                }
                toAwait = existing;
            } else {
                // 构造即“执行中”(done=false)，发布后到达的 MERGE 触发会正确等待，不存在提前返回窗口
                run = new WorkflowRun(newRunId(workflowId), workflow);
                inFlight.put(workflowId, run);
            }
        }

        if (toAwait != null) {
            log.info("[合并] workflow={} 本次触发合并到在途运行 run={}，等待其终态后复用结论（不重复执行任何任务）",
                    workflowId, toAwait.getRunId());
            toAwait.awaitCompletion();
            return toAwait;
        }

        log.info("[触发] workflow={} run={} policy={} 抢占执行槽成功，开始执行",
                workflowId, run.getRunId(), effective);
        execute(run, order, false, "INIT");
        return run;
    }

    /**
     * 从失败点恢复一次已结束的运行：校验在恢复前完成；SUCCEEDED 任务保持成功并跳过，
     * FAILED/BLOCKED/PENDING 任务在新阶段按拓扑顺序重新执行（重试次数重新计算）。
     */
    public WorkflowRun recover(WorkflowRun run) {
        if (run == null) {
            throw new IllegalArgumentException("待恢复的运行不能为空");
        }
        Workflow workflow = run.getWorkflow();
        String workflowId = workflow.getId();

        // 恢复前再次校验拓扑，定义被改成非法时明确拒绝而非部分执行
        List<String> order = workflow.topologicalOrder();

        Object lock = workflowLocks.computeIfAbsent(workflowId, k -> new Object());
        synchronized (lock) {
            if (!run.isDone()) {
                throw new OrchestrationException(RejectReason.RUN_NOT_TERMINAL,
                        "恢复被拒绝: run=" + run.getRunId() + " 当前阶段 " + run.getPhaseName() + " 尚未结束");
            }
            if (run.succeeded()) {
                throw new OrchestrationException(RejectReason.RECOVERY_NOT_APPLICABLE,
                        "恢复被拒绝: run=" + run.getRunId() + " 已全部成功，不存在失败任务");
            }
            if (run.failureConclusion() == null) {
                throw new OrchestrationException(RejectReason.RECOVERY_NOT_APPLICABLE,
                        "恢复被拒绝: run=" + run.getRunId() + " 不存在失败结论");
            }
            WorkflowRun existing = inFlight.get(workflowId);
            if (existing != null) {
                throw new OrchestrationException(RejectReason.RUN_NOT_TERMINAL,
                        "恢复被拒绝: workflow=" + workflowId + " 存在执行中的运行 " + existing.getRunId());
            }
            // 先翻转为执行中再发布，杜绝“已发布但仍是终态”窗口被并发触发误判
            run.markInFlightForRecovery();
            inFlight.put(workflowId, run);
        }

        log.info("[恢复] workflow={} run={} 从失败点恢复：已成功任务将跳过，失败点及其下游重新执行",
                workflowId, run.getRunId());
        execute(run, order, true, "RECOVERY-" + System.currentTimeMillis());
        return run;
    }

    private void execute(WorkflowRun run, List<String> order, boolean recovery, String phaseName) {
        // execute 仅由持有执行槽的线程调用；恢复与初始触发分别在调用方负责移除执行槽，
        // 这里统一在 finally 中防御性移除，确保异常路径也不会永久占槽。
        String workflowId = run.getWorkflow().getId();
        try {
            run.beginPhase(phaseName, recovery);
            // 同一阶段内全部任务、同一任务的全部尝试共享一个数据区，便于任务间传递本地产物
            Map<String, Object> sharedData = new ConcurrentHashMap<>();

            for (String taskId : order) {
                TaskState state = run.getState(taskId);
                if (state == TaskState.BLOCKED) {
                    // 已因上游失败被阻断（下游标记在重试耗尽时完成），直接跳过
                    continue;
                }
                if (recovery && state == TaskState.SUCCEEDED) {
                    // 恢复重放：已成功任务不重复执行、不重复产生副作用
                    run.transition(taskId, TaskState.SUCCEEDED, 0,
                            "恢复重放时跳过：任务已在上一阶段成功，副作用不重复");
                    log.info("[跳过] run={} phase={} task={} 已在上一阶段成功，恢复重放时跳过（副作用不重复）",
                            run.getRunId(), phaseName, taskId);
                    continue;
                }
                executeTaskWithRetries(run, taskId, sharedData);
            }
            run.finishTerminal();
            if (run.succeeded()) {
                log.info("[完成] run={} phase={} 全部任务成功", run.getRunId(), phaseName);
            } else {
                FailureConclusion c = run.failureConclusion();
                log.warn("[完成] run={} phase={} 重试耗尽任务={} 阻断下游={} 错误={}",
                        run.getRunId(), phaseName, c.failedTaskId(), c.blockedTasks(), c.errorMessage());
            }
        } finally {
            inFlight.remove(workflowId, run);
        }
    }

    private void executeTaskWithRetries(WorkflowRun run, String taskId, Map<String, Object> sharedData) {
        Workflow workflow = run.getWorkflow();
        TaskDefinition definition = workflow.getTask(taskId);
        int maxAttempts = definition.getRetryPolicy().maxAttempts();

        run.transition(taskId, TaskState.RUNNING, run.getAttempts(taskId),
                "进入执行态，maxAttempts=" + maxAttempts);
        Throwable lastFailure = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            run.recordAttemptStart(taskId);
            run.markInvocation(taskId);
            TaskContext context = new TaskContext(run.getRunId(), attempt, sharedData);
            try {
                log.info("[执行] run={} phase={} task={} attempt={}/{} 开始执行",
                        run.getRunId(), run.getPhaseName(), taskId, attempt, maxAttempts);
                definition.getAction().execute(context);
                run.transition(taskId, TaskState.SUCCEEDED, attempt,
                        "第 " + attempt + " 次尝试执行成功");
                log.info("[成功] run={} task={} attempt={} 执行成功，累计副作用调用次数={}",
                        run.getRunId(), taskId, attempt, run.getInvocationCount(taskId));
                return;
            } catch (Throwable t) {
                lastFailure = t;
                String message = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
                run.recordError(taskId, message);
                if (attempt < maxAttempts) {
                    log.warn("[重试] run={} task={} attempt={}/{} 失败: {} —— 将重试",
                            run.getRunId(), taskId, attempt, maxAttempts, message, t);
                } else {
                    log.warn("[耗尽] run={} task={} attempt={}/{} 失败: {} —— 重试次数用尽",
                            run.getRunId(), taskId, attempt, maxAttempts, message, t);
                }
            }
        }

        // 重试耗尽：固化失败结论，阻断其所有下游（直接与间接）
        Set<String> blocked = collectDownstream(workflow, taskId);
        String errorMessage = lastFailure == null ? "unknown"
                : (lastFailure.getMessage() == null ? lastFailure.getClass().getSimpleName() : lastFailure.getMessage());
        FailureConclusion conclusion = new FailureConclusion(
                taskId, errorMessage, maxAttempts, Set.copyOf(blocked));
        run.transition(taskId, TaskState.FAILED, maxAttempts,
                "重试次数用尽（" + maxAttempts + " 次尝试均失败），下游将被阻断");
        run.attachFailureConclusion(conclusion);
        for (String blockedTask : blocked) {
            markBlocked(run, blockedTask, errorMessage);
        }
    }

    private void markBlocked(WorkflowRun run, String taskId, String reason) {
        if (run.getState(taskId) == TaskState.BLOCKED) {
            return;
        }
        run.transition(taskId, TaskState.BLOCKED, 0,
                "上游失败，阻断执行: " + reason);
        log.info("[阻断] run={} task={} 因上游失败被阻断，不产生任何副作用（累计调用次数保持 {}）",
                run.getRunId(), taskId, run.getInvocationCount(taskId));
    }

    /** 收集失败任务的全部直接与间接下游（按依赖关系反向可达）。 */
    private Set<String> collectDownstream(Workflow workflow, String failedTaskId) {
        Map<String, Set<String>> downstream = new ConcurrentHashMap<>();
        for (String id : workflow.getTasks().keySet()) {
            downstream.put(id, new HashSet<>());
        }
        for (TaskDefinition task : workflow.getTasks().values()) {
            for (String dep : task.getDependencies()) {
                downstream.get(dep).add(task.getId());
            }
        }
        Set<String> result = new HashSet<>();
        java.util.Deque<String> stack = new java.util.ArrayDeque<>(downstream.get(failedTaskId));
        while (!stack.isEmpty()) {
            String current = stack.pop();
            if (result.add(current)) {
                stack.addAll(downstream.get(current));
            }
        }
        return result;
    }

    private String newRunId(String workflowId) {
        return workflowId + "#run" + runSequence.incrementAndGet();
    }

    @Override
    public void close() {
        inFlight.clear();
        log.info("[关闭] 本地编排引擎已关闭");
    }
}
