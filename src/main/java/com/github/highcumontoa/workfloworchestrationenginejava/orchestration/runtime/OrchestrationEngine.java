package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.runtime;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.DuplicateTriggerException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.OrchestrationException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.TaskNotRunnableException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.UnknownTaskException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.FailureReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskEvent;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskExecutionContext;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskRecord;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskStatus;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.validation.WorkflowValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地任务编排引擎。
 *
 * <p>职责：
 * <ol>
 *     <li>注册时校验工作流（重复注册 / 依赖缺失 / 循环依赖直接拒绝）；</li>
 *     <li>按依赖拓扑调度任务，同一任务并发触发按 {@link TriggerPolicy} 合并或拒绝；</li>
 *     <li>任务失败按 {@code RetryPolicy} 重试，耗尽后置 FAILED 并阻断全部下游为 BLOCKED；</li>
 *     <li>恢复重放只重置 FAILED / BLOCKED，SUCCEEDED 不重复执行，代次 +1 全程可追溯。</li>
 * </ol>
 *
 * <p>可变状态全部下沉到 {@link TaskRuntime}（监视器加锁），引擎仅做编排；
 * 调度过程不嵌套持锁，避免多任务死锁。
 */
@Component
public class OrchestrationEngine {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationEngine.class);

    private final WorkflowValidator validator;
    private final ExecutorService executor;

    private final AtomicBoolean registered = new AtomicBoolean(false);

    private String workflowId = "";
    private Map<String, TaskDefinition> definitions = Map.of();
    private Map<String, TaskRuntime> runtimes = Map.of();
    /** dep -&gt; 直接下游集合（图已校验无环）。 */
    private Map<String, Set<String>> dependents = Map.of();
    private List<String> topoOrder = List.of();

    @Autowired
    public OrchestrationEngine(WorkflowValidator validator) {
        this(validator, Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "orchestration-worker");
            t.setDaemon(true);
            return t;
        }));
    }

    OrchestrationEngine(WorkflowValidator validator, ExecutorService executor) {
        this.validator = validator;
        this.executor = executor;
    }

    // ============================== 注册 ==============================

    /**
     * 装载并校验工作流定义。任一非法情况（重复注册、依赖缺失、循环依赖）
     * 都会抛出可区分异常，且引擎保持未装载状态，不会部分生效。
     */
    public synchronized void register(WorkflowDefinition definition) {
        if (definition == null) {
            throw new OrchestrationException("工作流定义不能为空");
        }
        log.info("[引擎] 开始注册工作流 workflowId={}, 候选任务数={}",
                definition.getWorkflowId(), definition.getTasks().size());
        // validate 内部对三类错误分别抛出可区分异常
        Map<String, TaskDefinition> validated = validator.validate(definition);
        List<String> order = validator.topoSort(definition, validated);

        Map<String, TaskRuntime> rt = new LinkedHashMap<>();
        Map<String, Set<String>> down = new HashMap<>();
        for (String id : order) {
            rt.put(id, new TaskRuntime(id));
            down.put(id, new HashSet<>());
        }
        for (TaskDefinition task : validated.values()) {
            for (String dep : task.getDependencies()) {
                down.get(dep).add(task.getTaskId());
            }
        }
        this.workflowId = definition.getWorkflowId();
        this.definitions = validated;
        this.runtimes = rt;
        this.dependents = down;
        this.topoOrder = order;
        registered.set(true);
        log.info("[引擎] 工作流 workflowId={} 注册完成，拓扑顺序={}", workflowId, order);
    }

    // ============================== 触发：单任务 ==============================

    /**
     * 触发单个任务。其上游必须全部 SUCCEEDED；本方法不会隐式级联执行上游，
     * 需要级联请使用 {@link #triggerAll}。
     *
     * <p>该任务已在途（RUNNING/WAITING）时：MERGE 返回同一个在途结果；
     * REJECT 抛 {@link DuplicateTriggerException}。
     * SUCCEEDED 直接返回既有结论；FAILED/BLOCKED 抛 {@link TaskNotRunnableException}
     * 引导走恢复流程，绝不重复执行。
     */
    public CompletableFuture<TaskRecord> triggerTask(String taskId, TriggerPolicy policy) {
        ensureRegistered();
        Objects.requireNonNull(policy, "TriggerPolicy 不能为空");
        TaskRuntime rt = mustGet(taskId);

        CompletableFuture<TaskRecord> created;
        synchronized (rt) {
            TaskStatus st = rt.getStatus();
            if (st == TaskStatus.RUNNING || st == TaskStatus.WAITING) {
                return mergeOrReject(taskId, policy, rt.getInflight(), st);
            }
            if (st == TaskStatus.SUCCEEDED) {
                log.info("[触发] taskId={} 已 SUCCEEDED，直接返回既有成功结论，不重复执行", taskId);
                return CompletableFuture.completedFuture(rt.snapshot());
            }
            if (st == TaskStatus.FAILED || st == TaskStatus.BLOCKED) {
                log.warn("[触发] taskId={} 当前状态={} reason={}，普通触发被拒绝，请走 resume 恢复",
                        taskId, st, rt.getFailureReason());
                throw new TaskNotRunnableException(
                        "任务 " + taskId + " 当前为 " + st + "(reason=" + rt.getFailureReason()
                                + ")，请使用 resume 恢复而非重新触发");
            }
            // PENDING：要求上游全部成功
            List<String> unfinished = unfinishedDependencies(taskId);
            if (!unfinished.isEmpty()) {
                throw new TaskNotRunnableException(
                        "任务 " + taskId + " 的上游尚未成功: " + unfinished
                                + "；如需级联执行请使用 triggerAll");
            }
            created = new CompletableFuture<>();
            CompletableFuture<TaskRecord> clash = rt.reserve(created);
            if (clash != null) {
                return mergeOrReject(taskId, policy, clash, rt.getStatus());
            }
            rt.recordTransition(TaskStatus.RUNNING, "单任务触发：上游均已成功，直接执行");
        }
        log.info("[触发] taskId={} 单任务触发，策略={}", taskId, policy);
        runWithRetry(taskId, rt, created, FailureReason.RETRY_EXHAUSTED);
        return created;
    }

    // ============================== 触发：全工作流 ==============================

    /**
     * 触发整个工作流：所有非终态任务按依赖顺序执行，SUCCEEDED 跳过。
     * 预留名额在调度前一次性完成，保证并发两次 triggerAll 不会让任何任务执行两遍。
     */
    public CompletableFuture<List<TaskRecord>> triggerAll(TriggerPolicy policy) {
        ensureRegistered();
        Objects.requireNonNull(policy, "TriggerPolicy 不能为空");
        log.info("[触发] workflowId={} 请求全量触发，策略={}", workflowId, policy);

        List<CompletableFuture<TaskRecord>> futures = new ArrayList<>();
        List<TaskRuntime> newlyReserved = new ArrayList<>();
        List<String> rejected = new ArrayList<>();

        for (String id : topoOrder) {
            TaskRuntime rt = runtimes.get(id);
            synchronized (rt) {
                TaskStatus st = rt.getStatus();
                if (st == TaskStatus.SUCCEEDED) {
                    futures.add(CompletableFuture.completedFuture(rt.snapshot()));
                    continue;
                }
                if (st == TaskStatus.FAILED || st == TaskStatus.BLOCKED) {
                    rollbackReservations(newlyReserved);
                    throw new TaskNotRunnableException(
                            "工作流存在失败/阻断任务 " + id + "(" + st + ", reason="
                                    + rt.getFailureReason() + ")，请使用 resume 恢复");
                }
                CompletableFuture<TaskRecord> inflight = rt.getInflight();
                if (inflight != null) {
                    if (policy == TriggerPolicy.REJECT) {
                        rejected.add(id);
                        continue;
                    }
                    log.info("[触发] taskId={} 已在途({})，MERGE 到同一在途结果", id, st);
                    futures.add(inflight);
                    continue;
                }
                CompletableFuture<TaskRecord> created = new CompletableFuture<>();
                CompletableFuture<TaskRecord> clash = rt.reserve(created);
                if (clash != null) {
                    if (policy == TriggerPolicy.REJECT) {
                        rejected.add(id);
                    } else {
                        futures.add(clash);
                    }
                    continue;
                }
                rt.recordTransition(TaskStatus.WAITING, "全量触发：等待上游完成");
                newlyReserved.add(rt);
                futures.add(created);
            }
        }

        if (!rejected.isEmpty()) {
            rollbackReservations(newlyReserved);
            log.warn("[触发] REJECT 策略下 {} 个任务已在途，明确拒绝: {}", rejected.size(), rejected);
            throw new DuplicateTriggerException("任务正在执行，按 REJECT 策略拒绝: " + rejected);
        }

        for (TaskRuntime rt : newlyReserved) {
            scheduleIfReady(rt.getTaskId());
        }
        return aggregate(futures);
    }

    // ============================== 恢复 ==============================

    /**
     * 从失败点恢复：
     * <ul>
     *     <li>SUCCEEDED 原样保留，绝不重放、不重复产生副作用；</li>
     *     <li>FAILED 开启新代次重新执行，重试预算重新计满；</li>
     *     <li>BLOCKED 随上游恢复结果自动放行或继续阻断；</li>
     *     <li>PENDING 纳入本轮一并调度。</li>
     * </ul>
     */
    public CompletableFuture<List<TaskRecord>> resume(TriggerPolicy policy) {
        ensureRegistered();
        Objects.requireNonNull(policy, "TriggerPolicy 不能为空");
        log.info("[恢复] workflowId={} 请求从失败点恢复，策略={}", workflowId, policy);

        // 第一阶段：在不改变 FAILED/BLOCKED 状态的前提下，先判定 REJECT 是否成立
        List<String> rejected = new ArrayList<>();
        for (String id : topoOrder) {
            TaskRuntime rt = runtimes.get(id);
            synchronized (rt) {
                TaskStatus st = rt.getStatus();
                if ((st == TaskStatus.RUNNING || st == TaskStatus.WAITING)
                        && policy == TriggerPolicy.REJECT) {
                    rejected.add(id);
                }
            }
        }
        if (!rejected.isEmpty()) {
            log.warn("[恢复] REJECT 策略下 {} 个任务正在执行，拒绝恢复: {}", rejected.size(), rejected);
            throw new DuplicateTriggerException("恢复时有任务正在执行，按 REJECT 策略拒绝: " + rejected);
        }

        // 第二阶段：逐任务预留名额并重置，每次只持有单任务锁，不跨任务嵌套，避免与
        // worker 线程的下游级联形成锁环路
        List<CompletableFuture<TaskRecord>> futures = new ArrayList<>();
        List<TaskRuntime> targets = new ArrayList<>();
        for (String id : topoOrder) {
            TaskRuntime rt = runtimes.get(id);
            CompletableFuture<TaskRecord> future;
            synchronized (rt) {
                TaskStatus st = rt.getStatus();
                if (st == TaskStatus.SUCCEEDED) {
                    futures.add(CompletableFuture.completedFuture(rt.snapshot()));
                    continue;
                }
                if (st == TaskStatus.RUNNING || st == TaskStatus.WAITING) {
                    // MERGE：复用在途结果
                    futures.add(rt.getInflight());
                    continue;
                }
                // FAILED / BLOCKED / PENDING
                // 注意顺序：先 reset（reset 会清空旧的失败结论与在途占位），再预留新名额，
                // 否则预留会被 reset 清掉，导致恢复轮永远不被调度
                boolean restarted = st == TaskStatus.FAILED || st == TaskStatus.BLOCKED;
                if (restarted) {
                    rt.resetForResume("resume 请求");
                }
                CompletableFuture<TaskRecord> created = new CompletableFuture<>();
                CompletableFuture<TaskRecord> clash = rt.reserve(created);
                if (clash != null) {
                    futures.add(clash);
                    continue;
                }
                rt.recordTransition(TaskStatus.WAITING,
                        restarted ? "恢复轮已预留名额，等待调度" : "resume：PENDING 任务纳入本轮");
                targets.add(rt);
                future = created;
            }
            futures.add(future);
        }

        // 第三阶段：全部预留完成后统一调度
        for (TaskRuntime rt : targets) {
            scheduleIfReady(rt.getTaskId());
        }
        return aggregate(futures);
    }

    // ============================== 查询 ==============================

    public TaskRecord getStatus(String taskId) {
        ensureRegistered();
        return mustGet(taskId).snapshot();
    }

    public List<TaskRecord> getAllStatus() {
        ensureRegistered();
        List<TaskRecord> all = new ArrayList<>();
        for (String id : topoOrder) {
            all.add(runtimes.get(id).snapshot());
        }
        return all;
    }

    /**
     * 返回某任务跨代次的完整状态流转事件（审计追溯用）。
     */
    public List<TaskEvent> getEvents(String taskId) {
        ensureRegistered();
        return mustGet(taskId).events();
    }

    // ============================== 内部：调度与级联 ==============================

    /**
     * 若任务上游全部 SUCCEEDED，则推出执行；若存在 FAILED/BLOCKED 上游，则级联阻断；
     * 其余情况保持 WAITING，等待上游完成时的再次调度。
     *
     * <p>加锁约定：先逐个（不嵌套）快照上游状态，再只持有自身锁做决策，
     * 因此本方法永远不会同时持有两个任务的监视器，杜绝跨任务锁环路。
     * 上游状态在快照后只会朝“更终态”推进（RUNNING→SUCCEEDED/FAILED，WAITING→…），
     * 决策若落在竞态窗口里，下一次上游完成时的级联调度会收敛到最终一致结果。
     */
    private void scheduleIfReady(String taskId) {
        // 只持有自身锁做决策：上游状态用各自监视器的无嵌套快照（getStatus 内部加锁即释放），
        // 因此本方法永远不会同时持有两个任务的监视器，杜绝跨任务锁环路。
        TaskRuntime rt = runtimes.get(taskId);
        boolean launch = false;
        boolean block = false;
        FailureReason blockedReason = FailureReason.UPSTREAM_BLOCKED;
        String blockerId = null;
        CompletableFuture<TaskRecord> reservedFuture = null;
        synchronized (rt) {
            TaskStatus st = rt.getStatus();
            if (st != TaskStatus.WAITING && st != TaskStatus.PENDING) {
                return;
            }
            reservedFuture = rt.getInflight();
            if (reservedFuture == null) {
                return;
            }
            // 在自身锁内重新核对上游终态（快照后上游只会推进到更终态）
            boolean allSucceeded = true;
            for (String dep : definitions.get(taskId).getDependencies()) {
                TaskRuntime depRt = runtimes.get(dep);
                TaskStatus s = depRt.getStatus();
                if (s == TaskStatus.FAILED || s == TaskStatus.BLOCKED) {
                    blockerId = dep;
                    blockedReason = s == TaskStatus.FAILED
                            ? FailureReason.UPSTREAM_FAILED
                            : depRt.getFailureReason();
                    block = true;
                    break;
                }
                if (s != TaskStatus.SUCCEEDED) {
                    allSucceeded = false;
                }
            }
            if (block) {
                finalizeBlocked(rt, blockedReason, "上游任务 " + blockerId + " 未成功，阻断下游");
            } else if (!allSucceeded) {
                // 上游仍在途：保持 WAITING，等上游终态时再次调度
                return;
            } else {
                rt.recordTransition(TaskStatus.RUNNING, "上游全部 SUCCEEDED，开始执行");
                launch = true;
            }
        }

        if (launch) {
            runWithRetry(taskId, rt, reservedFuture, FailureReason.RETRY_EXHAUSTED);
        } else {
            // 自身已 BLOCKED，继续向下传播
            propagateBlocked(taskId, blockedReason);
        }
    }

    /**
     * 从一个已阻断节点沿可达下游 BFS 传播 BLOCKED，已终态节点不受影响。
     */
    private void propagateBlocked(String seedId, FailureReason reason) {
        Deque<String> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        queue.add(seedId);
        visited.add(seedId);
        while (!queue.isEmpty()) {
            String id = queue.poll();
            for (String down : dependents.getOrDefault(id, Set.of())) {
                if (!visited.add(down)) {
                    continue;
                }
                TaskRuntime downRt = runtimes.get(down);
                boolean becameBlocked = false;
                synchronized (downRt) {
                    TaskStatus st = downRt.getStatus();
                    if (st == TaskStatus.WAITING || st == TaskStatus.PENDING) {
                        boolean direct = definitions.get(down).getDependencies().contains(id);
                        FailureReason r = direct && reason == FailureReason.UPSTREAM_FAILED
                                ? FailureReason.UPSTREAM_FAILED
                                : FailureReason.UPSTREAM_BLOCKED;
                        finalizeBlocked(downRt, r,
                                "传递阻断：经由 " + id + "(seed=" + seedId + ")");
                        becameBlocked = true;
                    }
                }
                if (becameBlocked) {
                    queue.add(down);
                }
            }
        }
    }

    /**
     * 任务 FAILED 后阻断其全部可达下游（直连下游记 UPSTREAM_FAILED，传递节点记 UPSTREAM_BLOCKED）。
     */
    private void blockDownstreams(String failedId) {
        Deque<String> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        queue.add(failedId);
        visited.add(failedId);
        while (!queue.isEmpty()) {
            String id = queue.poll();
            for (String down : dependents.getOrDefault(id, Set.of())) {
                if (!visited.add(down)) {
                    continue;
                }
                TaskRuntime downRt = runtimes.get(down);
                boolean becameBlocked;
                synchronized (downRt) {
                    TaskStatus st = downRt.getStatus();
                    becameBlocked = st == TaskStatus.WAITING || st == TaskStatus.PENDING;
                    if (becameBlocked) {
                        boolean direct = definitions.get(down).getDependencies().contains(failedId);
                        FailureReason reason = direct
                                ? FailureReason.UPSTREAM_FAILED
                                : FailureReason.UPSTREAM_BLOCKED;
                        finalizeBlocked(downRt, reason,
                                (direct ? "直接上游 " : "传递阻断自 ") + failedId + " 已 FAILED");
                    }
                }
                if (becameBlocked) {
                    queue.add(down);
                }
            }
        }
    }

    private void finalizeBlocked(TaskRuntime rt, FailureReason reason, String detail) {
        CompletableFuture<TaskRecord> f = rt.getInflight();
        rt.markTerminal(TaskStatus.BLOCKED, reason, rt.getAttempts(), detail);
        if (f != null) {
            // 阻断是终态：完成在途结果并释放名额，使 resume 可以重新预留并重置该任务
            f.complete(rt.snapshot());
            rt.release(f);
        }
        log.warn("[阻断] taskId={} 置为 BLOCKED，reason={}，detail={}",
                rt.getTaskId(), reason, detail);
    }

    // ============================== 内部：执行与重试 ==============================

    /**
     * 异步执行任务体，按 RetryPolicy 重试；耗尽后置 FAILED（保留可查询结论）并阻断下游。
     */
    private void runWithRetry(String taskId,
                              TaskRuntime rt,
                              CompletableFuture<TaskRecord> future,
                              FailureReason terminalReason) {
        TaskDefinition def = definitions.get(taskId);
        int maxAttempts = Math.max(1, def.getRetryPolicy().maxAttempts());
        long backoff = Math.max(0L, def.getRetryPolicy().backoffMillis());

        executor.submit(() -> {
            int attempt = 0;
            Throwable lastError = null;
            boolean failed = false;
            try {
                while (attempt < maxAttempts) {
                    attempt++;
                    synchronized (rt) {
                        rt.incrementAttempts();
                    }
                    log.info("[执行] taskId={} generation={} 第 {}/{} 次尝试开始",
                            taskId, rt.getGeneration(), attempt, maxAttempts);
                    try {
                        def.getExecution().execute(new TaskExecutionContext());
                        synchronized (rt) {
                            // 只有在途持有者可落终态，防止跨代次的过期结果污染状态
                            if (rt.getInflight() != future) {
                                log.warn("[执行] taskId={} 在途占位已变更，丢弃过期结果", taskId);
                                return;
                            }
                            rt.markTerminal(TaskStatus.SUCCEEDED, FailureReason.NONE,
                                    attempt, "第 " + attempt + " 次尝试成功");
                        }
                        future.complete(rt.snapshot());
                        log.info("[执行] taskId={} 成功（尝试 {} 次），放行下游", taskId, attempt);
                        // 在自身锁之外级联调度，避免与触发线程形成锁环路
                        for (String down : dependents.getOrDefault(taskId, Set.of())) {
                            scheduleIfReady(down);
                        }
                        return;
                    } catch (Exception e) {
                        lastError = e;
                        log.warn("[执行] taskId={} 第 {}/{} 次尝试失败: {}",
                                taskId, attempt, maxAttempts, e.toString());
                        if (attempt < maxAttempts && backoff > 0) {
                            try {
                                Thread.sleep(backoff);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }
                synchronized (rt) {
                    if (rt.getInflight() != future) {
                        return;
                    }
                    String msg = "重试耗尽(" + attempt + "/" + maxAttempts + "): "
                            + (lastError == null ? "unknown" : lastError.toString());
                    rt.markTerminal(TaskStatus.FAILED, terminalReason, attempt, msg);
                    failed = true;
                }
                future.complete(rt.snapshot());
                log.error("[执行] taskId={} 重试耗尽，置 FAILED reason={}，阻断下游",
                        taskId, terminalReason);
            } finally {
                rt.release(future);
                // 在自身锁之外阻断下游，避免与触发/恢复线程的预留逻辑死锁
                if (failed) {
                    blockDownstreams(taskId);
                }
            }
        });
    }

    // ============================== 内部：辅助 ==============================

    private CompletableFuture<TaskRecord> mergeOrReject(
            String taskId, TriggerPolicy policy,
            CompletableFuture<TaskRecord> inflight, TaskStatus state) {
        if (inflight == null) {
            throw new OrchestrationException("任务 " + taskId + " 状态不一致: " + state);
        }
        if (policy == TriggerPolicy.REJECT) {
            log.warn("[触发] taskId={} 处于{}，按 REJECT 策略明确拒绝重复触发", taskId, state);
            throw new DuplicateTriggerException(
                    "任务 " + taskId + " 正在执行(" + state + ")，按 REJECT 策略拒绝重复触发");
        }
        log.info("[触发] taskId={} 处于{}，按 MERGE 策略合并到同一在途结果（不重复执行）",
                taskId, state);
        return inflight;
    }

    private void rollbackReservations(List<TaskRuntime> reserved) {
        for (TaskRuntime rt : reserved) {
            synchronized (rt) {
                rt.release(rt.getInflight());
                if (rt.getStatus() == TaskStatus.WAITING) {
                    rt.recordTransition(TaskStatus.PENDING, "触发被整体拒绝，回滚为 PENDING");
                }
            }
        }
    }

    private CompletableFuture<List<TaskRecord>> aggregate(List<CompletableFuture<TaskRecord>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<TaskRecord> result = new ArrayList<>();
                    for (String id : topoOrder) {
                        result.add(runtimes.get(id).snapshot());
                    }
                    return result;
                });
    }

    private List<String> unfinishedDependencies(String taskId) {
        List<String> unfinished = new ArrayList<>();
        for (String dep : definitions.get(taskId).getDependencies()) {
            TaskStatus s = runtimes.get(dep).getStatus();
            if (s != TaskStatus.SUCCEEDED) {
                unfinished.add(dep + "(" + s + ")");
            }
        }
        return unfinished;
    }

    private TaskRuntime mustGet(String taskId) {
        TaskRuntime rt = runtimes.get(taskId);
        if (rt == null) {
            throw new UnknownTaskException("任务不存在: taskId=" + taskId);
        }
        return rt;
    }

    private void ensureRegistered() {
        if (!registered.get()) {
            throw new OrchestrationException("引擎尚未注册工作流，请先调用 register");
        }
    }
}
