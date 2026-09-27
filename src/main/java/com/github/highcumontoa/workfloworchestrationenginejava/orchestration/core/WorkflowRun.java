package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.FailureConclusion;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RunEvent;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskState;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 一次工作流运行的状态容器。
 *
 * <p>一个 {@code WorkflowRun} 可能经历多个串行<b>阶段</b>：初始触发为第 1 阶段，
 * 每次从失败点恢复产生一个新阶段。任意时刻同一 run 至多有一个执行阶段在进行，
 * 并发触发由引擎保证以 MERGE（等待本 run 结束）或 REJECT（拒绝）方式处理。
 *
 * <p>容器内所有状态均可查询：任务状态快照、各阶段尝试次数、副作用累计调用次数、
 * 最近错误信息、完整事件轨迹与重试耗尽后的失败结论。
 */
public final class WorkflowRun {

    private final String runId;
    private final Workflow workflow;

    private final ConcurrentHashMap<String, TaskState> states = new ConcurrentHashMap<>();
    /** 当前/最近一个阶段内每个任务的尝试次数（恢复时对未成功任务归零重计）。 */
    private final ConcurrentHashMap<String, Integer> attempts = new ConcurrentHashMap<>();
    /** 跨全部阶段累计的任务动作实际调用次数（用于验证恢复不重复产生副作用）。 */
    private final ConcurrentHashMap<String, Integer> invocationCount = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> lastErrors = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<RunEvent> events = new CopyOnWriteArrayList<>();

    private volatile FailureConclusion failureConclusion;
    private volatile String phaseName = "INIT";

    /** 阶段屏障：done=false 表示有阶段在执行；构造即执行中，每次阶段结束置 true 并唤醒等待者。 */
    private final Object monitor = new Object();
    private volatile boolean done = false;

    public WorkflowRun(String runId, Workflow workflow) {
        this.runId = runId;
        this.workflow = workflow;
        for (String taskId : workflow.getTasks().keySet()) {
            states.put(taskId, TaskState.PENDING);
        }
    }

    public String getRunId() {
        return runId;
    }

    public Workflow getWorkflow() {
        return workflow;
    }

    public String getPhaseName() {
        return phaseName;
    }

    // ---- 引擎使用的阶段协调原语（同一 run 由引擎保证串行调用） ----

    /**
     * 开始一个新阶段。{@code recovery=true} 时把未成功任务（FAILED/BLOCKED/PENDING）
     * 复位为 PENDING、清空其尝试次数与错误，SUCCEEDED 任务原样保留，
     * 保证恢复重放不会重复执行已成功任务。
     */
    public void beginPhase(String name, boolean recovery) {
        synchronized (monitor) {
            this.phaseName = name;
            this.done = false;
            this.failureConclusion = null;
            if (recovery) {
                for (String taskId : states.keySet()) {
                    TaskState state = states.get(taskId);
                    if (state != TaskState.SUCCEEDED) {
                        // 仅复位将被重放的任务；已成功任务保持状态与最近一次成功的尝试记录
                        states.put(taskId, TaskState.PENDING);
                        attempts.put(taskId, 0);
                        lastErrors.remove(taskId);
                    }
                }
            }            events.add(new RunEvent(System.currentTimeMillis(),
                    null, null, null, 0, "阶段开始: " + name + (recovery ? "（失败点恢复）" : "（初始触发）")));
            monitor.notifyAll();
        }
    }

    /** 阶段以终态结束（全部成功或重试耗尽），唤醒所有等待者。 */
    public void finishTerminal() {
        synchronized (monitor) {
            this.done = true;
            events.add(new RunEvent(System.currentTimeMillis(),
                    null, null, null, 0, "阶段结束(终态): " + phaseName));
            monitor.notifyAll();
        }
    }

    /**
     * 恢复发布前调用：在工作流发布锁内、发布到在途表之前翻转回执行中，
     * 杜绝“已发布但仍显示终态”的窗口被并发触发误判为可立即复用。
     * 状态复位（FAILED/BLOCKED -> PENDING）仍由随后的 {@link #beginPhase} 完成。
     */
    public void markInFlightForRecovery() {
        synchronized (monitor) {
            this.done = false;
            monitor.notifyAll();
        }
    }

    // ---- 引擎使用的状态变更原语 ----

    public void markInvocation(String taskId) {
        invocationCount.merge(taskId, 1, Integer::sum);
    }

    public void recordAttemptStart(String taskId) {
        attempts.merge(taskId, 1, Integer::sum);
    }

    public void transition(String taskId, TaskState to, int attempt, String message) {
        TaskState from = states.get(taskId);
        states.put(taskId, to);
        events.add(new RunEvent(System.currentTimeMillis(), taskId, from, to, attempt, message));
    }

    public void recordError(String taskId, String errorMessage) {
        lastErrors.put(taskId, errorMessage);
    }

    public void attachFailureConclusion(FailureConclusion conclusion) {
        this.failureConclusion = conclusion;
        events.add(new RunEvent(System.currentTimeMillis(),
                conclusion.failedTaskId(), TaskState.RUNNING, TaskState.FAILED,
                conclusion.attempts(),
                "重试耗尽，失败结论已固化；被阻断下游=" + conclusion.blockedTasks()
                        + "；原因=" + conclusion.errorMessage()));
    }

    // ---- 查询接口 ----

    public TaskState getState(String taskId) {
        return states.get(taskId);
    }

    public int getAttempts(String taskId) {
        return attempts.getOrDefault(taskId, 0);
    }

    public int getInvocationCount(String taskId) {
        return invocationCount.getOrDefault(taskId, 0);
    }

    public String getLastError(String taskId) {
        return lastErrors.get(taskId);
    }

    public Map<String, TaskState> stateSnapshot() {
        return Map.copyOf(states);
    }

    public List<RunEvent> events() {
        return List.copyOf(events);
    }

    public FailureConclusion failureConclusion() {
        return failureConclusion;
    }

    public boolean isDone() {
        return done;
    }

    public boolean succeeded() {
        return done && states.values().stream().allMatch(s -> s == TaskState.SUCCEEDED);
    }

    /** 阻塞直到当前阶段以终态结束。 */
    public void awaitCompletion() {
        synchronized (monitor) {
            while (!done) {
                try {
                    monitor.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("等待工作流运行被中断: run=" + runId, e);
                }
            }
        }
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (monitor) {
            while (!done) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return done;
                }
                try {
                    monitor.wait(remaining / 1_000_000L, (int) (remaining % 1_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("等待工作流运行被中断: run=" + runId, e);
                }
            }
            return true;
        }
    }
}
