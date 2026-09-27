package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.runtime;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.FailureReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskEvent;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskRecord;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 单个任务的运行时状态。
 *
 * <p>持有：当前 {@link TaskStatus}、恢复代次 generation、已尝试次数 attempts、
 * 失败原因码、在途 Future（用于并发去重）以及跨代次保留的审计事件列表。
 *
 * <p>所有读写均在对象监视器内完成，保证状态流转的原子性与可见性。
 * 包内可见，仅供引擎使用。
 */
public class TaskRuntime {

    private final String taskId;

    private int generation = 0;
    private TaskStatus status = TaskStatus.PENDING;
    private FailureReason failureReason = FailureReason.NONE;
    private int attempts = 0;
    private String message = "";

    /** 已预留或正在执行的在途结果；非 null 表示该任务已被某一轮触发占用。 */
    private CompletableFuture<TaskRecord> inflight;

    private final List<TaskEvent> events = new ArrayList<>();

    TaskRuntime(String taskId) {
        this.taskId = taskId;
        events.add(new TaskEvent(taskId, generation, TaskStatus.PENDING, TaskStatus.PENDING,
                "任务注册，初始状态 PENDING", Instant.now()));
    }

    public String getTaskId() {
        return taskId;
    }

    public synchronized int getGeneration() {
        return generation;
    }

    public synchronized TaskStatus getStatus() {
        return status;
    }

    public synchronized FailureReason getFailureReason() {
        return failureReason;
    }

    public synchronized int getAttempts() {
        return attempts;
    }

    public synchronized String getMessage() {
        return message;
    }

    /**
     * 记录一次状态流转并追加审计事件。
     */
    public synchronized void recordTransition(TaskStatus to, String detail) {
        TaskStatus from = this.status;
        this.status = to;
        events.add(new TaskEvent(taskId, generation, from, to, detail, Instant.now()));
    }

    /**
     * 递增尝试次数（每次真正调用执行体前调用）。
     */
    public synchronized void incrementAttempts() {
        this.attempts++;
    }

    /**
     * 记录终态：SUCCEEDED / FAILED / BLOCKED。
     */
    public synchronized void markTerminal(TaskStatus terminal,
                                          FailureReason reason,
                                          int attempts,
                                          String message) {
        TaskStatus from = this.status;
        this.status = terminal;
        this.failureReason = reason;
        this.attempts = attempts;
        this.message = message == null ? "" : message;
        events.add(new TaskEvent(taskId, generation, from, terminal,
                "终态: reason=" + reason + ", attempts=" + attempts
                        + ", message=" + this.message, Instant.now()));
    }

    /**
     * 为恢复重放重置：开启新代次，FAILED/BLOCKED 回到 PENDING，
     * 清空在途占用与失败结论。SUCCEEDED 任务不会调用本方法。
     */
    public synchronized void resetForResume(String detail) {
        TaskStatus from = this.status;
        this.generation++;
        this.status = TaskStatus.PENDING;
        this.failureReason = FailureReason.NONE;
        this.attempts = 0;
        this.message = "";
        this.inflight = null;
        events.add(new TaskEvent(taskId, generation, from, TaskStatus.PENDING,
                "恢复代次 " + generation + ": " + from + " -> PENDING; " + detail, Instant.now()));
    }

    // ---------- 在途 Future（并发去重） ----------

    public synchronized CompletableFuture<TaskRecord> getInflight() {
        return inflight;
    }

    /**
     * 预留执行名额。返回 null 表示预留成功；返回非 null 表示已被占用，
     * 调用方应按 TriggerPolicy 合并或拒绝。
     */
    public synchronized CompletableFuture<TaskRecord> reserve(CompletableFuture<TaskRecord> future) {
        if (this.inflight != null) {
            return this.inflight;
        }
        this.inflight = future;
        return null;
    }

    /**
     * 释放执行名额（仅当传入的是当前在途 Future 时生效，避免跨代次误释放）。
     */
    public synchronized void release(CompletableFuture<TaskRecord> future) {
        if (this.inflight == future) {
            this.inflight = null;
        }
    }

    /**
     * 当前状态快照（查询用）。
     */
    public synchronized TaskRecord snapshot() {
        return new TaskRecord(taskId, generation, status, failureReason,
                attempts, message, Instant.now());
    }

    /**
     * 只读审计事件列表副本（跨代次完整保留，用于追溯）。
     */
    public synchronized List<TaskEvent> events() {
        return new ArrayList<>(events);
    }
}
