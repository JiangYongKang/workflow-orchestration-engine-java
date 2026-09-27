package com.github.highcumontoa.workfloworchestrationenginejava.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.FailureReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskEvent;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskRecord;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskStatus;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.runtime.OrchestrationEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：首轮 A 重试耗尽导致 A=FAILED、下游 BLOCKED；随后修复 A 后调用 resume。
 * 判定依据：
 * 1) 已 SUCCEEDED 的任务（例如下游链中的独立成功节点）副作用只发生一次、代次不变；
 * 2) FAILED 任务开启新代次（generation+1）重新执行并重试预算重新计满；
 * 3) BLOCKED 下游随上游恢复自动放行，整体终态一致；
 * 4) 审计事件完整记录跨代次状态流转，可追溯。
 */
class ResumeReplayTest {

    private static final Logger log = LoggerFactory.getLogger(ResumeReplayTest.class);

    private OrchestrationEngine newEngine() {
        return new OrchestrationEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("恢复重放：修复后 resume，已成功任务不重放，失败任务新代次成功，阻断下游放行")
    void resumeReplaysOnlyFailedAndReleasesBlocked() throws Exception {
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        AtomicBoolean aShouldFail = new AtomicBoolean(true);
        log.info("[测试] 触发条件：DAG A->B->C。首轮 A 始终失败(maxAttempts=2)；之后翻转开关再 resume。"
                + "预期：A 代次 1 成功，B/C 从 BLOCKED 放行，所有任务副作用各只发生应有的次数");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("resume-wf", List.of(
                new TaskDefinition("A", List.of(), RetryPolicy.fixed(2, 1L), ctx -> {
                    runs.computeIfAbsent("A", k -> new AtomicInteger()).incrementAndGet();
                    if (aShouldFail.get()) {
                        throw new RuntimeException("A transient failure");
                    }
                }),
                ok("B", List.of("A"), runs),
                ok("C", List.of("B"), runs)
        )));

        // 第一轮：A 失败，B/C 被阻断
        List<TaskRecord> first = engine.triggerAll(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        Map<String, TaskRecord> m1 = byId(first);
        log.info("[测试] 首轮判定依据：A={}/{}, B={}/{}, C={}/{}, A执行次数={}",
                m1.get("A").getStatus(), m1.get("A").getFailureReason(),
                m1.get("B").getStatus(), m1.get("B").getFailureReason(),
                m1.get("C").getStatus(), m1.get("C").getFailureReason(),
                runs.getOrDefault("A", new AtomicInteger(0)).get());
        assertEquals(TaskStatus.FAILED, m1.get("A").getStatus());
        assertEquals(FailureReason.RETRY_EXHAUSTED, m1.get("A").getFailureReason());
        assertEquals(TaskStatus.BLOCKED, m1.get("B").getStatus());
        assertEquals(TaskStatus.BLOCKED, m1.get("C").getStatus());
        assertEquals(2, runs.getOrDefault("A", new AtomicInteger(0)).get());
        assertEquals(0, runs.getOrDefault("B", new AtomicInteger(0)).get());
        assertEquals(0, runs.getOrDefault("C", new AtomicInteger(0)).get());

        // 修复 A，恢复重放
        aShouldFail.set(false);
        log.info("[测试] 恢复条件：A 的故障开关已关闭，调用 resume(MERGE)");
        List<TaskRecord> second = engine.resume(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        Map<String, TaskRecord> m2 = byId(second);
        log.info("[测试] 恢复轮判定依据：A={} gen={} attempts={}, B={} gen={}, C={} gen={}；副作用计数={}",
                m2.get("A").getStatus(), m2.get("A").getGeneration(), m2.get("A").getAttempts(),
                m2.get("B").getStatus(), m2.get("B").getGeneration(),
                m2.get("C").getStatus(), m2.get("C").getGeneration(),
                runs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue().get()).toList());

        assertEquals(TaskStatus.SUCCEEDED, m2.get("A").getStatus());
        assertEquals(TaskStatus.SUCCEEDED, m2.get("B").getStatus());
        assertEquals(TaskStatus.SUCCEEDED, m2.get("C").getStatus());
        // A 首轮 2 次失败 + 恢复轮 1 次成功
        assertEquals(3, runs.getOrDefault("A", new AtomicInteger(0)).get(), "A 恢复轮应只执行 1 次（预算重新计满且一次成功）");
        assertEquals(1, runs.getOrDefault("B", new AtomicInteger(0)).get(), "B 被阻断过，恢复后只执行一次");
        assertEquals(1, runs.getOrDefault("C", new AtomicInteger(0)).get(), "C 被阻断过，恢复后只执行一次");
        // 代次：A/B/C 均经历了恢复，generation=1
        assertEquals(1, m2.get("A").getGeneration());
        assertEquals(1, m2.get("B").getGeneration());
        assertEquals(1, m2.get("C").getGeneration());
    }

    @Test
    @DisplayName("恢复不重放成功任务：独立成功节点 X 的副作用保持一次、代次保持 0")
    void succeededTaskNotReplayedOnResume() throws Exception {
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        AtomicBoolean aShouldFail = new AtomicBoolean(true);
        log.info("[测试] 触发条件：A 失败、无关节点 X 已成功；resume 后 X 不应重复执行");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("resume-idem-wf", List.of(
                new TaskDefinition("A", List.of(), RetryPolicy.fixed(2, 1L), ctx -> {
                    runs.computeIfAbsent("A", k -> new AtomicInteger()).incrementAndGet();
                    if (aShouldFail.get()) {
                        throw new RuntimeException("fail A");
                    }
                }),
                ok("X", List.of(), runs)
        )));

        engine.triggerAll(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        int xRunsBefore = runs.getOrDefault("X", new AtomicInteger(0)).get();
        int xGenBefore = engine.getStatus("X").getGeneration();

        aShouldFail.set(false);
        List<TaskRecord> after = engine.resume(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        TaskRecord xAfter = after.stream().filter(r -> r.getTaskId().equals("X")).findFirst().orElseThrow();

        log.info("[测试] 判定依据：X 恢复前执行次数={}/代次={}，恢复后执行次数={}/代次={}",
                xRunsBefore, xGenBefore, runs.getOrDefault("X", new AtomicInteger(0)).get(), xAfter.getGeneration());
        assertEquals(1, runs.getOrDefault("X", new AtomicInteger(0)).get(), "已成功任务恢复时不得重复产生副作用");
        assertEquals(0, xAfter.getGeneration(), "已成功任务代次不应变化");
        assertEquals(TaskStatus.SUCCEEDED, xAfter.getStatus());
    }

    @Test
    @DisplayName("审计追溯：跨代次状态流转事件完整保留，包含 FAILED、reset、SUCCEEDED")
    void auditEventsPreserveCrossGenerationTransitions() throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);
        log.info("[测试] 触发条件：A 首轮失败、恢复成功；检查 A 的完整事件链");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("audit-wf", List.of(
                new TaskDefinition("A", List.of(), RetryPolicy.fixed(1, 1L), ctx -> {
                    if (fail.get()) {
                        throw new RuntimeException("fail");
                    }
                })
        )));
        engine.triggerAll(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        fail.set(false);
        engine.resume(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);

        List<TaskEvent> events = engine.getEvents("A");
        log.info("[测试] 判定依据：A 的事件链 ={}", events.stream()
                .map(e -> "gen" + e.generation() + ":" + e.fromStatus() + "->" + e.toStatus()
                        + "(" + e.detail() + ")")
                .toList());

        assertTrue(events.stream().anyMatch(e -> e.toStatus() == TaskStatus.FAILED
                && e.generation() == 0), "应保留首轮 FAILED 事件");
        assertTrue(events.stream().anyMatch(e -> e.fromStatus() == TaskStatus.FAILED
                && e.toStatus() == TaskStatus.PENDING && e.generation() == 1),
                "应记录恢复代次重置事件");
        assertTrue(events.stream().anyMatch(e -> e.toStatus() == TaskStatus.SUCCEEDED
                && e.generation() == 1), "应记录恢复轮 SUCCEEDED 事件");
        assertEquals(TaskStatus.SUCCEEDED, engine.getStatus("A").getStatus());
        assertEquals(FailureReason.NONE, engine.getStatus("A").getFailureReason());
    }

    @Test
    @DisplayName("恢复仍失败：新代次重试再次耗尽，结论更新为 FAILED 且下游继续阻断")
    void resumeThatStillFailsKeepsDownstreamBlocked() throws Exception {
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        log.info("[测试] 触发条件：A 首轮失败，resume 时仍失败(maxAttempts=2)；"
                + "预期：A 在 gen=1 再次 FAILED，B 仍 BLOCKED");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("resume-fail-again-wf", List.of(
                new TaskDefinition("A", List.of(), RetryPolicy.fixed(2, 1L), ctx -> {
                    runs.computeIfAbsent("A", k -> new AtomicInteger()).incrementAndGet();
                    throw new RuntimeException("still failing");
                }),
                ok("B", List.of("A"), runs)
        )));
        engine.triggerAll(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        List<TaskRecord> after = engine.resume(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        Map<String, TaskRecord> m = byId(after);

        log.info("[测试] 判定依据：A gen={} 状态={} attempts={}, B 状态={}/{}, A总执行次数={}",
                m.get("A").getGeneration(), m.get("A").getStatus(), m.get("A").getAttempts(),
                m.get("B").getStatus(), m.get("B").getFailureReason(), runs.getOrDefault("A", new AtomicInteger(0)).get());
        assertEquals(1, m.get("A").getGeneration());
        assertEquals(TaskStatus.FAILED, m.get("A").getStatus());
        assertEquals(FailureReason.RETRY_EXHAUSTED, m.get("A").getFailureReason());
        assertEquals(2, m.get("A").getAttempts(), "新代次重试预算应重新计满");
        assertEquals(4, runs.getOrDefault("A", new AtomicInteger(0)).get());
        assertEquals(TaskStatus.BLOCKED, m.get("B").getStatus());
        assertEquals(0, runs.getOrDefault("B", new AtomicInteger(0)).get());
    }

    private TaskDefinition ok(String id, List<String> deps, Map<String, AtomicInteger> runs) {
        return new TaskDefinition(id, deps, RetryPolicy.noRetry(), ctx ->
                runs.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet());
    }

    private Map<String, TaskRecord> byId(List<TaskRecord> records) {
        return records.stream().collect(Collectors.toMap(TaskRecord::getTaskId, Function.identity()));
    }
}
