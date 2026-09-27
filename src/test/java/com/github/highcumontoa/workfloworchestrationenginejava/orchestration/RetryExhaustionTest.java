package com.github.highcumontoa.workfloworchestrationenginejava.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.TaskNotRunnableException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.FailureReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskDefinition;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：任务持续失败，重试次数达到上限；并观察失败任务的下游。
 * 判定依据：失败任务精确执行 maxAttempts 次后置 FAILED + RETRY_EXHAUSTED；
 * 直接下游 BLOCKED/UPSTREAM_FAILED，传递下游 BLOCKED/UPSTREAM_BLOCKED，原因码可区分；
 * 失败后普通触发被拒绝并引导恢复。
 */
class RetryExhaustionTest {

    private static final Logger log = LoggerFactory.getLogger(RetryExhaustionTest.class);

    private OrchestrationEngine newEngine() {
        return new OrchestrationEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("重试耗尽：始终失败的任务精确执行 maxAttempts 次，终态 FAILED/RETRY_EXHAUSTED")
    void retriesUntilExhausted() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        int maxAttempts = 3;
        log.info("[测试] 触发条件：A 始终抛异常，maxAttempts={}；预期判定：执行 {} 次后 FAILED",
                maxAttempts, maxAttempts);

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("retry-wf", List.of(
                new TaskDefinition("A", List.of(),
                        RetryPolicy.fixed(maxAttempts, 1L),
                        ctx -> {
                            runs.incrementAndGet();
                            throw new IllegalStateException("boom-" + runs.get());
                        })
        )));

        TaskRecord rec = engine.triggerTask("A", TriggerPolicy.MERGE)
                .get(5, TimeUnit.SECONDS);
        log.info("[测试] 判定依据：实际执行次数={}, 状态={}, 原因={}, message={}",
                runs.get(), rec.getStatus(), rec.getFailureReason(), rec.getMessage());

        assertEquals(maxAttempts, runs.get(), "重试次数应精确等于 maxAttempts");
        assertEquals(TaskStatus.FAILED, rec.getStatus());
        assertEquals(FailureReason.RETRY_EXHAUSTED, rec.getFailureReason());
        assertTrue(rec.getMessage().contains("重试耗尽"));
        assertTrue(rec.getMessage().contains("boom-3"));
    }

    @Test
    @DisplayName("失败阻断下游：直接下游 UPSTREAM_FAILED，传递下游 UPSTREAM_BLOCKED，均不执行")
    void failureBlocksDownstreamsWithDistinguishableReasons() throws Exception {
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        log.info("[测试] 触发条件：DAG A->B->D, A->C->D，A 始终失败(maxAttempts=2)；"
                + "预期判定：A=FAILED，B/C=BLOCKED/UPSTREAM_FAILED，D=BLOCKED/UPSTREAM_BLOCKED");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("block-wf", List.of(
                flaky("A", List.of(), runs, 2, id -> {
                    throw new RuntimeException("A always fails");
                }),
                ok("B", List.of("A"), runs),
                ok("C", List.of("A"), runs),
                ok("D", List.of("B", "C"), runs)
        )));

        List<TaskRecord> all = engine.triggerAll(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        Map<String, TaskRecord> byId = all.stream()
                .collect(Collectors.toMap(TaskRecord::getTaskId, Function.identity()));
        log.info("[测试] 判定依据：{}", all.stream()
                .map(r -> r.getTaskId() + "=" + r.getStatus() + "/" + r.getFailureReason())
                .toList());

        assertEquals(TaskStatus.FAILED, byId.get("A").getStatus());
        assertEquals(FailureReason.RETRY_EXHAUSTED, byId.get("A").getFailureReason());
        assertEquals(TaskStatus.BLOCKED, byId.get("B").getStatus());
        assertEquals(TaskStatus.BLOCKED, byId.get("C").getStatus());
        assertEquals(FailureReason.UPSTREAM_FAILED, byId.get("B").getFailureReason());
        assertEquals(FailureReason.UPSTREAM_FAILED, byId.get("C").getFailureReason());
        assertEquals(TaskStatus.BLOCKED, byId.get("D").getStatus());
        assertEquals(FailureReason.UPSTREAM_BLOCKED, byId.get("D").getFailureReason());
        assertEquals(2, runs.get("A").get(), "A 应执行 2 次");
        assertEquals(0, runs.getOrDefault("B", new AtomicInteger()).get());
        assertEquals(0, runs.getOrDefault("C", new AtomicInteger()).get());
        assertEquals(0, runs.getOrDefault("D", new AtomicInteger()).get());
    }

    @Test
    @DisplayName("失败结论可查询且普通再触发被拒绝：必须走 resume")
    void failedTaskRejectsNormalTrigger() throws Exception {
        log.info("[测试] 触发条件：A FAILED 后再次 triggerTask(A)；预期判定：TaskNotRunnableException 引导恢复");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("retrigger-wf", List.of(
                new TaskDefinition("A", List.of(), RetryPolicy.noRetry(),
                        ctx -> {
                            throw new RuntimeException("fail");
                        })
        )));
        engine.triggerTask("A", TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);

        TaskNotRunnableException ex = assertThrows(TaskNotRunnableException.class,
                () -> engine.triggerTask("A", TriggerPolicy.MERGE));
        log.info("[测试] 判定依据：异常类型={}, message={}", ex.getClass().getSimpleName(), ex.getMessage());
        TaskRecord snapshot = engine.getStatus("A");
        assertEquals(TaskStatus.FAILED, snapshot.getStatus());
        assertEquals(FailureReason.RETRY_EXHAUSTED, snapshot.getFailureReason());
    }

    private TaskDefinition ok(String id, List<String> deps, Map<String, AtomicInteger> runs) {
        return new TaskDefinition(id, deps, RetryPolicy.noRetry(), ctx ->
                runs.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet());
    }

    @FunctionalInterface
    private interface FailingBody {
        void run(String id);
    }

    private TaskDefinition flaky(String id, List<String> deps,
                                 Map<String, AtomicInteger> runs,
                                 int maxAttempts, FailingBody body) {
        return new TaskDefinition(id, deps, RetryPolicy.fixed(maxAttempts, 1L), ctx -> {
            runs.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            body.run(id);
        });
    }
}
