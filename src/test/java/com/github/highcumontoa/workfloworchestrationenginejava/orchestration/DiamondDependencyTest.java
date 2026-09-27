package com.github.highcumontoa.workfloworchestrationenginejava.orchestration;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：菱形 DAG（A -> B、C -> D），让 B、C 以不同时序完成。
 * 判定依据：D 必须在两个上游都 SUCCEEDED 后才执行，且执行体只运行一次；
 * 两个上游分别完成时的级联调度不会让 D 提前运行或运行两次。
 */
class DiamondDependencyTest {

    private static final Logger log = LoggerFactory.getLogger(DiamondDependencyTest.class);

    @Test
    @DisplayName("菱形汇合：B 慢、C 快时，D 等 B 完成后只执行一次")
    void joinTaskWaitsForBothBranchesAndRunsOnce() throws Exception {
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        CountDownLatch bEntered = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        log.info("[测试] 触发条件：A 完成后 B 被闸门挡住、C 立即完成；随后放行 B；预期 D 只执行一次");

        OrchestrationEngine engine = new OrchestrationEngine(new WorkflowValidator());
        engine.register(new WorkflowDefinition("diamond-wf", List.of(
                counted("A", List.of(), runs, null, null),
                counted("B", List.of("A"), runs, bEntered, releaseB),
                counted("C", List.of("A"), runs, null, null),
                counted("D", List.of("B", "C"), runs, null, null)
        )));

        java.util.concurrent.CompletableFuture<List<TaskRecord>> future =
                engine.triggerAll(TriggerPolicy.MERGE);
        assertTrue(bEntered.await(5, TimeUnit.SECONDS), "B 应已进入执行体（此时 C 必然尚未放行 D）");
        // B 被挡住期间，C 应已完成并尝试调度 D；D 必须仍处于等待
        TaskRecord dWhileBRunning = engine.getStatus("D");
        assertEquals(TaskStatus.WAITING, dWhileBRunning.getStatus(),
                "B 未完成时汇合任务 D 必须保持 WAITING");
        assertEquals(0, runs.getOrDefault("D", new AtomicInteger()).get(),
                "D 不能在任一上游未完成时提前执行");

        releaseB.countDown();
        List<TaskRecord> result = future.get(5, TimeUnit.SECONDS);
        Map<String, TaskRecord> byId = result.stream()
                .collect(Collectors.toMap(TaskRecord::getTaskId, Function.identity()));

        log.info("[测试] 判定依据：各任务次数={}，D 状态={}",
                runs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue().get()).toList(),
                byId.get("D").getStatus());
        assertEquals(1, runs.get("A").get());
        assertEquals(1, runs.get("B").get());
        assertEquals(1, runs.get("C").get());
        assertEquals(1, runs.get("D").get(), "汇合任务在两个上游分别级联调度下只能执行一次");
        assertEquals(TaskStatus.SUCCEEDED, byId.get("D").getStatus());
    }

    private TaskDefinition counted(String id, List<String> deps,
                                   Map<String, AtomicInteger> runs,
                                   CountDownLatch entered, CountDownLatch release) {
        return new TaskDefinition(id, deps, RetryPolicy.noRetry(), ctx -> {
            runs.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            if (entered != null) {
                entered.countDown();
            }
            if (release != null) {
                release.await(10, TimeUnit.SECONDS);
            }
        });
    }
}
