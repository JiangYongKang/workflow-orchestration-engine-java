package com.github.highcumontoa.workfloworchestrationenginejava.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.DuplicateTriggerException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：任务正在执行时，多线程同时重复触发同一任务 / 重复提交整个工作流。
 * 判定依据：MERGE 时执行体只运行一次且多方拿到同一结果；REJECT 时抛
 * {@link DuplicateTriggerException}；状态最终一致（全部 SUCCEEDED）。
 */
class ConcurrentTriggerTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentTriggerTest.class);

    private OrchestrationEngine newEngine() {
        return new OrchestrationEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("MERGE：20 个线程并发触发同一在途任务，执行体仅运行一次且结果合并")
    void concurrentTriggerMergeExecutesOnce() throws Exception {
        int threads = 20;
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // 第二道闸门：保证所有并发触发都在 A 仍处于 RUNNING 时发出，
        // 消除“部分触发在任务完成后才到达”的测试侧竞态
        CountDownLatch triggersReady = new CountDownLatch(threads - 1);
        CountDownLatch fire = new CountDownLatch(1);
        log.info("[测试] 触发条件：{} 个线程在 A 执行期间并发 triggerTask(A, MERGE)", threads);

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("conc-merge-wf", List.of(
                new TaskDefinition("A", List.of(), RetryPolicy.noRetry(), ctx -> {
                    runs.incrementAndGet();
                    entered.countDown();
                    release.await(10, TimeUnit.SECONDS);
                })
        )));

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CompletableFuture<TaskRecord> first = engine.triggerTask("A", TriggerPolicy.MERGE);
            assertTrue(entered.await(5, TimeUnit.SECONDS), "A 应当已进入执行体");

            List<Future<TaskRecord>> merged = new java.util.ArrayList<>();
            for (int i = 0; i < threads - 1; i++) {
                merged.add(pool.submit(() -> {
                    triggersReady.countDown();
                    fire.await(10, TimeUnit.SECONDS);
                    return engine.triggerTask("A", TriggerPolicy.MERGE).get(10, TimeUnit.SECONDS);
                }));
            }
            // 等所有触发线程就位后再统一放行，此时 A 仍被 release 闸门挡在执行体中
            assertTrue(triggersReady.await(5, TimeUnit.SECONDS), "所有触发线程应当就位");
            fire.countDown();
            // 给触发线程一个时间窗口完成“在途合并”，然后才放 A 结束
            Thread.sleep(100);
            release.countDown();

            TaskRecord ref = first.get(5, TimeUnit.SECONDS);
            for (Future<TaskRecord> f : merged) {
                assertSame(ref, f.get(5, TimeUnit.SECONDS), "MERGE 必须复用同一个在途结果对象");
            }
        } finally {
            release.countDown();
            pool.shutdownNow();
        }

        log.info("[测试] 判定依据：执行体实际运行次数={}（预期 1），终态={}",
                runs.get(), engine.getStatus("A").getStatus());
        assertEquals(1, runs.get(), "并发重复触发不得导致重复执行");
        assertEquals(TaskStatus.SUCCEEDED, engine.getStatus("A").getStatus());
    }

    @Test
    @DisplayName("REJECT：任务执行中的重复触发被明确拒绝，异常类型可区分")
    void concurrentTriggerRejectThrows() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        log.info("[测试] 触发条件：A 执行期间再次 triggerTask(A, REJECT)；预期判定：DuplicateTriggerException");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("conc-reject-wf", List.of(
                new TaskDefinition("A", List.of(), RetryPolicy.noRetry(), ctx -> {
                    entered.countDown();
                    release.await(5, TimeUnit.SECONDS);
                })
        )));

        CompletableFuture<TaskRecord> first = engine.triggerTask("A", TriggerPolicy.REJECT);
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        try {
            // 拒绝发生在同步预留阶段，直接抛出可区分异常
            DuplicateTriggerException ex = assertThrows(DuplicateTriggerException.class,
                    () -> engine.triggerTask("A", TriggerPolicy.REJECT));
            log.info("[测试] 判定依据：异常类型={}, message={}",
                    ex.getClass().getSimpleName(), ex.getMessage());
        } finally {
            release.countDown();
        }
        assertEquals(TaskStatus.SUCCEEDED, first.get(5, TimeUnit.SECONDS).getStatus());
    }

    @Test
    @DisplayName("全量触发并发：两个线程同时 triggerAll(MERGE)，每个任务执行体只运行一次")
    void concurrentTriggerAllMergeRunsEachOnce() throws Exception {
        ConcurrentHashMap<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch canEnterB = new CountDownLatch(1);
        log.info("[测试] 触发条件：DAG A->B->C，两个线程在 A 执行期间同时提交 triggerAll(MERGE)");

        OrchestrationEngine engine = newEngine();
        List<TaskDefinition> defs = List.of(
                gated("A", List.of(), runs, entered, release, null),
                gated("B", List.of("A"), runs, null, null, canEnterB),
                simple("C", List.of("A", "B"), runs)
        );
        engine.register(new WorkflowDefinition("conc-all-wf", defs));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> f1 = pool.submit(() -> engine.triggerAll(TriggerPolicy.MERGE).join());
            assertTrue(entered.await(5, TimeUnit.SECONDS), "A 应已进入执行");
            Future<?> f2 = pool.submit(() -> engine.triggerAll(TriggerPolicy.MERGE).join());
            release.countDown();
            canEnterB.countDown();
            f1.get(10, TimeUnit.SECONDS);
            f2.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            canEnterB.countDown();
            pool.shutdownNow();
        }

        log.info("[测试] 判定依据：各任务执行次数={}（预期全部为 1）",
                runs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue().get()).toList());
        assertEquals(1, runs.get("A").get());
        assertEquals(1, runs.get("B").get());
        assertEquals(1, runs.get("C").get());
        for (TaskRecord r : engine.getAllStatus()) {
            assertEquals(TaskStatus.SUCCEEDED, r.getStatus());
        }
    }

    private TaskDefinition simple(String id, List<String> deps,
                                  ConcurrentHashMap<String, AtomicInteger> runs) {
        return new TaskDefinition(id, deps, RetryPolicy.noRetry(), ctx ->
                runs.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet());
    }

    private TaskDefinition gated(String id, List<String> deps,
                                 ConcurrentHashMap<String, AtomicInteger> runs,
                                 CountDownLatch entered, CountDownLatch release,
                                 CountDownLatch before) {
        return new TaskDefinition(id, deps, RetryPolicy.noRetry(), ctx -> {
            runs.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            if (before != null) {
                before.await(5, TimeUnit.SECONDS);
            }
            if (entered != null) {
                entered.countDown();
            }
            if (release != null) {
                release.await(5, TimeUnit.SECONDS);
            }
        });
    }
}
