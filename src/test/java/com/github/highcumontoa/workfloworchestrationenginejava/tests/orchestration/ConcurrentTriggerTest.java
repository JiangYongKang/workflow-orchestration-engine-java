package com.github.highcumontoa.workfloworchestrationenginejava.tests.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.OrchestrationException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.Workflow;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.WorkflowRun;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.engine.LocalWorkflowEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：首个任务内部用闩锁阻塞，期间 N 个线程同时触发同一工作流。
 * 判定依据：
 * <ul>
 *     <li>MERGE：全部调用合并到同一次运行（runId 唯一），每个任务副作用只产生一次；</li>
 *     <li>REJECT：恰有 1 个触发获得执行权，其余 N-1 个以 TRIGGER_REJECTED_ALREADY_RUNNING 拒绝。</li>
 * </ul>
 */
class ConcurrentTriggerTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentTriggerTest.class);
    private static final int THREADS = 20;

    @Test
    void mergePolicyCollapsesConcurrentTriggersIntoSingleRun() throws Exception {
        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        AtomicInteger invocationsA = new AtomicInteger();
        AtomicInteger invocationsB = new AtomicInteger();

        Workflow workflow = new Workflow("wf-concurrent-merge");
        workflow.register(new TaskDefinition("A", ctx -> {
            invocationsA.incrementAndGet();
            taskEntered.countDown();
            releaseTask.await(5, TimeUnit.SECONDS);
        }));
        workflow.register(new TaskDefinition("B", Set.of("A"), RetryPolicy.of(1), ctx -> invocationsB.incrementAndGet()));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        Set<String> runIds = Collections.synchronizedSet(new HashSet<>());
        AtomicInteger returned = new AtomicInteger();

        try {
            // 第一个线程负责建立在途运行
            Future<?> first = pool.submit(() -> {
                WorkflowRun r = engine.trigger(workflow, TriggerPolicy.MERGE);
                runIds.add(r.getRunId());
                returned.incrementAndGet();
            });
            assertTrue(taskEntered.await(5, TimeUnit.SECONDS), "首个任务应已进入并阻塞");
            log.info("[触发条件] 在途运行已建立且任务 A 阻塞，剩余 {} 个线程同时以 MERGE 触发", THREADS - 1);

            for (int i = 0; i < THREADS - 1; i++) {
                pool.submit(() -> {
                    WorkflowRun r = engine.trigger(workflow, TriggerPolicy.MERGE);
                    runIds.add(r.getRunId());
                    returned.incrementAndGet();
                    return null;
                });
            }
            // 给并发触发一点时间全部进入等待，再放行任务
            Thread.sleep(300);
            releaseTask.countDown();
            first.get(5, TimeUnit.SECONDS);
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            releaseTask.countDown();
            pool.shutdownNow();
            engine.close();
        }

        assertEquals(THREADS, returned.get(), "所有触发都应拿到返回（合并语义）");
        assertEquals(1, runIds.size(), "MERGE 必须合并为同一次运行，实际 runId 集合=" + runIds);
        assertEquals(1, invocationsA.get(), "任务 A 副作用只能产生一次");
        assertEquals(1, invocationsB.get(), "任务 B 副作用只能产生一次");
        log.info("[结果] {} 个并发触发全部合并到 runId={}，A/B 调用次数均为 1（判定依据：runId 唯一且副作用计数=1）",
                THREADS, runIds);
    }

    @Test
    void rejectPolicyRefusesConcurrentTriggersWithDistinctReason() throws Exception {
        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();

        Workflow workflow = new Workflow("wf-concurrent-reject");
        workflow.register(new TaskDefinition("A", ctx -> {
            invocations.incrementAndGet();
            taskEntered.countDown();
            releaseTask.await(5, TimeUnit.SECONDS);
        }));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger wrongReasons = new AtomicInteger();

        try {
            // 第一个线程建立在途运行
            Future<?> first = pool.submit(() -> {
                engine.trigger(workflow, TriggerPolicy.REJECT);
                accepted.incrementAndGet();
            });
            assertTrue(taskEntered.await(5, TimeUnit.SECONDS));
            log.info("[触发条件] 在途运行已建立，{} 个线程同时以 REJECT 触发；[判定依据] 仅首个成功，其余原因必须是 TRIGGER_REJECTED_ALREADY_RUNNING",
                    THREADS - 1);

            for (int i = 0; i < THREADS - 1; i++) {
                pool.submit(() -> {
                    try {
                        engine.trigger(workflow, TriggerPolicy.REJECT);
                        accepted.incrementAndGet();
                    } catch (OrchestrationException e) {
                        if (e.getReason() == RejectReason.TRIGGER_REJECTED_ALREADY_RUNNING) {
                            rejected.incrementAndGet();
                        } else {
                            wrongReasons.incrementAndGet();
                        }
                    }
                    return null;
                });
            }
            Thread.sleep(300);
            releaseTask.countDown();
            first.get(5, TimeUnit.SECONDS);
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            releaseTask.countDown();
            pool.shutdownNow();
            engine.close();
        }

        assertEquals(1, accepted.get(), "REJECT 策略下只允许一个触发执行");
        assertEquals(THREADS - 1, rejected.get(), "其余触发必须全部被拒绝");
        assertEquals(0, wrongReasons.get(), "被拒绝原因必须统一且可区分");
        assertEquals(1, invocations.get(), "被拒绝的触发不得导致重复执行");
        log.info("[结果] accepted={} rejected={} wrongReasons={} 任务调用次数={}",
                accepted.get(), rejected.get(), wrongReasons.get(), invocations.get());
    }
}
