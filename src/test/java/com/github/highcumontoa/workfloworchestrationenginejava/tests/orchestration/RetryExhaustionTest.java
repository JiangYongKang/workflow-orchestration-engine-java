package com.github.highcumontoa.workfloworchestrationenginejava.tests.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.Workflow;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.WorkflowRun;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.engine.LocalWorkflowEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.FailureConclusion;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskState;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：任务 B 持续失败并配置 3 次尝试；拓扑 A;B<-A;C<-B;D<-C;E 独立。
 * 判定依据：
 * B 恰好尝试 3 次后 FAILED；C、D 全部 BLOCKED 且副作用次数为 0；A 与独立任务 E 仍 SUCCEEDED；
 * 失败结论可查询（失败任务、尝试次数、错误信息、被阻断集合）。
 */
class RetryExhaustionTest {

    private static final Logger log = LoggerFactory.getLogger(RetryExhaustionTest.class);

    @Test
    void retriesUpToLimitThenBlocksDownstreamAndKeepsConclusion() {
        AtomicInteger aCalls = new AtomicInteger();
        AtomicInteger bCalls = new AtomicInteger();
        AtomicInteger cCalls = new AtomicInteger();
        AtomicInteger dCalls = new AtomicInteger();
        AtomicInteger eCalls = new AtomicInteger();

        Workflow workflow = new Workflow("wf-retry");
        workflow.register(new TaskDefinition("A", ctx -> aCalls.incrementAndGet()));
        workflow.register(new TaskDefinition("B", Set.of("A"), RetryPolicy.of(3), ctx -> {
            bCalls.incrementAndGet();
            throw new IllegalStateException("boom-on-purpose");
        }));
        workflow.register(new TaskDefinition("C", Set.of("B"), RetryPolicy.of(1), ctx -> cCalls.incrementAndGet()));
        workflow.register(new TaskDefinition("D", Set.of("C"), RetryPolicy.of(1), ctx -> dCalls.incrementAndGet()));
        workflow.register(new TaskDefinition("E", Set.of(), RetryPolicy.of(1), ctx -> eCalls.incrementAndGet()));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        log.info("[触发条件] B 始终抛异常且 maxAttempts=3；[判定依据] attempts(B)=3, B=FAILED, C/D=BLOCKED 且无副作用, A/E=SUCCEEDED");
        WorkflowRun run = engine.trigger(workflow, TriggerPolicy.MERGE);

        assertFalse(run.succeeded(), "存在重试耗尽任务时运行不得判定为成功");
        assertTrue(run.isDone(), "阶段必须以终态结束");

        assertEquals(3, bCalls.get(), "B 必须恰好尝试 3 次");
        assertEquals(3, run.getAttempts("B"), "记录的尝试次数应为 3");
        assertEquals(TaskState.FAILED, run.getState("B"));
        assertEquals(TaskState.SUCCEEDED, run.getState("A"));
        assertEquals(TaskState.BLOCKED, run.getState("C"));
        assertEquals(TaskState.BLOCKED, run.getState("D"));
        assertEquals(TaskState.SUCCEEDED, run.getState("E"), "与失败点无关的独立任务不应被阻断");
        assertEquals(0, cCalls.get(), "被阻断任务不得产生副作用");
        assertEquals(0, dCalls.get(), "被阻断任务不得产生副作用");
        assertEquals(1, eCalls.get());
        log.info("[结果] 状态快照={} 调用次数 A={} B={} C={} D={} E={}",
                run.stateSnapshot(), aCalls.get(), bCalls.get(), cCalls.get(), dCalls.get(), eCalls.get());

        FailureConclusion conclusion = run.failureConclusion();
        assertNotNull(conclusion, "重试耗尽后必须保留可查询的失败结论");
        assertEquals("B", conclusion.failedTaskId());
        assertEquals(3, conclusion.attempts());
        assertTrue(conclusion.errorMessage().contains("boom-on-purpose"), "失败结论需保留原始错误信息");
        assertEquals(Set.of("C", "D"), conclusion.blockedTasks(), "失败结论需给出完整被阻断下游集合");
        assertEquals("boom-on-purpose", run.getLastError("B"));
        log.info("[结果] 失败结论: failedTask={} attempts={} blocked={} error={}",
                conclusion.failedTaskId(), conclusion.attempts(), conclusion.blockedTasks(), conclusion.errorMessage());
    }

    @Test
    void taskSucceedsWithinRetryBudget() {
        AtomicInteger calls = new AtomicInteger();
        Workflow workflow = new Workflow("wf-retry-recover-within");
        workflow.register(new TaskDefinition("R", RetryPolicy.of(3), ctx -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("transient-" + calls.get());
            }
        }));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        log.info("[触发条件] R 前 2 次失败、第 3 次成功，maxAttempts=3；[判定依据] 最终 SUCCEEDED 且尝试次数=3");
        WorkflowRun run = engine.trigger(workflow, TriggerPolicy.MERGE);
        assertEquals(TaskState.SUCCEEDED, run.getState("R"));
        assertEquals(3, run.getAttempts("R"));
        assertEquals(3, run.getInvocationCount("R"));
        log.info("[结果] R 在重试预算内成功，attempts=3，无失败结论: {}", run.failureConclusion());
    }
}
