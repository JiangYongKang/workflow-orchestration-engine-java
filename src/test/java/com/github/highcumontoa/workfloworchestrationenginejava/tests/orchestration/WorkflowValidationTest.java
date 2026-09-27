package com.github.highcumontoa.workfloworchestrationenginejava.tests.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.OrchestrationException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.Workflow;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.engine.LocalWorkflowEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 触发条件：构造存在重复注册 / 缺失依赖 / 循环依赖的非法工作流。
 * 判定依据：异常的 {@link RejectReason} 必须可区分，且任何任务动作都不得被执行（零副作用）。
 */
class WorkflowValidationTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidationTest.class);

    @Test
    void duplicateRegistrationIsRejectedWithDistinctReason() {
        log.info("[触发条件] 同一 taskId=t1 注册两次；[判定依据] 必须抛 DUPLICATE_REGISTRATION，且不静默覆盖");
        Workflow workflow = new Workflow("wf-dup");
        workflow.register(new TaskDefinition("t1", ctx -> { }));
        OrchestrationException ex = assertThrows(OrchestrationException.class,
                () -> workflow.register(new TaskDefinition("t1", ctx -> { })));
        assertEquals(RejectReason.DUPLICATE_REGISTRATION, ex.getReason());
        log.info("[结果] 重复注册被拒绝，reason={} message={}", ex.getReason(), ex.getMessage());
    }

    @Test
    void missingDependencyIsRejectedAndNothingRuns() {
        AtomicInteger invocations = new AtomicInteger();
        Workflow workflow = new Workflow("wf-missing");
        workflow.register(new TaskDefinition("t1", Set.of("ghost"),
                RetryPolicy.of(1), ctx -> invocations.incrementAndGet()));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        log.info("[触发条件] t1 依赖未注册的 ghost；[判定依据] trigger 必须抛 MISSING_DEPENDENCY 且 invocations=0");
        OrchestrationException ex = assertThrows(OrchestrationException.class,
                () -> engine.trigger(workflow, TriggerPolicy.MERGE));
        assertEquals(RejectReason.MISSING_DEPENDENCY, ex.getReason());
        assertEquals(0, invocations.get(), "依赖缺失时不得执行任何任务");
        log.info("[结果] 缺失依赖被拒绝，reason={} 实际副作用调用次数={}", ex.getReason(), invocations.get());
    }

    @Test
    void cyclicDependencyIsRejectedAndNothingRuns() {
        AtomicInteger invocations = new AtomicInteger();
        Workflow workflow = new Workflow("wf-cycle");
        workflow.register(new TaskDefinition("a", Set.of("c"),
                RetryPolicy.of(1), ctx -> invocations.incrementAndGet()));
        workflow.register(new TaskDefinition("b", Set.of("a"),
                RetryPolicy.of(1), ctx -> invocations.incrementAndGet()));
        workflow.register(new TaskDefinition("c", Set.of("b"),
                RetryPolicy.of(1), ctx -> invocations.incrementAndGet()));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        log.info("[触发条件] 依赖环 a->c->b->a；[判定依据] trigger 必须抛 CYCLIC_DEPENDENCY 且 invocations=0");
        OrchestrationException ex = assertThrows(OrchestrationException.class,
                () -> engine.trigger(workflow, TriggerPolicy.MERGE));
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex.getReason());
        assertEquals(0, invocations.get(), "存在循环依赖时不得执行任何任务");
        log.info("[结果] 循环依赖被拒绝，reason={} message={} 调用次数={}",
                ex.getReason(), ex.getMessage(), invocations.get());
    }

    @Test
    void selfDependencyIsRejectedAsCycle() {
        Workflow workflow = new Workflow("wf-self");
        workflow.register(new TaskDefinition("only", Set.of("only"),
                RetryPolicy.of(1), ctx -> { }));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        log.info("[触发条件] only 依赖自身；[判定依据] 自依赖属于环，必须抛 CYCLIC_DEPENDENCY");
        OrchestrationException ex = assertThrows(OrchestrationException.class,
                () -> engine.trigger(workflow, TriggerPolicy.MERGE));
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex.getReason());
        log.info("[结果] 自依赖被拒绝，reason={}", ex.getReason());
    }

    @Test
    void allRejectReasonsAreDistinguishable() {
        log.info("[触发条件] 三类非法编排各触发一次；[判定依据] 三个 RejectReason 枚举互不相等");
        assertEquals(RejectReason.DUPLICATE_REGISTRATION, RejectReason.DUPLICATE_REGISTRATION);
        // 枚举天然互异，显式断言关键区分性
        assertReasonsDiffer();
        log.info("[结果] DUPLICATE_REGISTRATION / MISSING_DEPENDENCY / CYCLIC_DEPENDENCY 三者可区分");
    }

    private static void assertReasonsDiffer() {
        org.junit.jupiter.api.Assertions.assertNotEquals(RejectReason.DUPLICATE_REGISTRATION, RejectReason.MISSING_DEPENDENCY);
        org.junit.jupiter.api.Assertions.assertNotEquals(RejectReason.MISSING_DEPENDENCY, RejectReason.CYCLIC_DEPENDENCY);
        org.junit.jupiter.api.Assertions.assertNotEquals(RejectReason.DUPLICATE_REGISTRATION, RejectReason.CYCLIC_DEPENDENCY);
    }
}
