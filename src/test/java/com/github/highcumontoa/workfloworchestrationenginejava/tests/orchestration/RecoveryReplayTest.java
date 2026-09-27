package com.github.highcumontoa.workfloworchestrationenginejava.tests.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.OrchestrationException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.Workflow;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.WorkflowRun;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.engine.LocalWorkflowEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskState;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：首次运行 B 始终失败（3 次耗尽），A 成功、C 阻断。
 * 判定依据（恢复重放）：
 * 恢复后 A 保持 SUCCEEDED 且累计副作用仍为 1（不重复）；B 用全新重试预算执行并成功；
 * C 解除阻断执行成功；runId 不变；失败结论被清空；事件轨迹包含“恢复阶段”，全程可追溯。
 */
class RecoveryReplayTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryReplayTest.class);

    @Test
    void recoverySkipsSucceededTasksAndReplaysFromFailurePoint() {
        AtomicInteger aCalls = new AtomicInteger();
        AtomicInteger bCalls = new AtomicInteger();
        AtomicInteger cCalls = new AtomicInteger();
        AtomicInteger bFailureCountdown = new AtomicInteger(3); // 首次阶段 3 次尝试全部失败

        Workflow workflow = new Workflow("wf-recovery");
        workflow.register(new TaskDefinition("A", ctx -> aCalls.incrementAndGet()));
        workflow.register(new TaskDefinition("B", Set.of("A"), RetryPolicy.of(3), ctx -> {
            bCalls.incrementAndGet();
            if (bFailureCountdown.decrementAndGet() >= 0) {
                throw new IllegalStateException("transient-b");
            }
            // 恢复阶段第一次尝试即成功（倒计时此时已为 -1）
        }));
        workflow.register(new TaskDefinition("C", Set.of("B"), RetryPolicy.of(1), ctx -> cCalls.incrementAndGet()));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        log.info("[触发条件] 首次运行 B 三次尝试均失败；[判定依据] B=FAILED,C=BLOCKED,A=SUCCEEDED，失败结论可查询");
        WorkflowRun run = engine.trigger(workflow, TriggerPolicy.MERGE);
        String runIdBefore = run.getRunId();
        assertEquals(TaskState.FAILED, run.getState("B"));
        assertEquals(TaskState.BLOCKED, run.getState("C"));
        assertEquals(1, aCalls.get());
        assertEquals(3, bCalls.get());
        assertEquals(0, cCalls.get());
        assertNotNull(run.failureConclusion());
        log.info("[首次结果] snapshot={} A调用={} B调用={} C调用={} 失败任务={}",
                run.stateSnapshot(), aCalls.get(), bCalls.get(), cCalls.get(),
                run.failureConclusion().failedTaskId());

        log.info("[触发条件] 对同一 run 执行 recover；[判定依据] A 跳过(副作用不重复)、B 新预算重试成功、C 补跑成功");
        WorkflowRun recovered = engine.recover(run);

        assertTrue(recovered == run, "恢复必须复用同一个 WorkflowRun 实例");
        assertEquals(runIdBefore, recovered.getRunId(), "恢复不产生新 runId");
        assertTrue(recovered.succeeded(), "恢复成功后全部任务应为 SUCCEEDED");
        assertEquals(TaskState.SUCCEEDED, recovered.getState("A"));
        assertEquals(TaskState.SUCCEEDED, recovered.getState("B"));
        assertEquals(TaskState.SUCCEEDED, recovered.getState("C"));

        assertEquals(1, aCalls.get(), "已成功任务 A 在恢复重放中绝不能重复产生副作用");
        assertEquals(4, bCalls.get(), "B 首次 3 次 + 恢复阶段 1 次 = 4 次累计调用");
        assertEquals(1, cCalls.get(), "被阻断的 C 应在恢复阶段补跑一次");
        assertEquals(1, recovered.getInvocationCount("A"));
        assertEquals(1, recovered.getAttempts("A"), "A 未参与恢复阶段，保留首次成功时的 1 次尝试记录");
        assertEquals(1, recovered.getAttempts("B"), "B 在恢复阶段第 1 次尝试即成功");
        log.info("[恢复结果] snapshot={} A累计调用={} B累计调用={} C累计调用={} 失败结论={}",
                recovered.stateSnapshot(), aCalls.get(), bCalls.get(), cCalls.get(),
                recovered.failureConclusion());

        long recoverySkips = recovered.events().stream()
                .filter(e -> e.message() != null && e.message().contains("恢复重放时跳过"))
                .count();
        assertTrue(recoverySkips >= 1, "事件轨迹必须记录已成功任务的跳过事件");
        assertTrue(recovered.events().stream()
                        .anyMatch(e -> e.message() != null && e.message().contains("失败点恢复")),
                "事件轨迹必须包含失败点恢复阶段标记，保证可追溯");
        log.info("[追溯] 事件轨迹共 {} 条，其中恢复跳过事件 {} 条；全部状态变化可从事件流复核",
                recovered.events().size(), recoverySkips);
    }

    @Test
    void recoveryThatFailsAgainKeepsBlockingAndQueryableConclusion() {
        AtomicInteger calls = new AtomicInteger();
        Workflow workflow = new Workflow("wf-recovery-again");
        workflow.register(new TaskDefinition("X", RetryPolicy.of(2), ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always-bad");
        }));
        workflow.register(new TaskDefinition("Y", Set.of("X"), RetryPolicy.of(1), ctx -> { }));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        WorkflowRun run = engine.trigger(workflow, TriggerPolicy.MERGE);
        assertEquals(2, calls.get());
        log.info("[触发条件] 恢复时 X 仍然全部失败(maxAttempts=2)；[判定依据] 恢复阶段尝试 2 次，Y 仍 BLOCKED，结论刷新");
        WorkflowRun again = engine.recover(run);
        assertEquals(TaskState.FAILED, again.getState("X"));
        assertEquals(TaskState.BLOCKED, again.getState("Y"));
        assertEquals(2, again.getAttempts("X"), "恢复阶段重试次数重新计算，应为新的 2 次");
        assertEquals(4, calls.get(), "两次阶段累计调用 2+2=4");
        assertNotNull(again.failureConclusion());
        assertEquals(Set.of("Y"), again.failureConclusion().blockedTasks());
        log.info("[结果] 恢复再次失败: snapshot={} 阶段尝试={} 累计调用={} blocked={}",
                again.stateSnapshot(), again.getAttempts("X"), calls.get(),
                again.failureConclusion().blockedTasks());
    }

    @Test
    void recoveringSucceededRunIsRejectedWithDistinctReason() {
        Workflow workflow = new Workflow("wf-recovery-none");
        workflow.register(new TaskDefinition("Z", ctx -> { }));
        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        WorkflowRun run = engine.trigger(workflow, TriggerPolicy.MERGE);

        log.info("[触发条件] 对已全部成功的 run 执行 recover；[判定依据] 必须以 RECOVERY_NOT_APPLICABLE 拒绝");
        OrchestrationException ex = assertThrows(OrchestrationException.class, () -> engine.recover(run));
        assertEquals(RejectReason.RECOVERY_NOT_APPLICABLE, ex.getReason());
        log.info("[结果] 被拒绝 reason={} message={}", ex.getReason(), ex.getMessage());
    }
}
