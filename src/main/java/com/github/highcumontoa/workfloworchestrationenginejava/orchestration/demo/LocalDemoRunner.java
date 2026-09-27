package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.demo;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.OrchestrationException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.Workflow;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.WorkflowRun;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.engine.LocalWorkflowEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 不依赖任何外部服务的本地演示：可直接运行 {@code main} 观察
 * 正常编排、重试耗尽阻断、失败点恢复的完整状态变化。
 */
public final class LocalDemoRunner {

    private LocalDemoRunner() {
    }

    public static void main(String[] args) {
        demoRetryAndRecovery();
        demoRejectionReasons();
    }

    private static void demoRetryAndRecovery() {
        AtomicInteger bCalls = new AtomicInteger();
        AtomicInteger flaky = new AtomicInteger(3);

        Workflow workflow = new Workflow("demo-order-pipeline");
        workflow.register(new TaskDefinition("loadData", ctx -> System.out.println("  -> 执行 loadData")));
        workflow.register(new TaskDefinition("transform", Set.of("loadData"), RetryPolicy.of(3), ctx -> {
            bCalls.incrementAndGet();
            System.out.println("  -> 执行 transform 第 " + ctx.getAttempt() + " 次");
            if (flaky.decrementAndGet() >= 0) {
                throw new IllegalStateException("模拟的临时性失败");
            }
        }));
        workflow.register(new TaskDefinition("publish", Set.of("transform"),
                RetryPolicy.of(1), ctx -> System.out.println("  -> 执行 publish")));

        try (LocalWorkflowEngine engine = new LocalWorkflowEngine()) {
            System.out.println("=== 第一次触发：transform 三次尝试全部失败 ===");
            WorkflowRun run = engine.trigger(workflow, TriggerPolicy.MERGE);
            System.out.println("状态快照: " + run.stateSnapshot());
            System.out.println("失败结论: " + run.failureConclusion());

            System.out.println("=== 从失败点恢复：loadData 跳过，transform 新预算首次成功，publish 补跑 ===");
            WorkflowRun recovered = engine.recover(run);
            System.out.println("恢复后状态: " + recovered.stateSnapshot());
            System.out.println("transform 累计调用次数: " + recovered.getInvocationCount("transform")
                    + "（首次 3 次 + 恢复 1 次）");
            System.out.println("loadData 累计调用次数: " + recovered.getInvocationCount("loadData")
                    + "（恢复时跳过，无重复副作用）");
            System.out.println("事件轨迹条数: " + recovered.events().size());
        }
    }

    private static void demoRejectionReasons() {
        System.out.println("=== 非法编排拒绝原因演示（互不相同）===");
        try (LocalWorkflowEngine engine = new LocalWorkflowEngine()) {
            Workflow cyclic = new Workflow("demo-cycle");
            cyclic.register(new TaskDefinition("a", Set.of("b"), RetryPolicy.of(1), ctx -> { }));
            cyclic.register(new TaskDefinition("b", Set.of("a"), RetryPolicy.of(1), ctx -> { }));
            try {
                engine.trigger(cyclic, TriggerPolicy.MERGE);
            } catch (OrchestrationException e) {
                System.out.println("拒绝原因: " + e.getReason() + " -> " + e.getMessage());
            }

            Workflow missing = new Workflow("demo-missing");
            missing.register(new TaskDefinition("a", Set.of("ghost"), RetryPolicy.of(1), ctx -> { }));
            try {
                engine.trigger(missing, TriggerPolicy.MERGE);
            } catch (OrchestrationException e) {
                System.out.println("拒绝原因: " + e.getReason() + " -> " + e.getMessage());
            }

            Workflow dup = new Workflow("demo-dup");
            dup.register(new TaskDefinition("a", ctx -> { }));
            try {
                dup.register(new TaskDefinition("a", ctx -> { }));
            } catch (OrchestrationException e) {
                System.out.println("拒绝原因: " + e.getReason() + " -> " + e.getMessage());
            }
            System.out.println("全部拒绝原因枚举: " + List.of(RejectReason.values()));
        }
    }
}
