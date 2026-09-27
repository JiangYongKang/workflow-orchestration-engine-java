package com.github.highcumontoa.workfloworchestrationenginejava.tests.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.Workflow;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core.WorkflowRun;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.engine.LocalWorkflowEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskState;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TriggerPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：合法 DAG（A 无依赖，B 依赖 A，C 依赖 A、B，D 独立）。
 * 判定依据：执行完成顺序满足拓扑约束；全部终态 SUCCEEDED；事件轨迹完整可追溯；
 * 独立任务 D 不依赖失败点，可在 A/B/C 之外独立执行。
 */
class ExecutionOrderTest {

    private static final Logger log = LoggerFactory.getLogger(ExecutionOrderTest.class);

    @Test
    void executesInTopologicalOrderWithTraceableStates() {
        CopyOnWriteArrayList<String> finishOrder = new CopyOnWriteArrayList<>();
        Workflow workflow = new Workflow("wf-order");
        workflow.register(new TaskDefinition("A", ctx -> {
            ctx.getSharedData().put("fromA", "product-A");
            finishOrder.add("A");
        }));
        workflow.register(new TaskDefinition("B", Set.of("A"), RetryPolicy.of(1), ctx -> {
            assertEquals("product-A", ctx.getSharedData().get("fromA"), "B 执行时必须能读到 A 写入的产物");
            finishOrder.add("B");
        }));
        workflow.register(new TaskDefinition("C", Set.of("A", "B"), RetryPolicy.of(1), ctx -> {
            finishOrder.add("C");
        }));
        workflow.register(new TaskDefinition("D", Set.of(), RetryPolicy.of(1), ctx -> finishOrder.add("D")));

        LocalWorkflowEngine engine = new LocalWorkflowEngine();
        log.info("[触发条件] DAG A;B<-A;C<-A,B;D 独立；[判定依据] A 在 B/C 前、B 在 C 前，全部 SUCCEEDED");
        WorkflowRun run = engine.trigger(workflow, TriggerPolicy.MERGE);

        assertTrue(run.succeeded(), "合法 DAG 必须全部成功");
        assertEquals(TaskState.SUCCEEDED, run.getState("A"));
        assertEquals(TaskState.SUCCEEDED, run.getState("B"));
        assertEquals(TaskState.SUCCEEDED, run.getState("C"));
        assertEquals(TaskState.SUCCEEDED, run.getState("D"));
        assertBefore(finishOrder, "A", "B");
        assertBefore(finishOrder, "A", "C");
        assertBefore(finishOrder, "B", "C");
        assertEquals(4, finishOrder.size());
        log.info("[结果] 实际完成顺序={}，状态快照={}", finishOrder, run.stateSnapshot());

        List<?> taskEvents = run.events().stream().filter(e -> e.taskId() != null).toList();
        assertTrue(taskEvents.size() >= 4, "每个任务至少应有 PENDING/RUNNING/SUCCEEDED 相关事件");
        assertTrue(run.events().stream().anyMatch(e -> e.message() != null && e.message().contains("阶段开始")),
                "事件轨迹必须包含阶段开始事件，保证可追溯");
        log.info("[结果] 事件轨迹共 {} 条，首条={}", run.events().size(),
                run.events().get(0).message());
    }

    private static void assertBefore(List<String> order, String first, String second) {
        assertTrue(order.indexOf(first) >= 0 && order.indexOf(second) >= 0
                        && order.indexOf(first) < order.indexOf(second),
                "期望 " + first + " 先于 " + second + "，实际顺序=" + order);
    }
}
