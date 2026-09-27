package com.github.highcumontoa.workfloworchestrationenginejava.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.TaskNotRunnableException;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：提交 A->B->C 型 DAG 并全量触发。
 * 判定依据：执行顺序严格满足依赖；全部 SUCCEEDED；已成功任务重复触发不产生二次副作用。
 */
class OrchestrationHappyPathTest {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationHappyPathTest.class);

    private OrchestrationEngine newEngine() {
        return new OrchestrationEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("全量触发：任务严格按依赖顺序执行且全部成功")
    void runsInDependencyOrder() throws Exception {
        ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();
        log.info("[测试] 触发条件：DAG A 无依赖，B 依赖 A，C 依赖 A,B；全量触发一次");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("happy-wf", List.of(
                def("A", List.of(), order),
                def("B", List.of("A"), order),
                def("C", List.of("A", "B"), order)
        )));

        List<TaskRecord> result = engine.triggerAll(TriggerPolicy.MERGE)
                .get(5, TimeUnit.SECONDS);
        log.info("[测试] 判定依据：实际执行顺序={}，终态={}", order,
                result.stream().map(r -> r.getTaskId() + "=" + r.getStatus()).toList());

        assertEquals(List.of("A", "B", "C"), List.copyOf(order));
        for (TaskRecord r : result) {
            assertEquals(TaskStatus.SUCCEEDED, r.getStatus(),
                    "任务 " + r.getTaskId() + " 应成功");
            assertEquals(1, r.getAttempts());
        }
    }

    @Test
    @DisplayName("重复触发已成功任务：直接返回成功结论，执行体不第二次运行")
    void succeededTaskIsNotReExecuted() throws Exception {
        ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();
        log.info("[测试] 触发条件：A 成功后再次 triggerTask(A)；预期判定：副作用仍只发生一次");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("idem-wf", List.of(def("A", List.of(), order))));

        engine.triggerAll(TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);
        TaskRecord again = engine.triggerTask("A", TriggerPolicy.MERGE).get(5, TimeUnit.SECONDS);

        log.info("[测试] 判定依据：执行次数={}, 第二次返回状态={}, 代次={}",
                order.size(), again.getStatus(), again.getGeneration());
        assertEquals(1, order.size(), "成功任务的副作用只能发生一次");
        assertEquals(TaskStatus.SUCCEEDED, again.getStatus());
        assertEquals(0, again.getGeneration());
    }

    @Test
    @DisplayName("单任务触发时上游未成功：明确拒绝（TaskNotRunnableException），不静默跳过")
    void triggerTaskWithUnfinishedUpstreamIsRejected() {
        ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();
        log.info("[测试] 触发条件：未跑过 A 直接 triggerTask(B)（B 依赖 A）；预期判定：TaskNotRunnableException");

        OrchestrationEngine engine = newEngine();
        engine.register(new WorkflowDefinition("up-wf", List.of(
                def("A", List.of(), order),
                def("B", List.of("A"), order)
        )));

        TaskNotRunnableException ex = assertThrows(TaskNotRunnableException.class,
                () -> engine.triggerTask("B", TriggerPolicy.MERGE));
        log.info("[测试] 判定依据：异常类型={}, message={}", ex.getClass().getSimpleName(), ex.getMessage());
        assertTrue(ex.getMessage().contains("A"));
        assertTrue(order.isEmpty(), "拒绝触发时不应有任何任务被执行");
    }

    private TaskDefinition def(String id, List<String> deps, ConcurrentLinkedQueue<String> sink) {
        return new TaskDefinition(id, deps, RetryPolicy.noRetry(), ctx -> sink.add(id));
    }
}
