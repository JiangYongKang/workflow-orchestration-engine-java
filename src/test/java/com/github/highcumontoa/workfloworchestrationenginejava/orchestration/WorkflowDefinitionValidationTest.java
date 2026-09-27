package com.github.highcumontoa.workfloworchestrationenginejava.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.CyclicDependencyException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.DuplicateTaskException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.exception.MissingDependencyException;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RetryPolicy;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 触发条件：向校验器提交包含不同缺陷的工作流定义。
 * 判定依据：三类缺陷必须抛出三种可区分的异常类型，且任何任务均未被执行。
 */
class WorkflowDefinitionValidationTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDefinitionValidationTest.class);

    private final WorkflowValidator validator = new WorkflowValidator();

    private TaskDefinition task(String id, List<String> deps) {
        return new TaskDefinition(id, deps, RetryPolicy.noRetry(), ctx -> { });
    }

    @Test
    @DisplayName("重复注册：同一 taskId 出现两次 -> DuplicateTaskException")
    void duplicateRegistrationIsRejected() {
        log.info("[测试] 触发条件：工作流中 taskId=A 注册两次；预期判定：DuplicateTaskException");
        WorkflowDefinition wf = new WorkflowDefinition("dup-wf", List.of(
                task("A", List.of()),
                task("A", List.of())
        ));
        DuplicateTaskException ex = assertThrows(DuplicateTaskException.class,
                () -> validator.validate(wf));
        log.info("[测试] 判定依据：实际异常类型={}, message={}", ex.getClass().getSimpleName(), ex.getMessage());
        assertTrue(ex.getMessage().contains("A"));
    }

    @Test
    @DisplayName("依赖缺失：B 依赖不存在的 X -> MissingDependencyException")
    void missingDependencyIsRejected() {
        log.info("[测试] 触发条件：taskId=B 声明依赖 X，但工作流中没有 X；预期判定：MissingDependencyException");
        WorkflowDefinition wf = new WorkflowDefinition("missing-wf", List.of(
                task("A", List.of()),
                task("B", List.of("X"))
        ));
        MissingDependencyException ex = assertThrows(MissingDependencyException.class,
                () -> validator.validate(wf));
        log.info("[测试] 判定依据：实际异常类型={}, message={}", ex.getClass().getSimpleName(), ex.getMessage());
        assertTrue(ex.getMessage().contains("X"));
    }

    @Test
    @DisplayName("循环依赖：A->B->C->A -> CyclicDependencyException 且消息还原环路径")
    void cyclicDependencyIsRejected() {
        log.info("[测试] 触发条件：A 依赖 C，C 依赖 B，B 依赖 A 构成环；预期判定：CyclicDependencyException");
        WorkflowDefinition wf = new WorkflowDefinition("cycle-wf", List.of(
                task("A", List.of("C")),
                task("B", List.of("A")),
                task("C", List.of("B"))
        ));
        CyclicDependencyException ex = assertThrows(CyclicDependencyException.class,
                () -> validator.validate(wf));
        log.info("[测试] 判定依据：实际异常类型={}, message={}", ex.getClass().getSimpleName(), ex.getMessage());
        assertTrue(ex.getMessage().contains("A"));
        assertTrue(ex.getMessage().contains("B"));
        assertTrue(ex.getMessage().contains("C"));
    }

    @Test
    @DisplayName("自依赖：A->A 也是循环依赖")
    void selfDependencyIsCycle() {
        log.info("[测试] 触发条件：taskId=A 依赖自身；预期判定：CyclicDependencyException");
        WorkflowDefinition wf = new WorkflowDefinition("self-wf", List.of(task("A", List.of("A"))));
        CyclicDependencyException ex = assertThrows(CyclicDependencyException.class,
                () -> validator.validate(wf));
        log.info("[测试] 判定依据：实际异常类型={}, message={}", ex.getClass().getSimpleName(), ex.getMessage());
        assertTrue(ex.getMessage().contains("A"));
    }

    @Test
    @DisplayName("合法 DAG：校验通过并产出依赖顺序（被依赖者排在前）")
    void validDagPassesAndTopoSorted() {
        log.info("[测试] 触发条件：C->[A,B] 的合法 DAG；预期判定：校验通过且拓扑序 A,B 在 C 之前");
        WorkflowDefinition wf = new WorkflowDefinition("ok-wf", List.of(
                task("A", List.of()),
                task("B", List.of("A")),
                task("C", List.of("A", "B"))
        ));
        var tasks = assertDoesNotThrow(() -> validator.validate(wf));
        var order = validator.topoSort(wf, tasks);
        log.info("[测试] 判定依据：拓扑顺序={}", order);
        assertEquals(3, order.size());
        assertTrue(order.indexOf("A") < order.indexOf("B"));
        assertTrue(order.indexOf("B") < order.indexOf("C"));
    }
}
