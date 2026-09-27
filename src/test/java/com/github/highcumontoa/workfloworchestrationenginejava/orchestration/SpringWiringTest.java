package com.github.highcumontoa.workfloworchestrationenginejava.orchestration;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.runtime.OrchestrationEngine;
import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 触发条件：启动 Spring 上下文并注入编排组件。
 * 判定依据：WorkflowValidator / OrchestrationEngine 均为非 null Bean。
 */
@SpringBootTest
class SpringWiringTest {

    private static final Logger log = LoggerFactory.getLogger(SpringWiringTest.class);

    @Autowired
    private WorkflowValidator validator;

    @Autowired
    private OrchestrationEngine engine;

    @Test
    @DisplayName("Spring 装配：校验器与编排引擎均可注入")
    void beansAreWired() {
        log.info("[测试] 触发条件：注入 WorkflowValidator 与 OrchestrationEngine；判定依据：两者非 null");
        assertNotNull(validator);
        assertNotNull(engine);
    }
}
