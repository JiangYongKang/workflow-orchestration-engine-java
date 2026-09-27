# 本地任务编排引擎（workflow-orchestration-engine-java）

一套**纯本地**的任务编排能力：把存在依赖关系的任务按拓扑顺序执行，支持并发触发去重、
失败重试、重试耗尽阻断下游，以及从失败点恢复重放。任务动作用本地 Java 方法模拟，
**不依赖任何外部服务 / 数据库 / 消息中间件**，状态全部保存在进程内存中。

- 技术栈：Java 21、Spring Boot 3.4.x（引擎本身不依赖 Spring 容器，可独立 new 出来使用）
- 核心代码：`src/main/java/.../orchestration/`
- 测试代码：`src/test/java/.../tests/orchestration/`

---

## 1. 编排模型

| 概念 | 类型 | 说明 |
| --- | --- | --- |
| 工作流 | `core.Workflow` | 任务注册表 + DAG 校验 + 拓扑排序（Kahn 算法，顺序稳定） |
| 任务定义 | `core.TaskDefinition` | `id`、`dependencies`（上游任务 ID 集合）、`RetryPolicy`、本地 `TaskAction` |
| 执行动作 | `core.TaskAction` | 函数式接口 `void execute(TaskContext)`，用本地代码模拟副作用 |
| 任务上下文 | `core.TaskContext` | 提供 `runId`、当前 `attempt`（从 1 开始）、同阶段共享数据 `sharedData` |
| 重试策略 | `core.RetryPolicy` | `maxAttempts`：最大尝试次数（含首次），必须 ≥ 1；默认 1（不重试） |
| 运行实例 | `core.WorkflowRun` | 一次工作流运行的全部状态：任务状态、尝试次数、**副作用累计调用次数**、事件轨迹、失败结论 |
| 引擎 | `engine.LocalWorkflowEngine` | 触发 / 恢复入口，保证单飞、重试阻断与恢复重放语义 |

依赖关系是一张有向无环图（DAG）：`B 依赖 A` 表示 A 必须先成功，B 才允许执行。
与失败点无关的独立任务不会被连带阻断。

### 非法编排必须显式拒绝（不部分执行、不静默跳过）

校验在**任何任务执行之前**完成，统一抛出带可区分原因的 `OrchestrationException`：

| `RejectReason` | 触发时机 | 含义 |
| --- | --- | --- |
| `DUPLICATE_REGISTRATION` | `workflow.register(...)` | 同一任务 ID 重复注册（拒绝覆盖，立即抛出） |
| `MISSING_DEPENDENCY` | `trigger/recover` | 某任务依赖了未注册的任务 |
| `CYCLIC_DEPENDENCY` | `trigger/recover` | 依赖图有环（含自依赖），异常信息附带检测到的环路径 |
| `TRIGGER_REJECTED_ALREADY_RUNNING` | `trigger(..., REJECT)` | 同一工作流已有执行中的运行 |
| `RUN_NOT_TERMINAL` | `recover(...)` | 运行尚未到达终态，或同工作流存在执行中的运行 |
| `RECOVERY_NOT_APPLICABLE` | `recover(...)` | 运行已全部成功 / 无失败结论，无需恢复 |

---

## 2. 任务状态流转

```
                ┌──────────────────────────────┐
                │                              │（恢复重放：已成功则跳过，保持 SUCCEEDED）
                ▼                              │
 PENDING ──调度──▶ RUNNING ──某次尝试成功──▶ SUCCEEDED（终态：成功，不重复产生副作用）
   ▲                 │
   │ 恢复时          ├──全部尝试失败──▶ FAILED（终态：重试耗尽，固化失败结论）
   │ 复位未成功任务   │                        │
   │                 └────────（其下游任务）──▶ BLOCKED（未执行、零副作用）
   └───────────────── recovery 阶段开始 ◀──────┘
```

- `PENDING`：已注册、等待调度或等待上游；
- `RUNNING`：任务正在执行（处于其 `maxAttempts` 尝试循环中）；
- `SUCCEEDED`：成功终态。恢复时**原样保留、直接跳过**，不重复执行；
- `FAILED`：达到 `maxAttempts` 后仍失败的终态，附带失败原因；
- `BLOCKED`：因（直接或间接）上游 FAILED 而被阻断，从不执行，调用次数保持 0。

所有状态变化都追加一条不可变的 `RunEvent`（时间戳、任务、from→to、attempt、说明），
可通过 `run.events()` 获取完整轨迹，保证**可追溯**。
`run.stateSnapshot()` 返回任务状态快照；`run.getInvocationCount(taskId)` 返回跨阶段累计副作用调用次数。

---

## 3. 并发触发：合并或拒绝，绝不重复执行

同一工作流在任意时刻**至多有一个执行中的 `WorkflowRun`**。引擎为每个工作流维护一把发布锁，
把“检查在途 → 决策 → 发布新运行”收敛为原子操作；`WorkflowRun` 构造即为“执行中”，
不存在“已发布但被误判为结束”的窗口。

| `TriggerPolicy` | 执行中再次触发的行为 |
| --- | --- |
| `MERGE`（默认） | 本次触发**合并**到在途运行：阻塞等待其终态，返回**同一个** run 实例/runId，不重复执行任何任务 |
| `REJECT` | 立即抛出 `TRIGGER_REJECTED_ALREADY_RUNNING` |

被合并的多次触发拿到的 `runId` 完全相同，且每个任务的副作用调用次数恒为 1。

---

## 4. 重试与恢复规则

### 4.1 失败重试
- 任务按自身 `RetryPolicy.maxAttempts` 尝试（含首次执行）；
- 每次失败记录错误信息与尝试序号；最后一次失败后进入 FAILED；
- **阻断范围仅限真实下游**：从失败任务沿依赖边反向可达的全部任务标记 BLOCKED；
  无依赖关系的独立任务照常执行；
- 重试耗尽后运行以终态结束，并固化可查询的失败结论：

  ```java
  FailureConclusion(failedTaskId, errorMessage, attempts, blockedTasks)
  ```

  通过 `run.failureConclusion()` 获取，包含失败任务、原始错误信息、尝试次数、被阻断任务集合。

### 4.2 从失败点恢复（`engine.recover(run)`）
- 仅允许对**已到终态且存在失败结论**的 run 恢复；否则分别以
  `RUN_NOT_TERMINAL` / `RECOVERY_NOT_APPLICABLE` 拒绝；
- 恢复是同一 `WorkflowRun` 上的一个新阶段（`runId` 不变），恢复前重新做一次 DAG 校验；
- 状态复位规则：
  - `SUCCEEDED` 任务：**保持成功并跳过**，不调用其 action，因此**不重复产生副作用**；
  - `FAILED / BLOCKED / PENDING` 任务：复位为 PENDING，按拓扑顺序重新执行；
- **重试次数在新阶段重新计算**（失败任务重新获得完整的 `maxAttempts` 预算），
  但跨阶段的累计副作用调用次数持续累加，可用于核对“成功任务只执行一次”；
- 恢复若再次耗尽，下游继续保持 BLOCKED，并刷新失败结论；可反复恢复直至成功；
- 恢复成功后失败结论清空，全部任务 SUCCEEDED。

---

## 5. 本地验证方法

### 5.1 运行全部单元测试

```bash
mvn test
```

共 14 个测试（含 Spring 上下文加载），日志中以 `[触发条件]` / `[判定依据]` / `[结果]`
打印每个场景的输入与断言依据：

| 测试类 | 覆盖场景 |
| --- | --- |
| `WorkflowValidationTest` | 重复注册、缺失依赖、循环依赖（含自依赖）的拒绝，原因可区分且零副作用 |
| `ExecutionOrderTest` | DAG 拓扑执行顺序、共享数据传递、状态流转、事件轨迹 |
| `ConcurrentTriggerTest` | 20 线程并发触发：MERGE 合并为单次运行；REJECT 仅 1 个成功、其余以专属原因拒绝 |
| `RetryExhaustionTest` | 3 次尝试耗尽 → FAILED + 下游 BLOCKED + 失败结论可查询；预算内成功；独立任务不受影响 |
| `RecoveryReplayTest` | 失败点恢复：成功任务副作用不重复、新预算重试、阻断解除补跑、再次失败、无可恢复内容被拒 |

也可以只跑某一个类：

```bash
mvn test -Dtest=RetryExhaustionTest
```

### 5.2 运行本地演示（无需测试框架）

```bash
mvn -q compile
mvn -q org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
    -Dexec.mainClass=com.github.highcumontoa.workfloworchestrationenginejava.orchestration.demo.LocalDemoRunner
```

演示（`orchestration.demo.LocalDemoRunner`，可直接从 IDE 运行 `main`）会依次打印：
首次触发三次重试失败 → 阻断 publish → 输出失败结论 → 从失败点恢复
（loadData 跳过、transform 用新预算首次成功、publish 补跑）→ 全部成功，
以及循环依赖 / 缺失依赖 / 重复注册三种可区分的拒绝原因。

### 5.3 最小使用示例

```java
Workflow wf = new Workflow("orders");
wf.register(new TaskDefinition("load", ctx -> { /* 本地副作用 */ }));
wf.register(new TaskDefinition("transform", Set.of("load"), RetryPolicy.of(3), ctx -> { /* ... */ }));
wf.register(new TaskDefinition("publish", Set.of("transform"), RetryPolicy.of(1), ctx -> { /* ... */ }));

try (LocalWorkflowEngine engine = new LocalWorkflowEngine()) {
    WorkflowRun run = engine.trigger(wf, TriggerPolicy.MERGE);
    if (!run.succeeded()) {
        FailureConclusion c = run.failureConclusion();   // 查询失败结论
        engine.recover(run);                             // 从失败点恢复
    }
}
```
