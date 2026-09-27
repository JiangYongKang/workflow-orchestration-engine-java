# 本地任务编排引擎（Workflow Orchestration Engine）

一个**纯本地、无外部服务依赖**的任务编排能力：把存在依赖关系的任务按既定顺序跑起来，
并处理并发触发去重、失败重试、失败点恢复与全链路状态追溯。

- 运行形态：Spring Boot 3.4 / Java 21，引擎为本地内存组件，任务执行体由调用方以函数式接口提供。
- 不依赖数据库、消息队列或任何外部服务；测试通过 `CountDownLatch` 控制时序，可离线运行（`mvn -o test`）。

---

## 1. 编排模型

### 1.1 核心概念

| 类型 | 说明 |
| --- | --- |
| `WorkflowDefinition` | 工作流定义：`workflowId` + 一组 `TaskDefinition`。 |
| `TaskDefinition` | 任务定义：`taskId`、依赖列表 `dependencies`、`RetryPolicy`、本地执行体 `TaskExecution`。 |
| `TaskExecution` | 函数式接口 `void execute(TaskExecutionContext)`，抛异常即视为本次尝试失败。 |
| `RetryPolicy` | `maxAttempts`（**总尝试次数，含首次**）+ `backoffMillis`（尝试间隔）。 |
| `TriggerPolicy` | 并发重复触发策略：`MERGE`（合并到同一在途执行）或 `REJECT`（明确拒绝）。 |
| `OrchestrationEngine` | 编排引擎：注册校验、调度、重试、阻断、恢复、查询。 |
| `TaskRuntime` | 单任务运行时：状态、代次（generation）、尝试次数、在途 Future、审计事件。 |

### 1.2 依赖关系与执行顺序

- 依赖是有向边 `task -> 它依赖的任务`；引擎在**注册时**对依赖图做强校验，
  使用 DFS 三色标记检测环、Kahn 入度算法产出拓扑序。
- 任务只会在**所有上游都 `SUCCEEDED`** 后被调度执行；不满足时保持 `WAITING`，
  由上游完成时的级联调度自动推进。
- `triggerTask(id, policy)` 只触发单个任务（要求上游已全部成功，**不隐式级联**）；
  `triggerAll(policy)` 一次性按依赖顺序执行整个工作流。

### 1.3 注册期的三类明确拒绝（原因可区分）

注册非法工作流时**立即抛出异常、引擎不部分生效**，三类问题对应三种异常类型：

| 场景 | 异常类型 | 触发示例 |
| --- | --- | --- |
| 同一 `taskId` 注册多次 / 同一依赖重复声明 | `DuplicateTaskException` | `A, A` 或 `B.deps=[X, X]` |
| 依赖指向不存在的任务 | `MissingDependencyException` | `B.deps=[X]` 但无 X |
| 依赖图存在环（含自依赖） | `CyclicDependencyException` | `A->B->C->A`，消息中还原环路径 |

---

## 2. 任务状态流转

```
                         注册
                          │
                          ▼
              ┌────── PENDING ───────┐ 单任务触发（上游均成功）
              │                      │
  全量/恢复预留名额                   ▼
              │                  RUNNING ──执行成功──► SUCCEEDED（终态）
              ▲                      │
              │                      └──失败且重试耗尽──► FAILED（终态）
              │                                            │
           WAITING ◄──恢复(reset)──── FAILED/BLOCKED       │ 阻断全部可达下游
              │                                           ▼
         上游全部成功                            BLOCKED（终态）
              └──────────────────► RUNNING
```

| 状态 | 含义 |
| --- | --- |
| `PENDING` | 初始状态，尚未被任何一轮触发占用。 |
| `WAITING` | 已被全量触发 / 恢复轮**预留名额**，等待上游成功。 |
| `RUNNING` | 执行体正在运行（可能处于某次重试尝试中）。 |
| `SUCCEEDED` | 终态：成功。**重复触发直接返回既有结论，绝不再次执行。** |
| `FAILED` | 终态：重试次数用尽。保留失败原因与尝试次数，可查询。 |
| `BLOCKED` | 终态：因上游 `FAILED`/`BLOCKED` 而被阻断，执行体从未运行。 |

- 普通触发遇到 `FAILED`/`BLOCKED` 会抛 `TaskNotRunnableException`，引导调用方走 `resume`，
  不允许用“重新触发”偷偷改写失败结论。
- 每次流转都会产生一条 `TaskEvent`（from、to、代次、明细、时间戳），
  通过 `getEvents(taskId)` 可获取**跨代次完整事件链**用于审计追溯。

---

## 3. 并发触发与去重规则

- 每个任务维护一个**在途占位（inflight Future）**：
  - `RUNNING` / `WAITING` 期间再次触发：
    - `MERGE`：返回**同一个在途 Future**，所有调用方拿到同一份结果，执行体只运行一次；
    - `REJECT`：抛 `DuplicateTriggerException`，且当轮批量预留整体回滚为 `PENDING`。
  - `SUCCEEDED` 后触发：返回已有成功快照（幂等，不执行）。
  - `FAILED` / `BLOCKED` 后触发：抛 `TaskNotRunnableException`（需恢复）。
- `triggerAll` 在调度前**按拓扑序一次性完成所有任务的名额预留**，
  因此两个线程并发 `triggerAll(MERGE)` 时，每个任务也只会执行一次（第二轮合并第一轮的在途结果）。
- 并发控制建立在每个任务独立的监视器（`synchronized` 方法）之上，
  调度线程**不嵌套持有两个任务的锁**，避免交叉加锁形成死锁。

---

## 4. 失败重试与恢复规则

### 4.1 重试

- 每次执行抛异常后，若已尝试次数 `< maxAttempts`，按 `backoffMillis` 间隔后重试；
  `maxAttempts=N` 表示“最多尝试 N 次”，因此只失败 1 次的任务执行体精确运行 N 次。
- 次数用尽后任务置 `FAILED`，失败原因码为 `RETRY_EXHAUSTED`，
  记录形如 `重试耗尽(N/N): <最后一次异常>` 的结论。

### 4.2 下游阻断（原因码可区分）

任务 `FAILED` 后，沿依赖图 BFS 阻断其全部可达下游：

| 下游位置 | 状态 | 原因码 `FailureReason` |
| --- | --- | --- |
| 直接下游 | `BLOCKED` | `UPSTREAM_FAILED` |
| 传递下游（下游的下游） | `BLOCKED` | `UPSTREAM_BLOCKED` |
| 自身 | `FAILED` | `RETRY_EXHAUSTED` |

被阻断任务的执行体**一次都不会运行**，阻断结论同样可查询。

### 4.3 恢复重放（`resume(policy)`）

- `SUCCEEDED` 任务：**原样保留，不重放、不重复产生副作用，代次不变。**
- `FAILED` 任务：开启**新代次（generation + 1）**、状态回到等待调度、重试预算重新计满后重新执行。
- `BLOCKED` 任务：同样开启新代次，随上游恢复结果自动放行；若恢复轮上游再次失败，则继续阻断。
- `PENDING` 任务：纳入恢复轮一并调度。
- 恢复轮仍失败时，新代次再次得到 `FAILED/RETRY_EXHAUSTED`，下游继续阻断，整体结论保持一致。
- 代次与每次状态流转都进入审计事件链：可以看到
  `gen0 ... -> FAILED`、`gen1 FAILED -> PENDING -> WAITING -> RUNNING -> SUCCEEDED` 的完整轨迹。

### 4.4 可区分异常一览

| 异常 | 语义 |
| --- | --- |
| `DuplicateTaskException` | 定义非法：重复注册任务/依赖。 |
| `MissingDependencyException` | 定义非法：依赖缺失。 |
| `CyclicDependencyException` | 定义非法：循环依赖。 |
| `DuplicateTriggerException` | 运行期：在途重复触发且策略为 REJECT。 |
| `TaskNotRunnableException` | 运行期：上游未就绪，或任务 FAILED/BLOCKED 需走 resume。 |
| `UnknownTaskException` | 查询/触发了不存在的 taskId。 |

---

## 5. API 速览

```java
OrchestrationEngine engine = new OrchestrationEngine(new WorkflowValidator());

engine.register(new WorkflowDefinition("demo-wf", List.of(
        new TaskDefinition("A", List.of(),                 RetryPolicy.noRetry(),        ctx -> doA()),
        new TaskDefinition("B", List.of("A"),              RetryPolicy.fixed(3, 100),    ctx -> doB()),
        new TaskDefinition("C", List.of("A", "B"),         RetryPolicy.noRetry(),        ctx -> doC())
)));

List<TaskRecord> result   = engine.triggerAll(TriggerPolicy.MERGE).join();
TaskRecord one            = engine.getStatus("B");
List<TaskEvent> trail     = engine.getEvents("B");

List<TaskRecord> resumed  = engine.resume(TriggerPolicy.MERGE).join();
```

> Spring Boot 环境中 `WorkflowValidator` 与 `OrchestrationEngine` 均注册为 Bean，可直接注入使用。

---

## 6. 本地验证方法

环境要求：JDK 21+、Maven 3.6+（本机已在 JDK 25、Maven 3.9 下验证）。

```bash
# 编译
mvn -o compile

# 运行全部单测（离线；首次联网后可去掉 -o）
mvn -o test

# 只跑某一类场景
mvn -o test -Dtest=WorkflowDefinitionValidationTest   # 重复注册 / 依赖缺失 / 循环依赖
mvn -o test -Dtest=OrchestrationHappyPathTest         # 依赖顺序 / 成功幂等 / 未就绪拒绝
mvn -o test -Dtest=ConcurrentTriggerTest              # 并发触发 MERGE / REJECT / 并发 triggerAll
mvn -o test -Dtest=DiamondDependencyTest              # 菱形 DAG 汇合等待
mvn -o test -Dtest=RetryExhaustionTest                # 重试耗尽 / 下游阻断原因码
mvn -o test -Dtest=ResumeReplayTest                   # 恢复重放 / 成功不重放 / 审计 / 恢复仍失败
```

### 测试覆盖矩阵（共 21 个用例）

| 测试类 | 覆盖场景 |
| --- | --- |
| `WorkflowDefinitionValidationTest` | 重复注册、依赖缺失、循环依赖（含环路径消息）、自依赖、合法 DAG 拓扑序 |
| `DiamondDependencyTest` | 菱形 DAG 汇合：两个上游先后完成，汇合任务等待双方成功且只执行一次 |
| `OrchestrationHappyPathTest` | 依赖顺序执行、成功任务重复触发不重放、上游未就绪时明确拒绝 |
| `SpringWiringTest` | Spring 上下文中校验器与引擎 Bean 可正常注入 |
| `ConcurrentTriggerTest` | 20 线程并发触发同一在途任务（MERGE 同一结果）、REJECT 抛可区分异常、并发 `triggerAll` 每个任务只执行一次 |
| `RetryExhaustionTest` | 精确重试 maxAttempts 次、FAILED/RETRY_EXHAUSTED 可查询、直连/传递阻断原因码、失败后拒绝普通触发 |
| `ResumeReplayTest` | 修复后恢复放行、成功任务不重放（副作用一次/代次不变）、跨代次审计事件链、恢复轮仍失败继续阻断 |

> 每个用例的日志都打印了**触发条件**（`[测试] 触发条件：...`）与**判定依据**
> （`[测试] 判定依据：...`），引擎侧日志带 `[校验]/[触发]/[执行]/[阻断]/[恢复]` 前缀，
> 可用 `target/surefire-reports/` 下的报告回放。并发用例通过双道 `CountDownLatch`
> 保证“所有重复触发都发生在执行期间”，避免测试侧竞态。
