# 新阶段的开发

# 开发计划

## 一、下一阶段上线开发方案

```go
HTTP/CLI 提交解析
        ↓
输入、仓库身份、命令指纹校验
        ↓
PostgreSQL 原子创建幂等记录 + PENDING Job
        ↓
Worker 领取带租约的任务
        ↓
准备本地/远程仓库工作区并解析不可变 Commit
        ↓
Git 变更规范化 → 文件检查 → 语言 Parser
        ↓
事务保存 Snapshot + Job + Artifact + Relation + Outbox
        ↓
Outbox 发布 parse.completed / parse.failed
        ↓
Graph、RAG 等下游仅消费 SUCCEEDED Snapshot
```

## 二、开发里程碑

### M0：冻结领域契约和状态规则

#### 开发内容

1. 增加统一仓库相对路径规范化函数：
    - 处理 `/`、`\`、`.`、`..`、盘符和 UNC。
    - GitCLI、ChangedPath、IncludePaths 和 Parser 共用。
    - 增加明确的递归 `*` Glob 语义。
2. 增加 `ParseResult.Validate()`：
    - Job、Snapshot、EntityMeta 状态一致。
    - Job.SnapshotID 与 SnapshotID 一致。
    - SUCCEEDED 必须 Progress=100、CommitSHA 非空。
    - FAILED 必须具有 ErrorCode 和 ErrorMessage。
    - 未知状态直接拒绝。
3. 定义 `FailureDescriptor`：
    
    ```
    type FailureDescriptor struct {
        Code       ErrorCode
        Operation  string
        Message    string
        Retryable  bool
    }
    ```
    
4. 定义强类型事件：
    - `ParseCompletedPayload`
    - `ParseFailedPayload`
    - 专用事件构造函数
    - 空集合统一编码为 `[]`，不能成为 `null`
5. 修改 IDGenerator，使 ID 生成错误可以返回，而不是 panic。
6. ParserRegistry 提供真实版本摘要，替换写死的 `registry@1`。
7. 固化 ArtifactID 和 RelationID 新规则：
    - ArtifactID 包含规范路径、类型、限定名和必要签名。
    - RelationID 不再依赖绝对行号。
    - 身份算法升级 ParserVersion。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M0 解决的是“不同代码对同一个概念理解不一致”的根本问题：
    
    - 统一路径规范化，解决 Windows Pattern、DeletedPath、Parser 路径身份不一致。
    - 统一 ParseResult 状态校验，防止 Job、Snapshot、EntityMeta 出现矛盾终态。
    - 统一 FailureDescriptor，避免所有错误都变成“仓库解析失败”和 `retryable=true`。
    - 强类型事件避免 Payload 字段缺失或 `null` 不符合 Schema。
    - IDGenerator 返回错误，避免随机源故障终止整个进程。
    - 固定 ArtifactID、RelationID 规则，解决重载冲突和行号变化导致身份漂移。
- **对于上线功能的意义**
    
    上线后的持久化数据、API 响应、事件和下游 Graph 都依赖稳定契约。M0 完成后：
    
    - API 才能返回可信的 Job 和 Snapshot 状态。
    - 数据库才能建立正确约束。
    - Event Consumer 才能稳定消费事件。
    - Parser 可以持续升级，同时通过 ParserVersion 区分身份算法。
    - 后续模块能够围绕固定 Code IR 开发，不需要反复修改接口。

#### 完成标准

- 路径、状态、事件和身份规则只有一套实现。
- 相关单元测试全部通过。
- 新旧 ParserVersion 能够区分身份算法。
- ID 生成失败不会导致进程退出

### M1：升级 PostgreSQL 模型和 RepositoryStore 契约

#### 开发内容

- 增加 Repository 元数据表：
    - TenantID、RepositoryID。
    - Provider。
    - 规范化仓库身份。
    - 本地仓库使用 canonical path。
    - 远程仓库使用规范化 URL/namespace。
    - 创建后禁止 RepositoryID 指向另一仓库。
- 修改 Snapshot 约束：
    - ResolveCommit 前失败时允许 CommitSHA 为空。
    - SUCCEEDED Snapshot 必须有 CommitSHA。
    - 允许同一 Commit 存在失败尝试和后续成功尝试。
    - “同仓库同 Commit 只能有一个成功快照”使用部分唯一索引实现。
- 扩展幂等记录：
    - CommandFingerprint。
    - JobID、SnapshotID。
    - 状态：RUNNING、SUCCEEDED、FAILED。
    - Retryable、RetryCount。
    - LeaseOwner、LeaseExpiresAt。
    - 创建和更新时间。
- 增加原子 Store 操作：
    - `AcquireIdempotency()`
    - `CompleteResult()`
    - `FailResult()`
    - `SaveResultIfLatest(expectedParentID)`
    - `ClaimPendingJob()`
    - `MarkOutboxPublished()`
- Snapshot、Job、Artifact、Relation、幂等记录和 Outbox 必须在同一个事务中写入。
- Artifact、GraphInput 查询增加 Snapshot 成功状态检查。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M1 将当前进程内、不具备事务保证的 Memory Store 替换为生产级状态边界：
    
    - 解决 Worker 重启后快照、幂等记录全部丢失。
    - 解决相同幂等键并发执行两次。
    - 通过 CAS (Compare-And-Swap)防止并发增量同步产生分叉快照；保存新快照前，先确认增量基线仍然是自己开始解析时读取的那个快照。
    - 通过事务(要么全保存，要么不保存)避免 Snapshot 已保存但 Job、Artifact 或事件未保存；
    - 保存 RepositoryID 与真实仓库身份的唯一绑定。
    - 区分 RUNNING、SUCCEEDED、FAILED，避免失败结果被当成成功缓存。
    - 拒绝状态矛盾的 ParseResult。
    - 阻止失败快照的部分 Artifact 暴露给下游。
    
    它把大量依赖进程变量和调用顺序的隐式规则，转换为数据库可验证的原子规则。
    
- **对于上线功能的意义**
    
    PostgreSQL 是异步 API、任务查询和多 Worker 部署的基础：
    
    - 用户可以跨请求查询 Job 状态。
    - Worker 重启后可以恢复未完成任务。
    - 多实例能够安全共享任务和快照。
    - 增量解析能够跨进程持续工作。
    - Snapshot、Artifact 和 Relation 可以提供长期查询。
    - Outbox 可以和业务结果在同一事务中保存。

#### 完成标准

- Worker 重启后仍能读取历史快照。
- 相同幂等键并发提交只产生一个有效任务。
- 同仓库并发增量同步不会产生分叉。
- 一个事务失败时不会留下半个 Snapshot 或孤立 Outbox。
- 失败 Snapshot 无法通过标准 Artifact 接口读取。
- PostgreSQL 集成测试覆盖提交、回滚、租约过期和并发冲突。

### M2：完善Git和仓库工作区能力

#### 开发内容

- 增加 Repository Workspace 层：
    
    ```
    Repository 配置
        → 准备本地工作区
        → fetch/clone
        → 返回受控本地路径
        → GitCLI 读取不可变 Commit
    ```
    
- 保留本地仓库支持，并增加远程仓库准备能力：
    - Provider 和 Repository URL。
    - CredentialsRef 只引用 Secret，不在命令、日志或数据库中保存明文凭据。
    - 使用受控缓存目录和清理策略。
    - 不直接让 Parser 接触凭据。
- 统一 Git 错误分类：
    - Context 取消/超时。
    - Repository 不存在。
    - Git CLI 缺失。
    - Ref/Commit 不存在。
    - 输出超限。
    - Git 命令临时失败。
- Git 输出上限改为可配置，并将超限标记为不可重试。
- 完整处理 Diff 状态：
    - Added、Modified、Deleted、Renamed、Copied、TypeChanged。
    - Copy 映射为 Added。
    - 未知状态明确失败。
- ListFiles 读取对象类型，只返回 Blob。
- ReadFile 再次验证对象是 Blob，拒绝 Tree 和 Gitlink。
- 增量基线消失时：
    - 只对旧基线 `REF_NOT_FOUND` 降级为 FULL。
    - 其他 Git 错误保持失败。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M2 解决 Repository Parser 对真实 Git 环境缺少防御的问题：
    
    - 正确区分仓库不存在、Ref 不存在、Git CLI 缺失、Context 取消和普通 Git 失败。
    - 强推导致旧 Commit 消失时安全降级为全量解析。
    - Git 输出超限不再被错误标记为可重试。
    - Copy 和未知 Diff 状态不会被静默忽略。
    - Gitlink、Tree 等非 Blob 对象不会进入 Parser。
    - RepositoryID 不会绑定到错误的本地或远程仓库。
    - Git 命令错误不会覆盖更准确的底层 DomainError。
    
    它使 GitRepository 从“理想输入下可用”变为“能够处理真实仓库生命周期和运行环境”。
    
- **对于上线功能的意义**
    
    M2 提供上线版本需要的仓库接入能力：
    
    - 同时支持本地仓库和远程仓库。
    - 支持受控 clone、fetch 和缓存。
    - 使用 CredentialsRef 获取凭据，不暴露明文密钥。
    - 所有解析固定到不可变 CommitSHA。
    - 可以在仓库强推、更新或缓存清理后继续运行。
    
    完成后，Repository Parser 才能从开发机本地工具升级为真正的仓库服务。
    

#### **完成标准**

- 本地与远程仓库都转换为不可变 Commit 后再解析。
- 凭据不出现在日志、错误、事件和 ParseResult。
- 强推后能够安全执行全量恢复。
- GitCLI 缺失、仓库不存在、Ref 不存在能被准确区分。
- 真实临时 Git 仓库测试和可控 Git 子进程测试全部通过。

### M3：建立统一 ChangedPath 和文件处理流水线

#### 开发内容

新增明确流水线：

```
Git 原始变更
  → validate
  → canonicalize
  → deduplicate
  → filter
  → limit
  → deterministic sort
  → process files
```

具体包括：

1. 校验 Path、OldPath、Kind 和 Rename 结构。
2. Deleted、Rename.OldPath 也必须经过安全检查。
3. 按 `Path + Kind + OldPath` 去重。
4. 按 `Path → Kind → OldPath` 稳定排序。
5. 无 IncludePaths 时仍复制输入 Slice。
6. 先过滤再检查 MaxFiles。
7. 使用完整内容判断 NUL 和 UTF-8，消除 8000 字节采样问题。
8. 每个变更完成检查后统一更新 Progress。
9. 保留当前已正确的行为：
    - MaxFileBytes 在 Parser 前判断。
    - too_large、binary、unsupported 写入 SkippedFiles。
    - 不支持文件不读取 Blob。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M3 在 Git 输出和 Parser 之间建立可信边界：
    
    - 拒绝空路径、未知 Kind 和不完整 Rename。
    - DeletedPath 和 Rename.OldPath 也经过安全检查。
    - 重复变更不会造成重复解析和重复 Artifact。
    - 相同 Path 的多种变更具备稳定排序规则。
    - IncludePaths 使用统一的递归 Glob 语义。
    - MaxFiles 在过滤后检查。
    - 二进制判断不再受 8000 字节采样边界影响。
    - Progress 能正确计算 Deleted 和 Skipped 文件。
    
    它把当前分散的过滤、排序和 ReadFile 检查整合成一条确定、可验证的输入流水线。
    
- **对于上线功能的意义**
    
    上线版本需要处理大型仓库和复杂增量变更。M3 完成后：
    
    - 大仓库解析范围可控。
    - IncludePaths 能可靠限制解析目录。
    - 增量结果具有可重复性。
    - Job Progress 可以真实展示给用户。
    - DeletedPaths 可以安全驱动 Graph 和 RAG 删除旧数据。
    - 后续增加新 Parser 时，不需要重复处理路径和文件边界。
    
    M3 是“稳定增量解析”和“可观察异步任务”的共同基础。
    

#### 完成标准

- 相同 Git 变更集合无论输入顺序如何，输出完全一致。
- 不安全路径不会进入 DeletedPaths、Parser 或事件。
- 重复变更不会产生重复 Artifact。
- Progress 能反映已检查的 Deleted 和 Skipped 文件。
- MaxFiles、MaxFileBytes 和二进制边界测试全部通过。

### M4：升级 Parser 能力和 Code IR 身份

#### 开发内容

- 保持 `LanguageParser` 端口不变，逐步替换正则内部实现。
- 第一批先修正现有语言：
    - Python 多行函数和多行声明。
    - TypeScript/Java 块注释、字符串和嵌套结构。
    - Java 重载方法。
    - 配置文件确定性语法错误。
    - 作用域内调用解析。
- 建立两阶段符号解析：
    - 第一阶段收集声明和作用域。
    - 第二阶段解析调用、继承、实现和导入关系。
    - 无法唯一解析时保留 unresolved，不错误绑定。
- 新增上线所需语言：
    - 优先增加当前项目真实测试中明确未支持的 Go 和 Rust。
    - 新语言必须实现同一 Code IR、身份和错误契约。
- 对规范化路径生成 ArtifactID、QualifiedName 和 SourceRef。
- ArtifactID：
    - 重载类型加入规范化签名。
    - 文件移动或名称变化按既定身份策略处理。
- RelationID：
    - 使用语义关系身份。
    - 行号只作为 Evidence。
    - 非语义空行不能导致关系删除重建。
- Parser 对确定语法错误必须返回 error，不能返回空结果加 nil。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M4 解决当前正则 Parser 的结构性限制：
    
    - 多行声明不会漏解析。
    - 块注释中的伪代码不会生成 Artifact。
    - Java 重载方法不会产生相同 ArtifactID。
    - 同名方法调用可以结合当前作用域解析。
    - 添加空行不会改变 RelationID。
    - 确定性语法错误不会被返回为解析成功。
    - 等价路径不会生成两套 Artifact 身份。
    
    它提高的不只是“提取数量”，更重要的是 Artifact 和 Relation 的身份稳定性与可信度。
    
- **对于上线功能的意义**
    
    Graph、RAG、Wiki 和 Assistant 的质量直接依赖 Parser 输出：
    
    - 稳定 ArtifactID 让增量 Graph 能正确更新，而不是反复删除重建。
    - 更准确的调用关系提升代码检索和问答质量。
    - ParserVersion 允许新旧算法并存和追踪。
    - 新增 Go、Rust 等语言扩大可解析仓库范围。
    - Code IR 保持统一，下游不需要理解每种语言的具体语法。
    
    M4 完成后，系统才能提供可被用户信任的代码结构和关系数据。
    

#### 完成标准

- 当前 Parser 异常测试全部转绿。
- Go、Rust 至少覆盖文件、模块/包、类型、函数/方法、导入和基础调用关系。
- 新身份算法有明确 ParserVersion。
- 同一源码非语义行移不会改变 ArtifactID 和 RelationID。
- 语法错误能够进入统一失败流程。

### M5：把同步 Service 拆成可靠任务生命周期

#### 开发内容

- 将当前 `Sync()` 内部拆分为：
    
    ```
    Submit(ctx, command) (ParseJob, error)
    Execute(ctx, jobID) (ParseResult, error)
    FinalizeSuccess(...)
    FinalizeFailure(...)
    ```
    
- 保留兼容入口：
    
    ```
    Sync = Submit + Execute
    ```
    
- Submit 阶段：
    - 校验 TraceID、路径和配置。
    - 解析或绑定仓库身份。
    - 解析 Ref 为不可变 Commit。
    - 生成命令指纹。
    - 原子获得幂等执行权。
    - 创建 PENDING/RUNNING Job 和 Snapshot。
- Execute 阶段：
    - 领取带租约任务。
    - 加载增量基线。
    - 执行 Git、ChangedPath、Parser 流程。
    - 定期检查取消。
    - 更新有意义的 Progress。
- FinalizeFailure 阶段：
    - 所有运行期步骤统一进入失败收口。
    - FailureDescriptor 决定 Code、Operation、Message、Retryable。
    - 使用独立且有界的清理 Context 保存失败。
    - 使用 `errors.Join()` 同时保留主错误和二次错误。
    - 已接受任务无论在哪一步失败，都形成可查询终态。
- 幂等状态规则：
    - SUCCEEDED：返回缓存结果。
    - RUNNING：返回现有任务或等待。
    - FAILED 且可重试：创建新 Attempt、JobID 和 SnapshotID。
    - FAILED 且不可重试：返回原失败，不返回 nil。
    - 未知状态：一致性错误。
- 成功保存失败时返回已完成 ParseResult 和 Persistence 错误，不能返回零值。
- 时间字段统一收口，防止系统时间回拨造成顺序错误。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M5 解决当前 `Sync()` 中失败处理不完整的问题：
    
    - ResolveCommit、LatestSnapshot、ListFiles、Diff 等早期失败也能形成失败任务。
    - 不同错误获得正确的 Code、Operation、Message 和 Retryable。
    - 保存失败状态时不会因为原 Context 已取消而必然失败。
    - 二次持久化或事件错误不会覆盖原始错误。
    - 成功结果保存失败时不会返回零值 ParseResult。
    - 失败幂等记录不会被当成成功缓存。
    - Worker 崩溃后任务可以通过租约重新领取。
    - 未知状态不会继续执行。
    
    它把任务从一次函数调用，升级为具有明确状态机、失败终态和恢复能力的业务实体。
    
- **对于上线功能的意义**
    
    异步 API 的核心不是“后台执行”，而是任务能够被查询、取消、重试和恢复。M5 完成后：
    
    - API 可以快速返回 PENDING Job。
    - 用户可以查询真实进度。
    - 可取消运行中的任务。
    - 临时失败可以安全重试。
    - 不可重试错误不会反复消耗资源。
    - Worker 可以水平扩容和故障恢复。
    - CLI 仍可通过兼容 Sync 入口同步执行。
    
    M5 是阶段 4 异步任务功能真正成立的核心。
    

#### 完成标准

- ResolveCommit、LatestSnapshot、ListFiles、Diff、ReadFile、Parse、Save、Publish 全部有明确失败终态。
- 请求 Context 取消后，失败现场仍能在有界时间内保存。
- 可重试失败能够重新执行，不可重试失败不会自动循环。
- 并发任务、租约过期和 Worker 崩溃恢复测试通过。
- 测试全部转绿。

### M6：实现 HTTP API、任务控制和事件投递

#### 开发内容

- 实现 Parse HTTP 接口：
    - 从 Header 获取 Idempotency-Key。
    - 从认证上下文获取 TenantID 和 TraceID。
    - 返回 PENDING/RUNNING ParseJob。
    - HTTP 不等待完整仓库解析。
- 增加任务接口：
    - 查询 Job 状态和进度。
    - 查询 Snapshot 结果。
    - 取消运行中任务。
    - 对可重试失败发起重试。
    - Artifact 分页查询仅允许成功 Snapshot。
- Worker：
    - 从 PostgreSQL 领取 Pending Job。
    - 使用租约和心跳避免重复执行。
    - Worker 崩溃后允许租约接管。
- Outbox Dispatcher：
    - 发布 NATS 事件。
    - 记录投递次数、最后错误、下次重试时间。
    - 已成功事件不重复发布。
    - 达到上限进入死信状态并告警。
- 下游触发：
    - `parse.completed.v1` 才触发 Graph/RAG。
    - `parse.failed.v1` 只用于任务状态和告警。
    - 下游再次验证 Snapshot 为 SUCCEEDED。
- 强类型事件构造必须通过现有 JSON Schema。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M6 解决系统边界上的可靠性问题：
    
    - API 不再等待长时间解析，降低请求超时风险。
    - Worker 使用租约，避免任务丢失或重复执行。
    - Outbox 解决结果保存成功但事件发布失败的问题。
    - 已发布事件不会因幂等命中重复投递。
    - Broker 临时不可用时可以自动重试。
    - 事件 Schema 由强类型构造保证。
    - Graph、RAG 只消费成功 Snapshot。
    - 失败 Artifact 不会进入下游知识链。
    
    它把应用内部的可靠状态，扩展为跨 API、数据库、Worker、Broker 和下游消费者的端到端可靠性。
    
- **对于上线功能的意义**
    
    M6 直接交付用户和其他系统可使用的功能：
    
    - `POST /parse` 异步提交任务。
    - 查询 Job、Snapshot 和 Artifact。
    - 取消和重试任务。
    - 多 Worker 后台解析。
    - NATS 发布解析完成或失败事件。
    - 自动触发 Graph 和 RAG。
    - 支持前端或调用方持续展示解析状态。
    
    这是从内部代码模块转变为可调用线上服务的关键里程碑。
    

#### 完成标准

- POST parse 在任务创建后快速返回 202。
- 重复请求返回同一有效任务或明确 409。
- 已发布 EventID 不会因为普通幂等命中再次投递。
- Broker 暂时失败后 Outbox 能够自动恢复。
- API 实现与 OpenAPI 一致。
- Repository → Parse → Event → Graph 集成测试通过。

### M7：配置、可观测性、安全和上线门禁

#### 开发内容

- 配置外部化：
    - MaxFiles、MaxFileBytes。
    - Git 输出上限。
    - Parse Timeout。
    - Failure Cleanup Timeout。
    - Job Lease。
    - Retry 次数和退避。
    - Workspace 缓存及保留时间。
- 构造函数：
    - 生产环境拒绝缺少 Store、Publisher、Observer。
    - 测试显式注入测试实现。
    - 非正数配置启动失败。
- 可观测性：
    - Job 状态计数。
    - 各阶段耗时。
    - Git/Parser/Store/Outbox 失败分类。
    - 重试次数。
    - 幂等命中和冲突。
    - 快照分叉冲突。
    - SkippedFiles 原因。
    - Outbox 积压和最老事件年龄。
- 安全：
    - 日志和事件禁止记录源码、Git 凭据和原始 stderr。
    - Workspace 目录隔离和生命周期清理。
    - CredentialsRef 权限校验。
    - Tenant、Repository、Snapshot 查询全链路隔离。
- 数据生命周期：
    - Workspace 清理。
    - 失败 Job 和 Snapshot 保留策略。
    - Artifact、Outbox 和历史快照归档策略。
    - 清理不能删除仍被 Graph/RAG 引用的成功 Snapshot。

#### 整合价值

- **对于异常和工程化问题的意义**
    
    M7 处理的是“代码逻辑正确，但生产环境仍可能不可控”的问题：
    
    - 非法配置在启动时失败，不再静默替换。
    - Git 输出、文件数、文件大小、超时、租约和重试次数均可配置。
    - 指标能够区分 Git、Parser、Store、Outbox 等失败阶段。
    - Outbox 堆积和任务卡死可以被发现。
    - 凭据、源码和 Git stderr 不会进入日志。
    - Workspace、失败任务和历史快照具备清理策略。
    - 并发、崩溃和迁移问题在上线前通过专项测试发现。
    
    它为前面里程碑提供运行时保护和故障发现能力。
    
- **对于上线功能的意义**
    
    上线不仅要求功能可用，还要求能够持续运营：
    
    - 运维人员可以观察任务吞吐、耗时和失败率。
    - 可以调整大仓库资源限制，而不需要重新编译。
    - 可以定位某个 TraceID 对应的完整任务链。
    - 可以发现 Worker 停滞、Broker 故障和数据库冲突。
    - 数据不会无限增长。
    - 凭据和租户数据满足基本安全隔离要求。
    - 数据库迁移和回滚经过验证。
    
    M7 完成后，整个 Repository Parser 才从“功能完成”达到“具备上线和持续运行条件”。
    

#### 完成标准

- `go test ./...` 全部通过。
- 并发相关包通过 `go test -race`。
- PostgreSQL、NATS、真实 Git 的集成测试通过。
- 所有已新增红色工程测试转绿。
- 日志扫描确认不包含凭据和源码。
- 压力测试证明资源限制有效。
- Worker 强制终止后任务可以恢复。
- 发布前完成数据库迁移演练和回滚演练。

## 三、实施依赖关系

```go
M0 领域契约
 ├── M1 PostgreSQL/幂等/Outbox 数据结构
 ├── M2 Git 与远程工作区
 └── M3 ChangedPath 流水线
          ↓
        M4 Parser
          ↓
M1 + M2 + M3 + M4
          ↓
M5 任务生命周期
          ↓
M6 HTTP/Worker/事件/下游
          ↓
M7 上线门禁
```

## 四、建议的提交拆分

为降低回归风险，每个提交保持单一目标：

1. `refactor(repository): add canonical paths and domain invariants`
2. `refactor(ports): make id generation failures explicit`
3. `feat(repository): add typed parse events and failure policy`
4. `feat(postgres): implement repository parser store`
5. `feat(repository): add idempotency leases and snapshot cas`
6. `fix(gitcli): preserve typed failures and blob semantics`
7. `fix(repository): normalize and validate changed paths`
8. `fix(parser): stabilize artifact and relation identities`
9. `feat(parser): add syntax-aware parsers and target languages`
10. `refactor(repository): split submit execute and finalize`
11. `feat(api): expose asynchronous repository parse jobs`
12. `feat(events): publish parse outbox through nats`
13. `test(repository): add postgres nats and crash recovery integration`
14. `chore(repository): add production config metrics and rollout checks`

每个提交都应先让对应异常/工程问题测试转绿，再合入下一项，避免最后一次性修复全部红色测试。

## 五、最终上线完成定义

Repository 解析模块达到下一阶段上线标准，需要同时满足：

- 支持持久化、跨进程增量解析。
- 支持本地及受控远程仓库。
- 提供异步 Parse API、状态查询、取消和重试。
- 相同仓库的快照历史保持线性。
- 幂等键绑定完整命令并具备并发原子性。
- 所有运行期失败都有可查询终态。
- Parser 不再因路径、重载、行移和同名符号产生明显错误身份。
- 失败 Snapshot 不会进入 Graph、RAG 或 Artifact 标准查询。
- 结果和 Outbox 原子保存，事件可恢复且不会错误重复发布。
- 配置、资源限制、日志、指标、凭据和数据生命周期满足生产运行要求。
- 当前阶段 3 新增的工程测试全部通过，并补齐 PostgreSQL、NATS、HTTP 和 Worker 恢复集成测试。