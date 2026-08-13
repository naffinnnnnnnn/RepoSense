# AI Wiki 自动生成模块

## 模块边界

AI Wiki 读取固定 `tenant_id + repository_id + snapshot_id` 的已发布图谱修订，可选地通过 `Retriever` 补充证据，生成版本化 Markdown 页面。模块只保存 `SourceRef`，不复制源码正文，也不会隐式读取“最新”快照。

核心实现：

- `internal/domain/wiki`：生成命令、空间、任务、页面修订、审核状态和可判定领域错误。
- `internal/application/wiki`：生成编排、证据校验、结构化生成器、幂等发布、事件和指标。
- `internal/adapters/memory/wiki_repository.go`：线程安全、原子发布、租户隔离的参考存储实现。
- `internal/ports/knowledge.go`：`WikiService`、`WikiRepository` 和可替换的 `WikiContentGenerator` 契约。
- `api/events/wiki.published.v1.schema.json`：发布事件的 JSON Schema。

## 输入与输出

`wiki.GenerateCommand` 必须包含完整 Scope、明确的 `graph_revision_id` 和 `idempotency_key`。Locale 支持 `zh-CN`（默认）与 `en-US`；`page_scope` 为空时生成以下完整导航：

1. `overview`
2. `architecture`
3. `modules`
4. `key-flows`
5. `interfaces`
6. `development-guide`

`Generate` 在当前实现中同步完成生成和原子发布，返回状态为 `SUCCEEDED` 的 `wiki.Job`。调用方再通过 `GetPage(ctx, scope, slug)` 获取精确快照的页面修订。该同步实现符合 `WikiService` 契约，未来切换异步 Worker 时可保持命令、任务和页面模型不变。

## 生成与引用规则

默认 `StructuredGenerator` 只依据图谱实体、关系和合法 `SourceRef` 生成内容，不推断源码中不存在的事实。每一页必须至少包含一条有效引用；跨提交引用、空正文、重复/缺失页面及未知页面都会使整次生成失败，不发布半成品。

`WikiContentGenerator` 是算法扩展点。`internal/adapters/llm.WikiGenerator` 已实现供应商无关的模型适配：Eino `ChatModel` 只需包装为窄 `ChatModel` 接口，模型输出只能引用已提供的证据目录索引。页面版本、引用验证、内容哈希、原子保存、事件和可观测性仍由应用服务负责。模型名称、Prompt 版本和 Token 用量会保存并写入发布事件。

## 增量与一致性

- `page_scope` 可只刷新受影响页面。
- Page ID 在仓库、Locale 和 Slug 范围内稳定；新快照生成同一页面时 `revision_no` 单调递增。
- 内容哈希覆盖 Markdown、提交、路径、符号、行号及证据哈希，可检测知识版本漂移。
- 同一租户/仓库内重复 `idempotency_key` 返回原任务并重放事件；若键被用于不同快照或图修订则返回冲突。
- 空间、任务及所有页面由 `SavePublication` 一次原子保存；旧快照页面始终可读。
- 图查询若因规模上限被截断，整次生成会失败，避免静默发布不完整 Wiki。

## 异常与可观测性

领域错误区分无效输入、图谱未就绪、证据不足、生成失败、页面不存在、并发冲突和持久化失败，并标明是否可重试。服务记录生成/读取阶段耗时，以及页面数、引用数、Token 数和幂等命中数；标签贯穿 `tenant_id`、`repository_id`、`snapshot_id` 与 `trace_id`。

## 测试

```powershell
go test ./...
go test -race ./internal/application/wiki ./internal/adapters/memory
go vet ./...
```

测试覆盖命令边界、双语页面、证据校验、模型异常、图修订不匹配、幂等、租户隔离、跨快照增量修订，以及 Repository ParseResult → Graph Revision → Wiki 的模块联调。
