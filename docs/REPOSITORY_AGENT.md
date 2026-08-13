# Repository 智能问答 Agent

## 功能边界

Agent 拥有会话、运行、计划、脱敏工具调用记录、流式事件和最终答案。它只通过
`ports.Retriever` 与 `ports.GraphStore` 读取固定快照知识，不直接访问数据库、索引或源码目录。

## 执行流程

1. Guard 校验租户、仓库、快照、会话、问题长度、Locale 与 `repo:read` 权限。
2. Planner 识别架构、调用链、故障定位、影响分析或一般问题，生成有上限的计划。
3. Retrieve 执行混合检索与图查询；工具参数只记录策略和配额，不持久化问题正文。
4. Evaluate 验证 `SourceRef`、去重、限制数量并丢弃跨提交引用。
5. Synthesize 调用可替换的 `AnswerGenerator`。默认生成器完全确定性；LLM 适配器使用引用目录索引，模型不能创建引用。
6. Finalize 持久化运行、Token、耗时、错误码和 `qa.run.completed.v1` 事件，并结束 SSE 流。

无有效证据但知识源正常响应时，运行以 `COMPLETED` 结束，答案设置
`insufficient_evidence=true`。所有知识源均故障、上下文取消、生成失败或持久化失败时，
流以结构化 `FAILED` 事件结束。

## 扩展点

- 用 Eino/模型规划器替换 `Planner`；计划仍受应用层轮次与工具白名单限制。
- 用 pgvector/FTS/重排实现替换 `Retriever`，无需修改 Agent。
- 用 Eino `ChatModel` 包装器实现现有 `llm.ChatModel`，保留引用索引校验。
- 在 Evaluate 阶段增加覆盖率、置信度、引用蕴含性和离线评估分数。
- 高风险工具加入后，可用 `RunInterrupted` 和 `Resume` 承载检查点；只读 QA 不创建伪审批点。

## 可观测性

`agent_run` 阶段记录 `tenant_id/repository_id/snapshot_id/trace_id` 与失败类型；指标包括
完成/失败运行数、引用数和 Token 数。运行还保存每次工具调用的耗时、结果数、轮次和稳定错误码，
但不保存检索 Query 或源码正文。

## 验证

```powershell
$env:GOCACHE="$PWD/.gocache"
go test ./...
go test -race ./internal/application/agent ./internal/adapters/memory
```
