# Repository Agent 领域

此目录定义框架无关的会话、运行、计划、工具调用、流式事件及基于证据的回答。

核心约束：

- 每个问题固定到 `tenant_id + repository_id + snapshot_id`，会话不得跨快照复用。
- Guard 要求 `repo:read`，问题最大 8000 个 Unicode 字符。
- 非降级回答至少包含一个通过 `SourceRef.Validate` 的同提交引用。
- `COMPLETED` 与 `FAILED` 是终态；只读问答当前不存在人工审批恢复点。
