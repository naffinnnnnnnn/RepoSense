# MCP 门面用例

`Service` 是协议无关的 MCP 应用门面，只依赖 `internal/ports` 中已经存在的
Graph、RAG、Wiki 和 Repository Agent 接口。它集中处理：

- 固定 `tenant_id + repository_id + snapshot_id` 的访问范围；
- 客户端授权、仓库白名单和 `repo:read` 权限；
- 按客户端和能力隔离的配额；
- 只保存请求 SHA-256 的追加式审计；
- 调用超时、稳定错误码、阶段耗时与计数指标。

算法和基础设施均保留接口缝隙：限流实现 `mcp.RateLimiter`，审计实现
`mcp.AuditSink`，知识能力实现既有 Application Port。首期能力见
[`docs/MCP_SERVER.md`](../../../docs/MCP_SERVER.md)。
