# RepoSense MCP Server

## 能力与边界

MCP Server 是已有应用服务的只读开放层，不直接访问数据库，也不复制 Graph、
RAG、Wiki 或 Agent 业务逻辑。每次请求必须显式指定 `repository_id` 和
`snapshot_id`，`tenant_id`、客户端与主体身份来自进程级授权配置。

| 能力 | 输入重点 | 输出 | 配额单位 |
|---|---|---|---:|
| `search_code` | query、策略、过滤器、top_k | 排序命中、上下文、诊断与源码引用 | 1 |
| `get_symbol` | symbol_id | 图实体与源码引用 | 1 |
| `find_call_chain` | symbol_id、方向、深度、上限 | CALLS 节点、边与查询诊断 | 2 |
| `get_wiki_page` | slug | 固定快照的 Wiki 修订与引用 | 1 |
| `ask_repository` | question、conversation_id、locale | run_id、带引用答案与证据状态 | 5 |

Wiki 同时以 Resource Template 开放：

```text
reposense://wiki/{repository_id}/{snapshot_id}/{slug}
```

## 安全与异常

- 授权状态必须为 `ACTIVE`，且未过期；仓库需位于 grant 白名单并包含
  `repo:read`。
- 限流键由 tenant、client 和 capability 组成，昂贵的 Agent 调用消耗更多单位。
- Graph 深度、结果数、RAG top_k、问题长度与 Wiki slug 均在领域层或 MCP
  门面执行上限校验。
- 对外稳定错误码包括 `INVALID_INPUT`、`UNAUTHENTICATED`、
  `PERMISSION_DENIED`、`RATE_LIMITED`、`UPSTREAM_FAILURE` 和
  `AUDIT_FAILURE`；底层错误不会暴露源码、令牌或凭据。
- 每次调用记录 capability、主体、作用域、trace_id、耗时、结果、配额与请求
  SHA-256；不记录原始问题、参数正文或源码内容。

## 本地启动

当前命令使用 stdio，并通过环境变量或同名参数注入本地授权：

```powershell
$env:REPOSENSE_TENANT_ID = "local"
$env:REPOSENSE_MCP_CLIENT_ID = "codex"
$env:REPOSENSE_MCP_PRINCIPAL_ID = "developer"
$env:REPOSENSE_MCP_REPOSITORIES = "reposense"
go run ./cmd/mcp
```

也可传入 `--tenant-id`、`--client-id`、`--principal-id`、`--repositories`、
`--quota` 和 `--quota-window`。标准输出严格保留给 MCP 消息，结构化诊断日志写
标准错误。

本地入口使用内存 Adapter 证明完整装配；生产部署应将相同 Port 替换为
PostgreSQL/pgvector/Neo4j、OIDC grant、分布式限流与持久化审计实现。

## 测试

```powershell
go test ./internal/application/mcp ./internal/transport/mcp
go test ./...
```

应用集成测试真实串联 Repository Store、Graph、RAG、Wiki 和 Agent；传输契约
测试使用官方 SDK 客户端验证 initialize/discovery、Tools 调用、结构化输出和
Resource 读取。
