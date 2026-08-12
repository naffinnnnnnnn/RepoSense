# Code Knowledge Graph 模块

## 模块定位

该模块消费 Repository Parser 产出的不可变 `CodeArtifact` 与 `CodeRelation`，发布按
`tenant_id + repository_id + snapshot_id` 隔离的图修订。图节点只保存 `artifact_id`、
展示属性和 `SourceRef`，不复制源码正文。

主要代码：

- `internal/domain/graph`：命令、节点、关系、修订、查询、领域错误与输入约束。
- `internal/application/graph`：全量/增量构建、符号解析、幂等、事件和观测编排。
- `internal/adapters/memory/graph_repository.go`：线程安全的参考实现及查询算法。
- `migrations/neo4j`：生产 Neo4j 适配器需要遵循的隔离约束和索引。
- `api/openapi`、`api/events`：同步 API 与 `graph.published.v1` 契约。

## 构建语义

`FULL` 从目标快照的全部解析产物构建新修订。`INCREMENTAL` 读取
`parent_snapshot_id` 对应的已发布修订，移除本次修改、删除或重命名路径关联的节点和
边，再叠加本次解析产物。父修订始终保持不可变；新修订完整构建后才以 `ACTIVE` 状态
原子发布，因此查询不会读到半成品。

`artifact_ids` 可限制重建范围。传入的每个 ID 都必须存在，否则整个构建失败。所有写
请求要求 `idempotency_key`；同一租户和仓库内重复调用返回同一修订，不重复写图。

## 符号解析与算法演进

当前 `NameResolver` 使用确定性的 artifact ID、限定名、短名、同文件优先和模块路径规则。
只有唯一候选才建立内部关系。无法解析或存在歧义的目标保留为显式外部节点，并通过
`unresolved_targets`、`ambiguous_relations` 指标暴露，避免静默建立错误关系。

解析算法位于独立的 `Resolver` 接口后，并在修订中记录 `algorithm_version`。后续可以接入
语言服务器、类型系统、导入作用域或向量重排，而无需改变领域模型、存储接口和查询 API。

## 查询约束

查询必须显式指定完整 Scope，不允许隐式读取“最新版”。根节点可以使用 `node_id` 或
`artifact_id`。支持：

- `BOTH`、`OUTGOING`、`INCOMING` 方向；
- 0–10 跳深度；
- 实体类型与关系类型过滤；
- 最大 10,000 个节点，默认 500；
- `context.Context` 超时和取消；
- 返回修订 ID、访问量、截断标志和耗时诊断。

## 可观测性与错误

构建和查询调用 `Observer.Stage` 记录阶段耗时与错误，并上报节点数、边数、未解析目标数、
幂等命中数和查询结果数。标签贯穿 `tenant_id`、`repository_id`、`snapshot_id`、`trace_id`。
可判定的 `DomainError` 区分输入、快照缺失、父修订缺失、发布冲突、构建失败和持久化失败，
并标注是否可重试。

## 运行测试

```powershell
go test ./...
go test -race ./internal/application/graph ./internal/adapters/memory
go vet ./...
```

测试覆盖命令边界、图构建、跨文件调用解析、歧义降级、增量修改/删除、父修订不可变、
幂等发布、查询投影、防御性拷贝和租户隔离。
